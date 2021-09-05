// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::Version as VersionTrait;
use super::{Error, Journal, Log, Sequencer, Snapshot, Transaction, VersionCell};

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::Arc;

use scc::ebr;
use scc::TreeIndex;

/// [Container] is a organized data container that is transactionally updated.
///
/// A [Container] may hold references to other [Container] instances, and those holding
/// [Container] references are called a directory.
pub struct Container<S: Sequencer> {
    /// It is either a [Data](Type::Data) or [Directory](Type::Directory) container.
    container: Type<S>,
}

impl<S: Sequencer> Container<S> {
    /// Creates a new directory [Container].
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// use tss::{AtomicCounter, Container, ContainerHandle};
    /// type Handle = scc::ebr::Arc<Container<AtomicCounter>>;
    ///
    /// let container_handle: Handle = Container::new_directory();
    /// ```
    pub fn new_directory() -> ebr::Arc<Container<S>> {
        ebr::Arc::new(Container {
            container: Type::Directory(TreeIndex::default()),
        })
    }

    /// Creates a new data [Container].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Container, ContainerHandle, RelationalTable};
    /// type Handle = scc::ebr::Arc<Container<AtomicCounter>>;
    ///
    /// let container_data = Box::new(RelationalTable::new());
    /// let container_handle: Handle = Container::new_container(container_data);
    /// ```
    pub fn new_container(
        container_data: Box<dyn ContainerData<S> + Send + Sync>,
    ) -> ebr::Arc<Container<S>> {
        ebr::Arc::new(Container {
            container: Type::Data(container_data),
        })
    }

    /// Creates a new container directory under the given name.
    pub fn create_directory(
        &self,
        name: &str,
        snapshot: &Snapshot<S>,
        journal: &mut Journal<S>,
    ) -> Option<ebr::Arc<Container<S>>> {
        let guard = crossbeam_epoch::pin();
        if let Type::Directory(directory) = &self.container {
            let mut directory_shared_ptr = directory.load(Relaxed, &guard);
            if directory_shared_ptr.is_null() {
                match directory.compare_exchange(
                    Shared::null(),
                    Owned::new(TreeIndex::new()),
                    Relaxed,
                    Relaxed,
                    &guard,
                ) {
                    Ok(result) => directory_shared_ptr = result,
                    Err(result) => directory_shared_ptr = result.current,
                }
                let directory_ref = unsafe { directory_shared_ptr.deref() };
                loop {
                    if let Some(Some(existing_container)) =
                        directory_ref.read(name, |_, anchor_ptr| {
                            let anchor_ref = unsafe { anchor_ptr.load(Acquire, &guard).deref() };
                            anchor_ref.get(snapshot, &guard)
                        })
                    {
                        return existing_container.create_handle();
                    }

                    let new_container_anchor_ptr = Atomic::new(ContainerAnchor::new());
                    if let Err((_, value)) =
                        directory_ref.insert(String::from(name), new_container_anchor_ptr.clone())
                    {
                        // Insertion failed.
                        let new_container_shared = value.swap(Shared::null(), Relaxed, &guard);
                        drop(unsafe { new_container_shared.into_owned() });
                    } else {
                        // The directory version has not become reachable.
                        let new_directory = Self::new_directory();
                        let new_directory_version_ptr = Atomic::new(Version::new());
                        let new_directory_version_shared = new_directory_version_ptr
                            .load(Relaxed, unsafe { crossbeam_epoch::unprotected() });
                        // The anchor is reachable by other threads, therefore a memory fence is required.
                        //  - It is possible that the anchor has been defer-destroyed.
                        let new_container_anchor_shared =
                            new_container_anchor_ptr.load(Acquire, &guard);
                        debug_assert!(!new_container_anchor_shared.is_null());
                        if unsafe {
                            new_container_anchor_shared.deref().push(
                                new_directory_version_shared,
                                snapshot,
                                &guard,
                            )
                        } && journal
                            .create(
                                unsafe { new_directory_version_shared.deref() },
                                Some(new_directory.clone()),
                            )
                            .is_ok()
                        {
                            // It is safe for the transaction to have a reference to the container, as the container is
                            // to stay intact as long as it is locked or its creation clock is not set.
                            return Some(new_directory);
                        }
                        // Versioning failed.
                        drop(unsafe { new_directory_version_shared.into_owned() });
                    }
                }
            }
        }
        None
    }

    /// Searches for a container associated with the given name.
    pub fn search<'b>(
        &self,
        name: &str,
        snapshot: &Snapshot<S>,
        barrier: &'b ebr::Barrier,
    ) -> ebr::Ptr<'b, Container<S>> {
        if let Type::Directory(directory) = &self.container {
            let directory_shared = directory.load(Relaxed, barrier);
            if !directory_shared.is_null() {
                if let Some(Some(existing_container)) =
                    unsafe { directory_shared.deref() }.read(name, |_, anchor_ptr| {
                        let anchor_ref = unsafe { anchor_ptr.load(Acquire, &barrier).deref() };
                        anchor_ref.get(snapshot, &barrier)
                    })
                {
                    return Some(existing_container);
                }
            }
        }
        None
    }

    /// Makes a link to the given data container if the current container is a directory.
    ///
    /// It overwrites an existing visible container if the container is the latest version under the same name.
    pub fn link(
        &self,
        name: &str,
        container_handle: ContainerHandle<S>,
        snapshot: &Snapshot<S>,
        journal: &mut Journal<S>,
    ) -> bool {
        let guard = crossbeam_epoch::pin();
        match (&self.container, container_handle.get(&guard)) {
            (Type::Directory(directory), Some(container)) => {
                if let Type::Data(_) = &container.container {
                    let mut directory_shared_ptr = directory.load(Relaxed, &guard);
                    if directory_shared_ptr.is_null() {
                        // Creates a new directory structure.
                        match directory.compare_exchange(
                            Shared::null(),
                            Owned::new(TreeIndex::new()),
                            Relaxed,
                            Relaxed,
                            &guard,
                        ) {
                            Ok(result) => directory_shared_ptr = result,
                            Err(result) => directory_shared_ptr = result.current,
                        }
                    }

                    // Makes a link to the given container.
                    let directory_ref = unsafe { directory_shared_ptr.deref() };
                    loop {
                        if let Some(result) = directory_ref.read(name, |_, anchor_ptr| {
                            // A container anchor under the same name exists.
                            let anchor_ref = unsafe { anchor_ptr.load(Acquire, &guard).deref() };
                            // Creates a new ContainerVersion for the given container.
                            let container_version_ptr = Atomic::new(Version::new());
                            let container_version_shared = container_version_ptr
                                .load(Relaxed, unsafe { crossbeam_epoch::unprotected() });
                            // Pushes the new ContainerVersion into the version chain.
                            if anchor_ref.push(container_version_shared, snapshot, &guard) {
                                // Successfully pushed the new ContainerVersion.
                                //
                                // The new ContainerVersion is now owned by the transaction,
                                // and therefore the transaction tries to take ownership.
                                if journal
                                    .create(
                                        unsafe { container_version_shared.deref() },
                                        Some(container_handle.clone()),
                                    )
                                    .is_ok()
                                {
                                    // The transaction took ownership.
                                    return true;
                                }
                            } else {
                                drop(unsafe { container_version_shared.into_owned() });
                            }
                            // Failed to push the new ContainerVersion.
                            //
                            // It can be regarded as a serialization failure.
                            false
                        }) {
                            return result;
                        }

                        // Needs to insert a new container anchor.
                        let new_container_anchor_ptr = Atomic::new(ContainerAnchor::new());
                        if let Err((_, value)) = directory_ref
                            .insert(String::from(name), new_container_anchor_ptr.clone())
                        {
                            // Insertion failed.
                            let new_container_shared = value.swap(Shared::null(), Relaxed, &guard);
                            drop(unsafe { new_container_shared.into_owned() });
                        }
                    }
                }
                false
            }
            (_, _) => false,
        }
    }

    /// Unlinks a container associated with the given name.
    pub fn unlink(&self, name: &str, snapshot: &Snapshot<S>, journal: &mut Journal<S>) -> bool {
        if let Type::Directory(directory) = &self.container {
            let guard = crossbeam_epoch::pin();
            let directory_shared_ptr = directory.load(Relaxed, &guard);
            let directory_ref = unsafe { directory_shared_ptr.deref() };
            if let Some(result) = directory_ref.read(name, |_, anchor_ptr| {
                let anchor_ref = unsafe { anchor_ptr.load(Acquire, &guard).deref() };
                // A ContainerVersion not pointing to a valid container is pushed.
                let container_version_ptr = Atomic::new(Version::new());
                let container_version_shared =
                    container_version_ptr.load(Relaxed, unsafe { crossbeam_epoch::unprotected() });
                if anchor_ref.push(container_version_shared, snapshot, &guard) {
                    if journal
                        .create(
                            unsafe { container_version_shared.deref() },
                            Some(ContainerHandle::null()),
                        )
                        .is_ok()
                    {
                        return true;
                    }
                } else {
                    drop(unsafe { container_version_shared.into_owned() });
                }
                false
            }) {
                return result;
            }
        }
        false
    }

    /// Unloads the container from memory.
    pub fn unload(&self) -> Result<(), Error> {
        Err(Error::Fail)
    }
}

/// ContainerData defines the data container interfaces.
///
/// A container is a two-dimensional plane of data.
pub trait ContainerData<S: Sequencer> {
    /// Gets the data located at the given position.
    fn get(
        &self,
        record_index: usize,
        column_index: usize,
        snapshot: &Snapshot<S>,
    ) -> Option<&[u8]>;

    /// Updates the data stored at the given position.
    ///
    /// It returns the new position of the updated data.
    fn update(
        &self,
        record_index: usize,
        column_index: usize,
        data: (&[u8], usize),
        transaction: &Transaction<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<(usize, usize), Error>;

    /// Puts the data into the container.
    fn put(
        &self,
        data: (&[u8], usize),
        transaction: &Transaction<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<usize, Error>;

    /// Removes the data stored at the given position.
    fn remove(
        &self,
        record_index: usize,
        column_index: usize,
        transaction: &Transaction<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<(usize, usize), Error>;

    /// Returns the size of the container.
    fn size(&self) -> (usize, usize);
}

/// [Container] can either be [Data](Type::Data) or [Directory](Type::Directory).
enum Type<S: Sequencer> {
    /// A two dimensional data plane.
    Data(Box<dyn ContainerData<S> + Send + Sync>),

    /// A directory has links to child containers.
    Directory(TreeIndex<String, Arc<ContainerAnchor<S>>>),
}

/// ContainerAnchor is the only access path to Container instances.
struct ContainerAnchor<S: Sequencer> {
    version_link: Atomic<Version<S>>,
}

impl<S: Sequencer> ContainerAnchor<S> {
    fn new() -> ContainerAnchor<S> {
        ContainerAnchor {
            version_link: Atomic::null(),
        }
    }

    /// Pushes a new version into the version chain.
    ///
    /// Pushes the version only if the previous version is visible to the given snapshot.
    fn push<'g>(
        &self,
        version_shared: Shared<'g, Version<S>>,
        snapshot: &Snapshot<S>,
        guard: &'g Guard,
    ) -> bool {
        let version_ref = unsafe { version_shared.deref() };
        debug_assert!(!version_ref.predate(snapshot, guard));
        let mut current_version_shared = self.version_link.load(Relaxed, guard);
        loop {
            if !current_version_shared.is_null()
                && !unsafe { current_version_shared.deref().predate(snapshot, guard) }
            {
                // The latest version is invisible to the given snapshot.
                //
                // It is regarded as a serialization failure error, assuming that the majority of transaction are committed,
                // thus returning an error immediately.
                return false;
            }
            version_ref.link.store(current_version_shared, Relaxed);
            match self.version_link.compare_exchange(
                current_version_shared,
                version_shared,
                Release,
                Relaxed,
                guard,
            ) {
                Ok(_) => return true,
                Err(current) => current_version_shared = current.current,
            }
        }
    }

    /// Returns a reference to the latest Container among those predate the given snapshot.
    fn get<'g>(&self, snapshot: &Snapshot<S>, guard: &'g Guard) -> Option<&'g Container<S>> {
        let mut current_version_shared = self.version_link.load(Acquire, guard);
        while !current_version_shared.is_null() {
            let current_version_ref = unsafe { current_version_shared.deref() };
            if current_version_ref.predate(snapshot, guard) {
                return current_version_ref.container_handle.get(&guard);
            }
            current_version_shared = current_version_ref.link.load(Acquire, guard);
        }
        None
    }
}

impl<S: Sequencer> Drop for ContainerAnchor<S> {
    fn drop(&mut self) {
        // The anchor becomes unreachable, so does the linked list.
        //
        // ContainerAnchor is subject to the epoch reclamation mechanism, hence the fact that it has become unreachable,
        // implies that there is no reader referencing a ContainerVersion attached to it.
        let guard = unsafe { crossbeam_epoch::unprotected() };
        let mut current_version_shared = self.version_link.load(Acquire, guard);
        while !current_version_shared.is_null() {
            let current_version_ref = unsafe { current_version_shared.deref() };
            let current_version_cell_shared =
                current_version_ref
                    .version_cell
                    .swap(Shared::null(), Relaxed, guard);
            if !current_version_cell_shared.is_null() {
                drop(unsafe { current_version_cell_shared.into_owned() });
            }
            let next_version_shared = current_version_ref
                .link
                .swap(Shared::null(), Relaxed, guard);
            drop(unsafe { current_version_shared.into_owned() });
            current_version_shared = next_version_shared;
        }
    }
}

/// [Version] implements the [Version](super::Version) trait, by implementing a linked list of
/// [Container] instance.
struct Version<S: Sequencer> {
    version_cell: ebr::AtomicArc<VersionCell<S>>,
    container: ebr::AtomicArc<Container<S>>,
    link: ebr::AtomicArc<Container<S>>,
}

impl<S: Sequencer> Version<S> {
    fn new() -> Version<S> {
        Version {
            version_cell: Atomic::new(VersionCell::new()),
            container: ContainerHandle::null(),
            link: Atomic::null(),
        }
    }
}

impl<S: Sequencer> VersionTrait<S> for Version<S> {
    type Data = ContainerHandle<S>;
    fn version_cell_ptr<'g>(&self, guard: &'g Guard) -> Shared<'g, VersionCell<S>> {
        self.version_cell.load(Acquire, guard)
    }
    fn write(&mut self, payload: ContainerHandle<S>) -> Option<Log> {
        self.container = payload;
        None
    }
    fn read(&self, snapshot: &Snapshot<S>, guard: &Guard) -> Option<&ContainerHandle<S>> {
        if self.predate(snapshot, guard) {
            Some(&self.container)
        } else {
            None
        }
    }
    fn consolidate(&self, guard: &Guard) -> bool {
        let version_cell_shared = self.version_cell.swap(Shared::null(), Relaxed, guard);
        if version_cell_shared.is_null() {
            false
        } else {
            unsafe { guard.defer_destroy(version_cell_shared) };
            true
        }
    }
}
