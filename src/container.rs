// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Journal, Sequencer, Snapshot, Transaction, Version, VersionCell};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use scc::TreeIndex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// A transactional data container.
///
/// Container is a container of organized data. The data organization is specified in the
/// metadata of the container. A container may point to external data sources, or embed all the
/// data inside it.
///
/// A Container may hold references to other Container instances, and those holding Container
/// references are called a directory.
pub struct Container<S: Sequencer> {
    /// It is either a Data or Directory container.
    container: ContainerType<S>,
    /// Container is reference counted.
    references: AtomicUsize,
}

impl<S: Sequencer> Container<S> {
    /// Creates a new directory container.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Container, ContainerHandle};
    /// type Handle = ContainerHandle<AtomicCounter>;
    ///
    /// let container_handle: Handle = Container::new_directory();
    /// ```
    pub fn new_directory() -> ContainerHandle<S> {
        ContainerHandle {
            container_ptr: Atomic::new(Container {
                container: ContainerType::Directory(Atomic::null()),
                references: AtomicUsize::new(1),
            }),
        }
    }

    /// Creates a new data container.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Container, ContainerHandle, RelationalTable};
    /// type Handle = ContainerHandle<AtomicCounter>;
    ///
    /// let container_data = Box::new(RelationalTable::new());
    /// let container_handle: Handle = Container::new_container(container_data);
    /// ```
    pub fn new_container(
        container_data: Box<dyn ContainerData<S> + Send + Sync>,
    ) -> ContainerHandle<S> {
        ContainerHandle {
            container_ptr: Atomic::new(Container {
                container: ContainerType::Data(container_data),
                references: AtomicUsize::new(1),
            }),
        }
    }

    /// Creates a new container handle out of self.
    pub fn create_handle(&self) -> Option<ContainerHandle<S>> {
        // Tries to increment the reference count by one.
        let mut prev_ref = self.references.load(Relaxed);
        loop {
            if prev_ref == 0 {
                return None;
            }
            match self
                .references
                .compare_exchange(prev_ref, prev_ref + 1, Relaxed, Relaxed)
            {
                Ok(_) => {
                    return Some(ContainerHandle {
                        container_ptr: Atomic::from(self as *const _),
                    })
                }
                Err(value) => prev_ref = value,
            }
        }
    }

    /// Creates a new container directory under the given name.
    pub fn create_directory(
        &self,
        name: &str,
        snapshot: &Snapshot<S>,
        journal: &mut Journal<S>,
    ) -> Option<ContainerHandle<S>> {
        let guard = crossbeam_epoch::pin();
        if let ContainerType::Directory(directory) = &self.container {
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
                        directory_ref.read(&String::from(name), |_, anchor_ptr| {
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
                        let new_directory_version_ptr =
                            Atomic::new(ContainerVersion::new(new_directory.clone()));
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
                            .lock(unsafe { new_directory_version_shared.deref() })
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
    pub fn search<'g>(
        &self,
        name: &str,
        snapshot: &Snapshot<S>,
        guard: &'g Guard,
    ) -> Option<&'g Container<S>> {
        if let ContainerType::Directory(directory) = &self.container {
            let directory_shared = directory.load(Relaxed, guard);
            if !directory_shared.is_null() {
                if let Some(Some(existing_container)) = unsafe { directory_shared.deref() }.read(
                    &String::from(name),
                    |_, anchor_ptr| {
                        let anchor_ref = unsafe { anchor_ptr.load(Acquire, &guard).deref() };
                        anchor_ref.get(snapshot, &guard)
                    },
                ) {
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
            (ContainerType::Directory(directory), Some(container)) => {
                if let ContainerType::Data(_) = &container.container {
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
                    }

                    let directory_ref = unsafe { directory_shared_ptr.deref() };
                    loop {
                        if let Some(result) =
                            directory_ref.read(&String::from(name), |_, anchor_ptr| {
                                let anchor_ref =
                                    unsafe { anchor_ptr.load(Acquire, &guard).deref() };
                                let container_version_ptr =
                                    Atomic::new(ContainerVersion::new(container_handle.clone()));
                                let container_version_shared = container_version_ptr
                                    .load(Relaxed, unsafe { crossbeam_epoch::unprotected() });
                                if anchor_ref.push(container_version_shared, snapshot, &guard) {
                                    if journal
                                        .lock(unsafe { container_version_shared.deref() })
                                        .is_ok()
                                    {
                                        return true;
                                    }
                                } else {
                                    drop(unsafe { container_version_shared.into_owned() });
                                }
                                false
                            })
                        {
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
        if let ContainerType::Directory(directory) = &self.container {
            let guard = crossbeam_epoch::pin();
            let directory_shared_ptr = directory.load(Relaxed, &guard);
            let directory_ref = unsafe { directory_shared_ptr.deref() };
            let name = String::from(name);
            if let Some(result) = directory_ref.read(&name, |_, anchor_ptr| {
                let anchor_ref = unsafe { anchor_ptr.load(Acquire, &guard).deref() };
                // A ContainerVersion not pointing to a valid container is pushed.
                let container_version_ptr =
                    Atomic::new(ContainerVersion::new(ContainerHandle::null()));
                let container_version_shared =
                    container_version_ptr.load(Relaxed, unsafe { crossbeam_epoch::unprotected() });
                if anchor_ref.push(container_version_shared, snapshot, &guard) {
                    if journal
                        .lock(unsafe { container_version_shared.deref() })
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

/// A ref-counted handle for Container.
pub struct ContainerHandle<S: Sequencer> {
    container_ptr: Atomic<Container<S>>,
}

impl<S: Sequencer> ContainerHandle<S> {
    /// Creates a null handle.
    fn null() -> ContainerHandle<S> {
        ContainerHandle {
            container_ptr: Atomic::null(),
        }
    }

    /// Gets a reference to the container.
    pub fn get<'g>(&self, guard: &'g Guard) -> Option<&'g Container<S>> {
        let container_shared = self.container_ptr.load(Relaxed, guard);
        if container_shared.is_null() {
            None
        } else {
            Some(unsafe { container_shared.deref() })
        }
    }
}

impl<S: Sequencer> Clone for ContainerHandle<S> {
    fn clone(&self) -> Self {
        // The Container instance is protected by a positive reference count.
        self.get(unsafe { crossbeam_epoch::unprotected() })
            .map(|container| container.references.fetch_add(1, Relaxed));
        ContainerHandle {
            container_ptr: self.container_ptr.clone(),
        }
    }
}

impl<S: Sequencer> Drop for ContainerHandle<S> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        let container_shared = self.container_ptr.load(Relaxed, &guard);
        if !container_shared.is_null() {
            unsafe {
                if container_shared.deref().references.fetch_sub(1, Relaxed) == 1 {
                    guard.defer_destroy(container_shared);
                }
            }
        }
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

/// ContainerDirectory is a tree storing versioned handles to sub containers.
///
/// Traversing a container path requires a Snapshot.
type ContainerDirectory<S> = TreeIndex<String, Atomic<ContainerAnchor<S>>>;

/// Container can either be Data or Directory.
enum ContainerType<S: Sequencer> {
    /// A two dimensional data plane.
    Data(Box<dyn ContainerData<S> + Send + Sync>),
    /// Directory has child containers without managing data.
    Directory(Atomic<ContainerDirectory<S>>),
}

/// ContainerAnchor is the only access path to Container instances.
struct ContainerAnchor<S: Sequencer> {
    version_link: Atomic<ContainerVersion<S>>,
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
        version_shared: Shared<'g, ContainerVersion<S>>,
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

/// ContainerVersion implements the Version trait, embeding a Container.
///
/// ContainerVersion forms a linked list of containers under the same path.
struct ContainerVersion<S: Sequencer> {
    /// version_cell represents the creation time point.
    version_cell: Atomic<VersionCell<S>>,
    /// container_handle pointing to nothing represents a state where the container has been removed.
    container_handle: ContainerHandle<S>,
    /// link points to the immediate adjacent version of the Container.
    link: Atomic<ContainerVersion<S>>,
}

impl<S: Sequencer> ContainerVersion<S> {
    fn new(container_handle: ContainerHandle<S>) -> ContainerVersion<S> {
        ContainerVersion {
            version_cell: Atomic::new(VersionCell::new()),
            container_handle,
            link: Atomic::null(),
        }
    }
}

impl<S: Sequencer> Version<S> for ContainerVersion<S> {
    type Data = ContainerVersion<S>;
    fn version_cell<'g>(&self, guard: &'g Guard) -> Shared<'g, VersionCell<S>> {
        self.version_cell.load(Acquire, guard)
    }
    fn read(&self) -> &ContainerVersion<S> {
        self
    }
    fn unversion(&self, guard: &Guard) -> bool {
        !self
            .version_cell
            .swap(Shared::null(), Relaxed, guard)
            .is_null()
    }
}
