// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::Version as VersionTrait;
use super::{Error, Journal, Log, Sequencer, Snapshot, Transaction, VersionCell};

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
        container_data: Box<dyn DataPlane<S> + Send + Sync>,
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
        let barrier = ebr::Barrier::new();
        if let Type::Directory(directory) = &self.container {
            loop {
                if let Some(anchor) = directory.read(name, |_, anchor| anchor) {
                    let container_ptr = anchor.get(snapshot, &barrier);
                    if let Some(container) = container_ptr.try_into_arc() {
                        return Some(container);
                    }
                    let new_directory = Self::new_directory();
                    let new_version = ebr::Arc::new(Version::new());
                    if anchor.push(new_version, snapshot, &barrier) {
                        if journal
                            .create(
                                &*new_version,
                                Some(ebr::AtomicArc::from(new_directory.clone())),
                            )
                            .is_ok()
                        {
                            // The transaction took ownership.
                            return Some(new_directory);
                        }
                        continue;
                    }
                    return None;
                }
                directory.insert(String::from(name), Arc::new(Anchor::new()));
            }
        }
        None
    }

    /// Searches for a [Container] associated with the given name.
    pub fn search<'b>(
        &self,
        name: &str,
        snapshot: &Snapshot<S>,
        barrier: &'b ebr::Barrier,
    ) -> ebr::Ptr<'b, Container<S>> {
        if let Type::Directory(directory) = &self.container {
            if let Some(container_ptr) =
                directory.read(name, |_, anchor| anchor.get(snapshot, &barrier))
            {
                return container_ptr;
            }
        }
        ebr::Ptr::null()
    }

    /// Makes a link to the given data container if the current container is a directory.
    ///
    /// It overwrites an existing visible container if the container is the latest version under the same name.
    pub fn link(
        &self,
        name: &str,
        container: ebr::Arc<Container<S>>,
        snapshot: &Snapshot<S>,
        journal: &mut Journal<S>,
    ) -> bool {
        let barrier = ebr::Barrier::new();
        match &self.container {
            Type::Directory(directory) => {
                if let Type::Data(_) = &container.container {
                    loop {
                        if let Some(result) = directory.read(name, |_, anchor| {
                            // Creates a new ContainerVersion for the given container.
                            let new_version = ebr::Arc::new(Version::new());
                            // Pushes the new ContainerVersion into the version chain.
                            if anchor.push(new_version, snapshot, &barrier) {
                                // Successfully pushed the new ContainerVersion.
                                //
                                // The new ContainerVersion is now owned by the transaction,
                                // and therefore the transaction tries to take ownership.
                                if journal
                                    .create(
                                        &*new_version,
                                        Some(ebr::AtomicArc::from(container.clone())),
                                    )
                                    .is_ok()
                                {
                                    // The transaction took ownership.
                                    return true;
                                }
                            }
                            // Failed to push the new ContainerVersion.
                            //
                            // It can be regarded as a serialization failure.
                            false
                        }) {
                            return result;
                        }

                        // Needs to insert a new container anchor.
                        let new_container_anchor_ptr = Atomic::new(Anchor::new());
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
            let barrier = ebr::Barrier::new();
            if let Some(result) = directory.read(name, |_, anchor| {
                // A ContainerVersion not pointing to a valid container is pushed.
                let container_version_ptr = Atomic::new(Version::new());
                let container_version_shared =
                    container_version_ptr.load(Relaxed, unsafe { crossbeam_epoch::unprotected() });
                if anchor.push(container_version_shared, snapshot, &barrier) {
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

    /// Unloads the [Container] from memory.
    pub fn unload(&self) -> Result<(), Error> {
        Err(Error::Fail)
    }
}

/// [DataPlane] defines the data container interfaces.
///
/// A container is a two-dimensional plane of data.
pub trait DataPlane<S: Sequencer> {
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
    Data(Box<dyn DataPlane<S> + Send + Sync>),

    /// A directory has links to child containers.
    Directory(TreeIndex<String, Arc<Anchor<S>>>),
}

/// [Anchor] is the only access path to [Container] instances.
struct Anchor<S: Sequencer> {
    version_link: ebr::AtomicArc<Version<S>>,
}

impl<S: Sequencer> Anchor<S> {
    fn new() -> Anchor<S> {
        Anchor {
            version_link: ebr::AtomicArc::null(),
        }
    }

    /// Pushes a new version into the version chain.
    ///
    /// Pushes the version only if the previous version is visible to the given [Snapshot].
    fn push<'b>(
        &self,
        mut new_version: ebr::Arc<Version<S>>,
        snapshot: &Snapshot<S>,
        barrier: &'b ebr::Barrier,
    ) -> bool {
        debug_assert!(!new_version.predate(snapshot, barrier));
        let mut current_version_ptr = self.version_link.load(Relaxed, barrier);
        loop {
            if let Some(current_version_ref) = current_version_ptr.as_ref() {
                if !current_version_ref.predate(snapshot, barrier) {
                    // The latest version is invisible to the given snapshot.
                    //
                    // It is regarded as a serialization failure error.
                    return false;
                }
            }
            new_version.link.swap(
                (current_version_ptr.try_into_arc(), ebr::Tag::None),
                Relaxed,
            );
            match self.version_link.compare_exchange(
                current_version_ptr,
                (Some(new_version), ebr::Tag::None),
                Release,
                Relaxed,
            ) {
                Ok(_) => return true,
                Err((passed, actual)) => {
                    new_version = passed.unwrap();
                    current_version_ptr = actual;
                }
            }
        }
    }

    /// Returns a reference to the latest [Container] among those predate the given snapshot.
    fn get<'b>(
        &self,
        snapshot: &Snapshot<S>,
        barrier: &'b ebr::Barrier,
    ) -> ebr::Ptr<'b, Container<S>> {
        let mut current_version_ptr = self.version_link.load(Acquire, barrier);
        while let Some(current_version_ref) = current_version_ptr.as_ref() {
            if current_version_ref.predate(snapshot, barrier) {
                return current_version_ref.container.load(Relaxed, barrier);
            }
            current_version_ptr = current_version_ref.link.load(Acquire, barrier);
        }
        ebr::Ptr::null()
    }
}

/// [Version] implements the [Version](super::Version) trait, by implementing a linked list of
/// [Container] instance.
struct Version<S: Sequencer> {
    version_cell: ebr::AtomicArc<VersionCell<S>>,
    container: ebr::AtomicArc<Container<S>>,
    link: ebr::AtomicArc<Version<S>>,
}

impl<S: Sequencer> Version<S> {
    fn new() -> Version<S> {
        Version {
            version_cell: ebr::AtomicArc::new(VersionCell::default()),
            container: ebr::AtomicArc::null(),
            link: ebr::AtomicArc::null(),
        }
    }
}

impl<S: Sequencer> VersionTrait<S> for Version<S> {
    type Data = ebr::AtomicArc<Container<S>>;

    fn version_cell_ptr<'b>(&self, barrier: &'b ebr::Barrier) -> ebr::Ptr<'b, VersionCell<S>> {
        self.version_cell.load(Acquire, barrier)
    }

    fn write(&mut self, payload: Self::Data) -> Option<Log> {
        self.container = payload;
        None
    }

    fn read(&self, snapshot: &Snapshot<S>, barrier: &ebr::Barrier) -> Option<&Self::Data> {
        if self.predate(snapshot, barrier) {
            Some(&self.container)
        } else {
            None
        }
    }

    fn consolidate(&self, barrier: &ebr::Barrier) -> bool {
        if let Some(version_cell) = self.version_cell.swap((None, ebr::Tag::None), Relaxed) {
            barrier.reclaim(version_cell);
            return true;
        }
        false
    }
}
