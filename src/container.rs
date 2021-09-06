// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::version::Cell;
use super::Version as VersionTrait;
use super::{Error, Journal, Log, Sequencer, Snapshot, Transaction};

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
    #[must_use]
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
    #[must_use]
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
                if let Some(result) = directory.read(name, |_, anchor| {
                    let new_version_ptr = anchor.install(snapshot, &barrier);
                    if let Some(new_version) = new_version_ptr.try_into_arc() {
                        let new_directory = Container::new_directory();
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
                    }
                    None
                }) {
                    return result;
                }
                let _result = directory.insert(String::from(name), Arc::new(Anchor::new()));
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
                directory.read(name, |_, anchor| anchor.get(snapshot, barrier))
            {
                return container_ptr;
            }
        }
        ebr::Ptr::null()
    }

    /// Makes a link to the given data container if the current container is a directory.
    ///
    /// It overwrites an existing visible container if the container is the latest version under the same name.
    #[allow(clippy::needless_pass_by_value)]
    pub fn link(
        &self,
        name: &str,
        container: ebr::Arc<Container<S>>,
        snapshot: &Snapshot<S>,
        journal: &mut Journal<S>,
    ) -> bool {
        let barrier = ebr::Barrier::new();
        if let Type::Directory(directory) = &self.container {
            loop {
                if let Some(result) = directory.read(name, |_, anchor| {
                    let new_version_ptr = anchor.install(snapshot, &barrier);
                    if let Some(new_version) = new_version_ptr.try_into_arc() {
                        if journal
                            .create(&*new_version, Some(ebr::AtomicArc::from(container.clone())))
                            .is_ok()
                        {
                            // The transaction took ownership.
                            return true;
                        }
                    }
                    false
                }) {
                    return result;
                }
                let _result = directory.insert(String::from(name), Arc::new(Anchor::new()));
            }
        }
        false
    }

    /// Unlinks a container associated with the given name.
    pub fn unlink(&self, name: &str, snapshot: &Snapshot<S>, journal: &mut Journal<S>) -> bool {
        let barrier = ebr::Barrier::new();
        if let Type::Directory(directory) = &self.container {
            if let Some(result) = directory.read(name, |_, anchor| {
                let new_version_ptr = anchor.install(snapshot, &barrier);
                if let Some(new_version) = new_version_ptr.try_into_arc() {
                    let deleted_version = ebr::AtomicArc::null();
                    deleted_version.update_tag_if(ebr::Tag::First, |_| true, Relaxed);
                    if journal.create(&*new_version, Some(deleted_version)).is_ok() {
                        // The transaction successfully deleted it.
                        return true;
                    }
                }
                false
            }) {
                return result;
            }
        }
        false
    }

    /// Unloads the [Container] from memory.
    ///
    /// # Errors
    ///
    /// An error is returned on failure.
    #[allow(clippy::unused_self)]
    pub fn unload(&self) -> Result<(), Error> {
        Err(Error::Fail)
    }
}

/// [`DataPlane`] defines the data container interfaces.
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

    /// Installs a new unowned version into the version chain.
    ///
    /// It returns a pointer to the newest unowned version. It pushes the version only if
    /// the previous version is visible to the given [Snapshot], and if not, it returns a null
    /// pointer.
    fn install<'b>(
        &self,
        snapshot: &Snapshot<S>,
        barrier: &'b ebr::Barrier,
    ) -> ebr::Ptr<'b, Version<S>> {
        let mut new_version: Option<ebr::Arc<Version<S>>> = None;
        let mut current_version_ptr = self.version_link.load(Relaxed, barrier);
        loop {
            if let Some(current_version_ref) = current_version_ptr.as_ref() {
                if current_version_ref.unowned(barrier) {
                    // An unowned version is at the head of the linked list.
                    return current_version_ptr;
                } else if !current_version_ref.predate(snapshot, barrier) {
                    // The latest version is invisible to the given snapshot.
                    //
                    // It is regarded as a serialization failure error.
                    break;
                }
            }
            new_version
                .get_or_insert(ebr::Arc::new(Version::new()))
                .link
                .swap(
                    (current_version_ptr.try_into_arc(), ebr::Tag::None),
                    Relaxed,
                );
            match self.version_link.compare_exchange(
                current_version_ptr,
                (new_version.take(), ebr::Tag::None),
                Release,
                Relaxed,
            ) {
                Ok((_, current_ptr)) => return current_ptr,
                Err((passed, actual)) => {
                    if let Some(passed) = passed {
                        new_version.replace(passed);
                    }
                    current_version_ptr = actual;
                }
            }
        }
        ebr::Ptr::null()
    }

    /// Returns a reference to the latest [Container] among those predate the given snapshot.
    fn get<'b>(
        &self,
        snapshot: &Snapshot<S>,
        barrier: &'b ebr::Barrier,
    ) -> ebr::Ptr<'b, Container<S>> {
        let mut current_version_ptr = self.version_link.load(Acquire, barrier);
        while let Some(current_version_ref) = current_version_ptr.as_ref() {
            if !current_version_ref.unowned(barrier)
                && current_version_ref.predate(snapshot, barrier)
            {
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
    /// Its [VersionCell].
    version_cell: ebr::AtomicArc<Cell<S>>,

    /// `container` being tagged indicates that the [Container] is deleted.
    container: ebr::AtomicArc<Container<S>>,

    /// A link to its predecessor.
    link: ebr::AtomicArc<Version<S>>,
}

impl<S: Sequencer> Version<S> {
    fn new() -> Version<S> {
        Version {
            version_cell: ebr::AtomicArc::new(Cell::default()),
            container: ebr::AtomicArc::null(),
            link: ebr::AtomicArc::null(),
        }
    }

    fn unowned(&self, barrier: &ebr::Barrier) -> bool {
        let container_ptr = self.container.load(Relaxed, barrier);
        container_ptr.is_null() && container_ptr.tag() == ebr::Tag::None
    }
}

impl<S: Sequencer> VersionTrait<S> for Version<S> {
    type Data = ebr::AtomicArc<Container<S>>;

    fn version_cell_ptr<'b>(&self, barrier: &'b ebr::Barrier) -> ebr::Ptr<'b, Cell<S>> {
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
