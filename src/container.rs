// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::version::Owner;
use super::Version as VersionTrait;
use super::{DataPlane, Error, Journal, Sequencer, Snapshot};

use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::Arc;
use std::time::Duration;

use scc::ebr;
use scc::TreeIndex;

/// [Container] is an organized data container that is transactionally updated.
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
    /// use tss::{AtomicCounter, Container};
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
    /// use tss::{AtomicCounter, Container, RelationalTable};
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
        timeout: Option<Duration>,
    ) -> Option<ebr::Arc<Container<S>>> {
        let barrier = ebr::Barrier::new();
        if let Type::Directory(directory) = &self.container {
            loop {
                if let Some(result) = directory.read(name, |_, anchor| {
                    let new_version_ptr = anchor.install(snapshot, &barrier);
                    if let Some(new_version) = new_version_ptr.try_into_arc() {
                        let new_directory = Container::new_directory();
                        #[allow(clippy::blocks_in_if_conditions)]
                        if journal
                            .create(
                                &*new_version,
                                |c| {
                                    *c = ebr::AtomicArc::from(new_directory.clone());
                                    Ok(None)
                                },
                                timeout,
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
        timeout: Option<Duration>,
    ) -> bool {
        let barrier = ebr::Barrier::new();
        if let Type::Directory(directory) = &self.container {
            loop {
                if let Some(result) = directory.read(name, |_, anchor| {
                    let new_version_ptr = anchor.install(snapshot, &barrier);
                    if let Some(new_version) = new_version_ptr.try_into_arc() {
                        #[allow(clippy::blocks_in_if_conditions)]
                        if journal
                            .create(
                                &*new_version,
                                |c| {
                                    *c = ebr::AtomicArc::from(container.clone());
                                    Ok(None)
                                },
                                timeout,
                            )
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
    pub fn unlink(
        &self,
        name: &str,
        snapshot: &Snapshot<S>,
        journal: &mut Journal<S>,
        timeout: Option<Duration>,
    ) -> bool {
        let barrier = ebr::Barrier::new();
        if let Type::Directory(directory) = &self.container {
            if let Some(result) = directory.read(name, |_, anchor| {
                let new_version_ptr = anchor.install(snapshot, &barrier);
                if let Some(new_version) = new_version_ptr.try_into_arc() {
                    let deleted_version = ebr::AtomicArc::null();
                    deleted_version.update_tag_if(ebr::Tag::First, |_| true, Relaxed);
                    #[allow(clippy::blocks_in_if_conditions)]
                    if journal
                        .create(
                            &*new_version,
                            |c| {
                                *c = deleted_version;
                                Ok(None)
                            },
                            timeout,
                        )
                        .is_ok()
                    {
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

    /// Reclaims unreachable resources held by the container.
    ///
    /// It calls the [vacuum](DataPlane::vacuum) method on all the containers attached to it.
    ///
    /// # Errors
    ///
    /// It returns an error is vacuuming could not be completed.
    pub fn vacuum(
        &self,
        snapshot: &Snapshot<S>,
        min_snapshot_clock: S::Clock,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        match &self.container {
            Type::Directory(directory) => {
                let barrier = ebr::Barrier::new();
                if directory.iter(&barrier).any(|(_, c)| {
                    c.vacuum(min_snapshot_clock, &barrier);
                    c.get(snapshot, &barrier).as_ref().map_or(false, |c| {
                        c.vacuum(snapshot, min_snapshot_clock, timeout).is_err()
                    })
                }) {
                    Err(Error::Fail)
                } else {
                    Ok(())
                }
            }
            Type::Data(data_plane) => data_plane.vacuum(min_snapshot_clock, timeout),
        }
    }
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

    /// Returns a reference to the latest [Container] among those that predate the snapshot.
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

    /// Unlinks obsolete container versions.
    fn vacuum(&self, min_snapshot_clock: S::Clock, barrier: &ebr::Barrier) {
        let mut current_version_ptr = self.version_link.load(Acquire, barrier);
        while let Some(current_version_ref) = current_version_ptr.as_ref() {
            if current_version_ref.try_consolidate(min_snapshot_clock, barrier) {
                // All the versions older the the current one can be discarded.
                if !current_version_ref.link.load(Relaxed, barrier).is_null() {
                    current_version_ref
                        .link
                        .swap((None, ebr::Tag::None), Relaxed);
                }
            }
            current_version_ptr = current_version_ref.link.load(Acquire, barrier);
        }
    }
}

/// [Version] implements the [Version](super::Version) trait, by implementing a linked list of
/// [Container] instance.
struct Version<S: Sequencer> {
    /// Its [Owner].
    owner: Owner<S>,

    /// `container` being tagged indicates that the [Container] is deleted.
    container: ebr::AtomicArc<Container<S>>,

    /// A link to its predecessor.
    link: ebr::AtomicArc<Version<S>>,
}

impl<S: Sequencer> Version<S> {
    fn new() -> Version<S> {
        Version {
            owner: Owner::default(),
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

    fn owner_field<'b>(&self) -> &Owner<S> {
        &self.owner
    }

    fn data_ref(&self) -> &Self::Data {
        &self.container
    }
}

#[cfg(test)]
mod test {
    use crate::{AtomicCounter, Storage};
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn insert_delete_vacuum() {
        let storage: Arc<Storage<AtomicCounter>> = Arc::new(Storage::new(None));

        let transaction = storage.transaction();
        let snapshot = transaction.snapshot();
        let mut journal = transaction.start();
        let result = storage.create_directory("/test/directory", &snapshot, &mut journal, None);
        assert!(result.is_ok());
        assert_eq!(journal.submit(), 1);
        drop(snapshot);
        drop(transaction.commit());

        let num_threads = 4;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let mut thread_handles = Vec::new();
        for _ in 0..num_threads {
            let storage_cloned = storage.clone();
            let barrier_cloned = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_cloned.wait();

                let transaction = storage_cloned.transaction();
                let snapshot = transaction.snapshot();
                let mut journal = transaction.start();
                let _result =
                    storage_cloned.remove("/test/directory", &snapshot, &mut journal, None);
                assert_eq!(journal.submit(), 1);
                drop(snapshot);
                drop(transaction.commit());
            }));
        }

        barrier.wait();
        drop(storage);

        thread_handles
            .into_iter()
            .for_each(|t| assert!(t.join().is_ok()));
    }
}
