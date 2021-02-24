// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Journal, Sequencer, Snapshot, Transaction, Version, VersionCell};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use scc::TreeIndex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

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

/// DefaultContainerData is a two dimensional array of u8.
pub struct DefaultContainerData {
    _data: Vec<Vec<u8>>,
}

impl DefaultContainerData {
    pub fn new() -> DefaultContainerData {
        DefaultContainerData { _data: Vec::new() }
    }
}

impl<S: Sequencer> ContainerData<S> for DefaultContainerData {
    fn get(
        &self,
        _record_index: usize,
        _column_index: usize,
        _snapshot: &Snapshot<S>,
    ) -> Option<&[u8]> {
        None
    }
    fn update(
        &self,
        _record_index: usize,
        _column_index: usize,
        _data: (&[u8], usize),
        _transaction: &Transaction<S>,
        _snapshot: &Snapshot<S>,
    ) -> Result<(usize, usize), Error> {
        Err(Error::Fail)
    }
    fn put(
        &self,
        _data: (&[u8], usize),
        _transaction: &Transaction<S>,
        _snapshot: &Snapshot<S>,
    ) -> Result<usize, Error> {
        Err(Error::Fail)
    }
    fn remove(
        &self,
        _record_index: usize,
        _column_index: usize,
        _transaction: &Transaction<S>,
        _snapshot: &Snapshot<S>,
    ) -> Result<(usize, usize), Error> {
        Err(Error::Fail)
    }
    fn size(&self) -> (usize, usize) {
        (0, 0)
    }
}

/// ContainerDirectory is a tree storing versioned handles to sub containers.
///
/// Traversing a container path requires a Snapshot.
type ContainerDirectory<S> = TreeIndex<String, ContainerVersion<S>>;

/// Container can either be Data or Directory.
enum ContainerType<S: Sequencer> {
    /// A two dimensional data plane.
    Data(Box<dyn ContainerData<S> + Send + Sync>),
    /// Directory has child containers without managing data.
    Directory(Atomic<ContainerDirectory<S>>),
}

/// ContainerVersion implements the Version trait, embeding a Container.
///
/// ContainerVersion forms a linked list of containers under the same path.
pub struct ContainerVersion<S: Sequencer> {
    /// version_cell represents the creation time point.
    version_cell: Atomic<VersionCell<S>>,
    /// container_handle pointing to nothing represents a state where the container has been removed.
    container_handle: ContainerHandle<S>,
    /// previous_version points to the immediate previous version of the Container.
    previous_version: Atomic<ContainerVersion<S>>,
}

impl<S: Sequencer> ContainerVersion<S> {
    fn new(container_handle: ContainerHandle<S>) -> ContainerVersion<S> {
        ContainerVersion {
            version_cell: Atomic::new(VersionCell::new()),
            container_handle,
            previous_version: Atomic::null(),
        }
    }
}

impl<S: Sequencer> Clone for ContainerVersion<S> {
    fn clone(&self) -> Self {
        ContainerVersion {
            version_cell: self.version_cell.clone(),
            container_handle: self.container_handle.clone(),
            previous_version: self.previous_version.clone(),
        }
    }
}

impl<S: Sequencer> Version<S> for ContainerVersion<S> {
    fn version_cell<'g>(&self, guard: &'g Guard) -> Shared<'g, VersionCell<S>> {
        self.version_cell.load(Acquire, guard)
    }
    fn unversion(&self, guard: &Guard) -> bool {
        !self
            .version_cell
            .swap(Shared::null(), Relaxed, guard)
            .is_null()
    }
}

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
    /// use tss::{Container, ContainerHandle, DefaultSequencer};
    /// type Handle = ContainerHandle<DefaultSequencer>;
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

    /// Creates a new default data container.
    ///
    /// # Examples
    /// ```
    /// use tss::{Container, ContainerHandle, DefaultSequencer};
    /// type Handle = ContainerHandle<DefaultSequencer>;
    ///
    /// let container_handle: Handle = Container::new_default_container();
    /// ```
    pub fn new_default_container() -> ContainerHandle<S> {
        ContainerHandle {
            container_ptr: Atomic::new(Container {
                container: ContainerType::Data(Box::new(DefaultContainerData::new())),
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
        journal: &mut Journal<S>,
    ) -> Option<ContainerHandle<S>> {
        let guard = crossbeam_epoch::pin();
        if let ContainerType::Directory(directory) = &self.container {
            let mut directory_shared_ptr = directory.load(Relaxed, &guard);
            if directory_shared_ptr.is_null() {
                match directory.compare_and_set(
                    Shared::null(),
                    Owned::new(TreeIndex::new()),
                    Relaxed,
                    &guard,
                ) {
                    Ok(result) => directory_shared_ptr = result,
                    Err(result) => directory_shared_ptr = result.current,
                }
                let directory_ref = unsafe { directory_shared_ptr.deref() };
                let mut name = String::from(name);
                let new_directory = Self::new_directory();
                let new_container_version = ContainerVersion::new(new_directory.clone());
                loop {
                    if let Err((key, _)) = directory_ref.insert(name, new_container_version.clone())
                    {
                        if let Some(existing_directory) = directory_ref.read(&key, |_, value| {
                            if let Some(ContainerType::Directory(_)) = &value
                                .container_handle
                                .get(&guard)
                                .map(|container| &container.container)
                            {
                                Some(value.container_handle.clone())
                            } else {
                                None
                            }
                        }) {
                            return existing_directory;
                        }
                        name = key;
                    } else {
                        if journal.lock(&new_container_version).is_ok() {
                            return Some(new_directory);
                        } else {
                            return None;
                        }
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
            let directory_shared_ptr = directory.load(Relaxed, guard);
            if !directory_shared_ptr.is_null() {
                if let Some(result) = unsafe {
                    directory_shared_ptr
                        .deref()
                        .read(&String::from(name), |_, value_ref| {
                            if value_ref.predate(snapshot, guard) {
                                value_ref.container_handle.get(guard)
                            } else {
                                None
                            }
                        })
                } {
                    return result;
                }
            }
        }
        None
    }

    /// Links the given data container to itself if it is a directory.
    pub fn link(
        &self,
        name: &str,
        container_handle: ContainerHandle<S>,
        journal: &mut Journal<S>,
    ) -> bool {
        let guard = crossbeam_epoch::pin();
        match (&self.container, container_handle.get(&guard)) {
            (ContainerType::Directory(directory), Some(container)) => {
                if let ContainerType::Data(_) = &container.container {
                    let mut directory_shared_ptr = directory.load(Relaxed, &guard);
                    if directory_shared_ptr.is_null() {
                        match directory.compare_and_set(
                            Shared::null(),
                            Owned::new(TreeIndex::new()),
                            Relaxed,
                            &guard,
                        ) {
                            Ok(result) => directory_shared_ptr = result,
                            Err(result) => directory_shared_ptr = result.current,
                        }
                    }
                    let new_container_version = ContainerVersion::new(container_handle);
                    if unsafe {
                        directory_shared_ptr
                            .deref()
                            .insert(String::from(name), new_container_version.clone())
                            .is_ok()
                    } {
                        if journal.lock(&new_container_version).is_ok() {
                            return true;
                        }
                    }
                }
                false
            }
            (_, _) => false,
        }
    }

    /// Unlinks a container associated with the given name.
    pub fn unlink(&self, name: &str, _journal: &mut Journal<S>) -> bool {
        if let ContainerType::Directory(directory) = &self.container {
            let guard = crossbeam_epoch::pin();
            let directory_shared_ptr = directory.load(Relaxed, &guard);
            if directory_shared_ptr.is_null() {
                false
            } else {
                unsafe { directory_shared_ptr.deref().remove(&String::from(name)) }
            }
        } else {
            false
        }
    }
}

/// A ref-counted handle for Container.
pub struct ContainerHandle<S: Sequencer> {
    container_ptr: Atomic<Container<S>>,
}

impl<S: Sequencer> ContainerHandle<S> {
    /// Creates a null handle.
    fn _null() -> ContainerHandle<S> {
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
