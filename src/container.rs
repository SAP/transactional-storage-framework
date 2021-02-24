// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

extern crate scc;

use super::{Error, Sequencer, Snapshot, Transaction, Version, VersionCell};
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

/// ContainerDirectory is a tree storing handles to sub containers.
type ContainerDirectory<S> = TreeIndex<String, ContainerHandle<S>>;

/// Container can either be Data or Directory.
enum ContainerType<S: Sequencer> {
    /// A two dimensional data plane.
    Data(Box<dyn ContainerData<S> + Send + Sync>),
    /// Directory has child containers without managing data.
    Directory(Atomic<ContainerDirectory<S>>),
    /// Obsolete is a version marker that represents a state where the container is removed.
    _Obsolete,
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
    container: ContainerType<S>,
    references: AtomicUsize,
    version_cell: Atomic<VersionCell<S>>,
    _prev_version: Atomic<Container<S>>,
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
            pointer: Atomic::from(Owned::new(Container {
                container: ContainerType::Directory(Atomic::null()),
                references: AtomicUsize::new(1),
                version_cell: Atomic::new(VersionCell::new()),
                _prev_version: Atomic::null(),
            })),
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
            pointer: Atomic::from(Owned::new(Container {
                container: ContainerType::Data(Box::new(DefaultContainerData::new())),
                references: AtomicUsize::new(1),
                version_cell: Atomic::new(VersionCell::new()),
                _prev_version: Atomic::null(),
            })),
        }
    }

    /// Creates a new container handle out of self.
    ///
    /// The instance may be reachable even when the reference count is zero due to the epoch-based
    /// memory reclamation mechanism.
    ///
    /// # Examples
    /// ```
    /// use tss::{Container, ContainerHandle, DefaultSequencer};
    /// type Handle = ContainerHandle<DefaultSequencer>;
    ///
    /// let container_handle_root: Handle = Container::new_directory();
    /// let container_handle_apple: Handle = Container::new_default_container();
    /// let apple = String::from("apple");
    ///
    /// let guard = crossbeam_epoch::pin();
    /// let root_ref = container_handle_root.get(&guard);
    /// let result = root_ref.link(&apple, container_handle_apple);
    /// assert!(result);
    ///
    /// let apple_ref = root_ref.search(&apple, &guard);
    /// assert!(apple_ref.is_some());
    ///
    /// if let Some(apple_ref) = apple_ref {
    ///     let apple_handle = apple_ref.create_container_handle();
    ///     drop(guard);
    ///     let apple_handle_cloned = apple_handle.clone();
    /// }
    ///
    /// ```
    pub fn create_container_handle(&self) -> Option<ContainerHandle<S>> {
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
                        pointer: Atomic::from(self as *const _),
                    })
                }
                Err(value) => prev_ref = value,
            }
        }
    }

    /// Creates a new container directory under the given name.
    ///
    /// If a directory exists under the given name, returns it.
    ///
    /// # Examples
    /// ```
    /// use tss::{Container, ContainerHandle, DefaultSequencer};
    /// type Handle = ContainerHandle<DefaultSequencer>;
    ///
    /// let container_handle_root: Handle = Container::new_directory();
    /// let apple = "apple";
    ///
    /// let guard = crossbeam_epoch::pin();
    /// let sub_directory = container_handle_root.get(&guard).create_directory(&apple);
    /// assert!(sub_directory.is_some());
    /// ```
    pub fn create_directory(&self, name: &str) -> Option<(ContainerHandle<S>, bool)> {
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
                loop {
                    if let Err((key, _)) = directory_ref.insert(name, new_directory.clone()) {
                        if let Some(existing_directory) = directory_ref.read(&key, |_, value| {
                            if let ContainerType::Directory(_) = &value.get(&guard).container {
                                Some(value.clone())
                            } else {
                                None
                            }
                        }) {
                            return existing_directory.map(|directory| (directory, false));
                        }
                        name = key;
                    } else {
                        return Some((new_directory, true));
                    }
                }
            }
        }
        None
    }

    /// Searches for a container associated with the given name.
    ///
    /// It does not perform a memory write operation, relying on the given guard.
    ///
    /// # Examples
    /// ```
    /// use tss::{Container, ContainerHandle, DefaultSequencer};
    /// type Handle = ContainerHandle<DefaultSequencer>;
    ///
    /// let container_handle_root: Handle = Container::new_directory();
    /// let container_handle_apple: Handle = Container::new_default_container();
    /// let apple = "apple";
    ///
    /// let guard = crossbeam_epoch::pin();
    /// let root_ref = container_handle_root.get(&guard);
    /// let result = root_ref.link(&apple, container_handle_apple);
    /// assert!(result);
    ///
    /// let apple_ref = root_ref.search(&apple, &guard);
    /// assert!(apple_ref.is_some());
    /// ```
    pub fn search<'g>(&self, name: &str, guard: &'g Guard) -> Option<&'g Container<S>> {
        if let ContainerType::Directory(directory) = &self.container {
            let directory_shared_ptr = directory.load(Relaxed, guard);
            if directory_shared_ptr.is_null() {
                None
            } else {
                unsafe {
                    directory_shared_ptr
                        .deref()
                        .read(&String::from(name), |_, value_ref| {
                            value_ref.pointer.load(Relaxed, guard).deref()
                        })
                }
            }
        } else {
            None
        }
    }

    /// Links the given container to the current container.
    ///
    /// The given container cannot be a directory container.
    ///
    /// # Examples
    /// ```
    /// use tss::{Container, ContainerHandle, DefaultSequencer};
    /// type Handle = ContainerHandle<DefaultSequencer>;
    ///
    /// let container_handle_root: Handle = Container::new_directory();
    /// let container_handle_apple: Handle = Container::new_default_container();
    /// let apple = "apple";
    ///
    /// let guard = crossbeam_epoch::pin();
    /// let root_ref = container_handle_root.get(&guard);
    /// let result = root_ref.link(&apple, container_handle_apple);
    /// assert!(result);
    /// ```
    pub fn link(&self, name: &str, container_handle: ContainerHandle<S>) -> bool {
        let guard = crossbeam_epoch::pin();
        match (&self.container, &container_handle.get(&guard).container) {
            (ContainerType::Directory(directory), ContainerType::Data(_)) => {
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
                unsafe {
                    directory_shared_ptr
                        .deref()
                        .insert(String::from(name), container_handle)
                        .is_ok()
                }
            }
            (_, _) => false,
        }
    }

    /// Unlinks a container associated with the given name.
    ///
    /// # Examples
    /// ```
    /// use tss::{Container, ContainerHandle, DefaultSequencer};
    /// type Handle = ContainerHandle<DefaultSequencer>;
    ///
    /// let container_handle_root: Handle = Container::new_directory();
    /// let container_handle_apple: Handle = Container::new_default_container();
    /// let apple = "apple";
    ///
    /// let guard = crossbeam_epoch::pin();
    /// let root_ref = container_handle_root.get(&guard);
    /// let result = root_ref.link(&apple, container_handle_apple);
    /// assert!(result);
    ///
    /// let apple_ref = root_ref.search(&apple, &guard);
    /// assert!(apple_ref.is_some());
    ///
    /// let result = root_ref.unlink(&apple);
    /// assert!(result);
    ///
    /// let apple_ref = root_ref.search(&apple, &guard);
    /// assert!(apple_ref.is_none());
    /// ```
    pub fn unlink(&self, name: &str) -> bool {
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

impl<S: Sequencer> Version<S> for Container<S> {
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

/// A ref-counted handle for Container.
pub struct ContainerHandle<S: Sequencer> {
    pointer: Atomic<Container<S>>,
}

impl<S: Sequencer> ContainerHandle<S> {
    /// Gets a reference to the container.
    pub fn get<'g>(&self, guard: &'g Guard) -> &'g Container<S> {
        unsafe { self.pointer.load(Relaxed, guard).deref() }
    }
}

impl<S: Sequencer> Clone for ContainerHandle<S> {
    fn clone(&self) -> Self {
        let guard = crossbeam_epoch::pin();
        self.get(&guard).references.fetch_add(1, Relaxed);
        ContainerHandle {
            pointer: self.pointer.clone(),
        }
    }
}

impl<S: Sequencer> Drop for ContainerHandle<S> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        let shared_ptr = self.pointer.load(Relaxed, &guard);
        unsafe {
            if shared_ptr.deref().references.fetch_sub(1, Relaxed) == 1 {
                guard.defer_destroy(shared_ptr);
            }
        };
    }
}
