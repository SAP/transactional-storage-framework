// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Container, ContainerHandle, Error, Journal, Logger, Sequencer, Snapshot, Transaction};

/// Storage is a transactional data storage.
///
/// Storage is a collection of internal containers that are hierarchically organized.
/// The organization of containers resembles that of a POSIX file system as it allows symbolic linking
/// and offers basic access control mechanisms.
///
/// Apart from containers being organized like a file system, every piece of data that a Storage
/// manages is multi-versioned, and transactionally updated. Therefore, a Storage can be viewed
/// as the storage layer of a huge database management system, while its flexibility allows the
/// developers and users to develop, or plug-in new features and transaction mechanisms easily.
pub struct Storage<S: Sequencer> {
    /// The logical clock generator of the storage.
    sequencer: S,
    /// The logger of the storage.
    _logger: Option<Box<dyn Logger<S> + Send + Sync>>,
    /// The root container of the storage.
    root_container: ContainerHandle<S>,
}

impl<S: Sequencer> Storage<S> {
    /// Creates a new Storage.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, FileLogger, Storage};
    ///
    /// let logger = Box::new(FileLogger::new("/home/dba/db"));
    /// let storage: Storage<AtomicCounter> = Storage::new(Some(logger));
    /// ```
    pub fn new(logger: Option<Box<dyn Logger<S> + Send + Sync>>) -> Storage<S> {
        let root_container = Container::new_directory();
        Storage {
            sequencer: S::new(),
            _logger: logger,
            root_container,
        }
    }

    /// Starts a storage transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Snapshot, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// ```
    pub fn transaction(&self) -> Transaction<S> {
        Transaction::new(self, &self.sequencer)
    }

    /// Takes a snapshot of the storage.
    ///
    /// If a Transaction is given, the snapshot includes changes that have been made by the
    /// transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let snapshot = storage.snapshot();
    /// ```
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::new(&self.sequencer, None, None)
    }

    /// Creates a new container directory.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    ///
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &snapshot, &mut journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    /// ```
    pub fn create_directory(
        &self,
        path: &str,
        snapshot: &Snapshot<S>,
        journal: &mut Journal<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        let split = path.split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        for name in split {
            if let Some(container_ref) = current_container_ref.as_ref() {
                if let Some(directory_handle) =
                    container_ref.create_directory(name, snapshot, journal)
                {
                    current_container_ref = directory_handle.get(&guard);
                } else {
                    return Err(Error::Fail);
                }
            } else {
                return Err(Error::Fail);
            }
        }
        if let Some(container_ref) = current_container_ref.take() {
            if let Some(container_handle) = container_ref.create_handle() {
                return Ok(container_handle);
            }
        }
        Err(Error::Fail)
    }

    /// Gets the Container located at the given path.
    ///
    /// When a Transaction is given, and the Container at the given path is created by the
    /// Transaction, the Container is returned.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    ///
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &snapshot, &mut journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    /// drop(snapshot);
    ///
    /// transaction.commit();
    ///
    /// let snapshot = storage.snapshot();
    /// let result = storage.get("/thomas/eats/apples", &snapshot);
    /// assert!(result.is_some());
    /// ```
    pub fn get(&self, path: &str, snapshot: &Snapshot<S>) -> Option<ContainerHandle<S>> {
        let split = path.split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        for name in split {
            if let Some(container_ref) = current_container_ref.as_ref() {
                current_container_ref = container_ref.search(name, &snapshot, &guard);
            } else {
                return None;
            }
        }
        if let Some(container_ref) = current_container_ref.take() {
            if let Some(container_handle) = container_ref.create_handle() {
                return Some(container_handle);
            }
        }
        None
    }

    /// Reads the Container at the given path.
    ///
    /// Getting a reference to a Container requires zero write operations on the storage.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    ///
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// storage.create_directory("/thomas/eats/apples", &snapshot, &mut journal);
    /// journal.submit();
    ///
    /// let snapshot = transaction.snapshot();
    /// let result = storage.read("/thomas/eats/apples", |_| true, &snapshot);
    /// assert!(result.unwrap());
    /// ```
    pub fn read<R, F: FnOnce(&Container<S>) -> R>(
        &self,
        path: &str,
        reader: F,
        snapshot: &Snapshot<S>,
    ) -> Option<R> {
        let split = path.split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        for name in split {
            if let Some(container_ref) = current_container_ref.as_ref() {
                current_container_ref = container_ref.search(name, &snapshot, &guard);
            } else {
                return None;
            }
        }
        if let Some(container_ref) = current_container_ref.take() {
            return Some(reader(container_ref));
        }
        None
    }

    /// Links a data container to the given directory.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Container, RelationalTable, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    ///
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &snapshot, &mut journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    /// drop(snapshot);
    ///
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let new_container_data = Box::new(RelationalTable::new());
    /// let new_data_container = Container::<AtomicCounter>::new_container(new_container_data);
    /// storage.link("/thomas/eats/apples", new_data_container, "apple1", &snapshot, &mut journal);
    /// journal.submit();
    /// drop(snapshot);
    ///
    /// let snapshot = transaction.snapshot();
    /// let result = storage.get("/thomas/eats/apples/apple1", &snapshot);
    /// assert!(result.is_some());
    /// drop(snapshot);
    ///
    /// transaction.commit();
    /// ```
    pub fn link(
        &self,
        path: &str,
        container: ContainerHandle<S>,
        name: &str,
        snapshot: &Snapshot<S>,
        journal: &mut Journal<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        if let Some(container_handle) = self.get(path, snapshot) {
            let guard = crossbeam_epoch::pin();
            let container_directory_ref = container_handle.get(&guard);
            if let Some(container_ref) = container_directory_ref {
                if container_ref.link(name, container.clone(), snapshot, journal) {
                    return Ok(container);
                }
            }
        }
        Err(Error::Fail)
    }

    /// Relocates a data container.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Container, RelationalTable, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    ///
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &snapshot, &mut journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    /// drop(snapshot);
    ///
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let new_container_data = Box::new(RelationalTable::new());
    /// let new_data_container = Container::<AtomicCounter>::new_container(new_container_data);
    /// storage.link("/thomas/eats/apples", new_data_container, "apple1", &snapshot, &mut journal);
    /// journal.submit();
    /// drop(snapshot);
    ///
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// storage.relocate("/thomas/eats/apples/apple1", "/thomas/eats", &mut journal, &snapshot);
    /// journal.submit();
    /// drop(snapshot);
    ///
    /// let snapshot = transaction.snapshot();
    /// let result = storage.get("/thomas/eats/apples/apple1", &snapshot);
    /// assert!(result.is_none());
    ///
    /// let result = storage.get("/thomas/eats/apple1", &snapshot);
    /// assert!(result.is_some());
    /// ```
    pub fn relocate(
        &self,
        path: &str,
        target_path: &str,
        journal: &mut Journal<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        if let Some(container_handle) = self.get(path, snapshot) {
            if let Some(target_directory_container_handle) = self.get(target_path, snapshot) {
                if let Some((name, _)) = Self::name(&path) {
                    let guard = crossbeam_epoch::pin();
                    if let Some(container_ref) = target_directory_container_handle.get(&guard) {
                        if container_ref.link(name, container_handle.clone(), snapshot, journal) {
                            let _result = self.remove(path, snapshot, journal);
                            return Ok(container_handle);
                        }
                    }
                }
            }
        }
        Err(Error::Fail)
    }

    /// Removes the Container at the given path.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    ///
    /// let mut transaction = storage.transaction();
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &snapshot, &mut journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    /// drop(snapshot);
    /// transaction.commit();
    ///
    /// let mut transaction = storage.transaction();
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let result = storage.remove("/thomas/eats/apples", &snapshot, &mut journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    /// drop(snapshot);
    /// transaction.commit();
    ///
    /// let snapshot = storage.snapshot();
    /// let result = storage.get("/thomas/eats/apples", &snapshot);
    /// assert!(result.is_none());
    /// ```
    pub fn remove(
        &self,
        path: &str,
        snapshot: &Snapshot<S>,
        journal: &mut Journal<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        let split = path.split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        let mut current_container_name: Option<&str> = None;
        let mut parent_container_ref: Option<&Container<S>> = None;
        for name in split {
            if let Some(container_ref) = current_container_ref.as_ref() {
                if let Some(child_container_ref) = container_ref.search(name, &snapshot, &guard) {
                    parent_container_ref.replace(container_ref);
                    current_container_name.replace(name);
                    current_container_ref.replace(child_container_ref);
                } else {
                    return Err(Error::Fail);
                }
            } else {
                return Err(Error::Fail);
            }
        }
        if let (
            Some(current_container_ref),
            Some(current_container_name),
            Some(parent_container_ref),
        ) = (
            current_container_ref.take(),
            current_container_name.take(),
            parent_container_ref.take(),
        ) {
            if let Some(container_handle) = current_container_ref.create_handle() {
                if parent_container_ref.unlink(current_container_name, snapshot, journal) {
                    return Ok(container_handle);
                }
            }
        }
        Err(Error::Fail)
    }

    /// Extracts the name and position of the container out of a string.
    fn name(path: &str) -> Option<(&str, usize)> {
        let split = path.split('/');
        let mut last_token = None;
        for (index, name) in split.enumerate() {
            last_token.replace((name, index));
        }
        last_token
    }
}
