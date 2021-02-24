// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

extern crate crossbeam_epoch;

use super::{Container, ContainerHandle, Error, Journal, Sequencer, Snapshot, Transaction};
use std::string::String;

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
    /// The name of the storage.
    _name: String,
    /// The logical clock generator of the storage.
    sequencer: S,
    /// The root container of the storage.
    root_container: ContainerHandle<S>,
}

impl<S: Sequencer> Storage<S> {
    /// Creates a new Storage.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// ```
    pub fn new(name: String) -> Storage<S> {
        Storage {
            _name: name,
            sequencer: S::new(),
            root_container: Container::new_directory(),
        }
    }

    /// Starts a storage transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Snapshot, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
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
    /// use tss::{DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// let snapshot = storage.snapshot();
    /// ```
    pub fn snapshot<'s>(&'s self) -> Snapshot<S> {
        Snapshot::new(&self.sequencer, None, None)
    }

    /// Creates a new container directory.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    ///
    /// let journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    /// ```
    pub fn create_directory(
        &self,
        path: &str,
        _journal: &Journal<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        let split = path.split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        for name in split {
            if let Some(directory_handle) = current_container_ref.create_directory(name) {
                current_container_ref = directory_handle.get(&guard);
            } else {
                return Err(Error::Fail);
            }
        }
        if let Some(container_handle) = current_container_ref.create_container_handle() {
            Ok(container_handle)
        } else {
            Err(Error::Fail)
        }
    }

    /// Gets the Container located at the given path.
    ///
    /// When a Transaction is given, and the Container at the given path is created by the
    /// Transaction, the Container is returned.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    ///
    /// let journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    ///
    /// transaction.commit();
    ///
    /// let snapshot = storage.snapshot();
    /// let result = storage.get("/thomas/eats/apples", &snapshot);
    /// assert!(result.is_some());
    /// ```
    pub fn get(&self, path: &str, _snapshot: &Snapshot<S>) -> Option<ContainerHandle<S>> {
        let split = path.split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        for name in split {
            if let Some(container_ref) = current_container_ref.search(name, &guard) {
                current_container_ref = container_ref;
            } else {
                return None;
            }
        }
        if let Some(container_handle) = current_container_ref.create_container_handle() {
            Some(container_handle)
        } else {
            None
        }
    }

    /// Reads the Container at the given path.
    ///
    /// Getting a reference to a Container requires zero write operations on the storage.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    ///
    /// let journal = transaction.start();
    /// storage.create_directory("/thomas/eats/apples", &journal);
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
        _snapshot: &Snapshot<S>,
    ) -> Option<R> {
        let split = path.split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        for name in split {
            if let Some(container_ref) = current_container_ref.search(name, &guard) {
                current_container_ref = container_ref;
            } else {
                return None;
            }
        }
        Some(reader(current_container_ref))
    }

    /// Links a data container to the given directory.
    ///
    /// # Examples
    /// ```
    /// use tss::{Container, DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    ///
    /// let journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    ///
    /// let journal = transaction.start();
    /// let snapshot = journal.snapshot();
    /// let new_data_container = Container::<DefaultSequencer>::new_default_container();
    /// storage.link("/thomas/eats/apples", new_data_container, "apple1", &journal, &snapshot);
    /// drop(snapshot);
    /// journal.submit();
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
        _journal: &Journal<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        if let Some(container_handle) = self.get(path, snapshot) {
            let guard = crossbeam_epoch::pin();
            if container_handle.get(&guard).link(name, container.clone()) {
                return Ok(container);
            }
        }
        Err(Error::Fail)
    }

    /// Relocates a data container.
    ///
    /// # Examples
    /// ```
    /// use tss::{Container, DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    ///
    /// let journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    ///
    /// let journal = transaction.start();
    /// let snapshot = journal.snapshot();
    /// let new_data_container = Container::<DefaultSequencer>::new_default_container();
    /// storage.link("/thomas/eats/apples", new_data_container, "apple1", &journal, &snapshot);
    /// drop(snapshot);
    /// journal.submit();
    ///
    /// let journal = transaction.start();
    /// let snapshot = journal.snapshot();
    /// storage.relocate("/thomas/eats/apples/apple1", "/thomas/eats", &journal, &snapshot);
    /// drop(snapshot);
    /// journal.submit();
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
        journal: &Journal<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        if let Some(container_handle) = self.get(path, snapshot) {
            if let Some(target_directory_container_handle) = self.get(target_path, snapshot) {
                if let Some((name, _)) = Self::name(&path) {
                    let guard = crossbeam_epoch::pin();
                    if target_directory_container_handle
                        .get(&guard)
                        .link(name, container_handle.clone())
                    {
                        let _result = self.remove(path, journal, snapshot);
                        return Ok(container_handle);
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
    /// use tss::{DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// let journal = transaction.start();
    /// let result = storage.create_directory("/thomas/eats/apples", &journal);
    /// assert!(result.is_ok());
    /// journal.submit();
    /// transaction.commit();
    ///
    /// let mut transaction = storage.transaction();
    /// let snapshot = transaction.snapshot();
    /// let journal = transaction.start();
    /// let result = storage.remove("/thomas/eats/apples", &journal, &snapshot);
    /// assert!(result.is_ok());
    /// drop(snapshot);
    /// journal.submit();
    /// transaction.commit();
    ///
    /// let snapshot = storage.snapshot();
    /// let result = storage.get("/thomas/eats/apples", &snapshot);
    /// assert!(result.is_none());
    /// ```
    pub fn remove(
        &self,
        path: &str,
        _journal: &Journal<S>,
        _snapshot: &Snapshot<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        let split = path.split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        let mut parent_container_ref: Option<&Container<S>> = None;
        let mut parent_container_name: Option<&str> = None;
        for name in split {
            if let Some(container_ref) = current_container_ref.search(name, &guard) {
                parent_container_ref.replace(current_container_ref);
                parent_container_name.replace(name);
                current_container_ref = container_ref;
            } else {
                return Err(Error::Fail);
            }
        }
        if let Some(container_handle) = current_container_ref.create_container_handle() {
            if let (Some(parent_container_ref), Some(parent_container_name)) =
                (parent_container_ref, parent_container_name)
            {
                if !parent_container_ref.unlink(parent_container_name) {
                    return Err(Error::Fail);
                }
            }
            Ok(container_handle)
        } else {
            Err(Error::Fail)
        }
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
