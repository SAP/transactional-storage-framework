extern crate crossbeam_epoch;

use super::{
    Container, ContainerHandle, DefaultSequencer, Error, Sequencer, Snapshot, Transaction,
};
use crossbeam_epoch::{Atomic, Owned, Shared};
use std::string::String;

/// A transactional storage.
///
/// tss::Storage is a collection of internal containers that are hierarchically organized.
/// The organization of containers resembles that of POSIX file system as it allows symbolic links
/// and offers basic access control mechanisms.
///
/// Apart from containers being organized like a file system, every piece of data that tss::Storage
/// manages is multi-versioned, and transactionally updated. Therefore, tss::Storage can be viewed
/// as the storage layer of a huge database management system, while its flexibility allows the
/// developers and users to develop, or plug-in new features and transaction mechanisms easily.
pub struct Storage<S: Sequencer> {
    /// The name of the storage.
    name: String,
    /// The logical clock of the storage.
    sequencer: S,
    /// The root container of the storage.
    root_container: ContainerHandle<S>,
}

impl<S: Sequencer> Storage<S> {
    /// Creates a new storage instance.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("farm"));
    /// ```
    pub fn new(name: String) -> Storage<S> {
        Storage {
            name,
            sequencer: S::new(),
            root_container: Container::new_directory(),
        }
    }

    /// Starts a storage transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Snapshot, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("farm"));
    /// let transaction = storage.transaction();
    /// ```
    pub fn transaction(&self) -> Transaction<S> {
        Transaction::new(self, &self.sequencer)
    }

    /// Takes a snapshot of the storage.
    ///
    /// If a transaction is given, the snapshot includes changes that have been made by the
    /// transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Snapshot, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("farm"));
    /// let transaction = storage.transaction();
    /// let snapshot = storage.snapshot(Some(&transaction));
    /// ```
    pub fn snapshot<'s>(&'s self, transaction: Option<&'s Transaction<S>>) -> Snapshot<S> {
        Snapshot::new(self, &self.sequencer, transaction)
    }

    /// Creates a new container directory.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("farm"));
    /// let transaction = storage.transaction();
    ///
    /// let result = storage.create_directory(&String::from("/thomas/eats/apples"), &transaction);
    /// assert!(result.is_ok());
    /// ```
    pub fn create_directory(
        &self,
        path: &String,
        transaction: &Transaction<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        let split = path.as_str().split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        for name in split {
            if let Some(directory_handle) =
                current_container_ref.create_directory(&String::from(name))
            {
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

    /// Gets a container located at the given path.
    ///
    /// When a transaction is given, and the container at the given path is created by the
    /// transaction, the container is returned.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    ///
    /// let result = storage.create_directory(&String::from("/thomas/eats/apples"), &transaction);
    /// assert!(result.is_ok());
    /// transaction.commit();
    ///
    /// let snapshot = storage.snapshot(None);
    /// let result = storage.get(&String::from("/thomas/eats/apples"), &snapshot);
    /// assert!(result.is_some());
    /// ```
    pub fn get(&self, path: &String, snapshot: &Snapshot<S>) -> Option<ContainerHandle<S>> {
        let split = path.as_str().split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        for name in split {
            if let Some(container_ref) = current_container_ref.search(&String::from(name), &guard) {
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

    /// Reads a container.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// let snapshot = storage.snapshot(Some(&transaction));
    ///
    /// storage.create_directory(&String::from("/thomas/eats/apples"), &transaction);
    /// let result = storage.read(&String::from("/thomas/eats/apples"), |_| true, &snapshot);
    /// assert!(result.unwrap());
    /// ```
    pub fn read<R, F: FnOnce(&Container<S>) -> R>(
        &self,
        path: &String,
        reader: F,
        snapshot: &Snapshot<S>,
    ) -> Option<R> {
        let split = path.as_str().split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        for name in split {
            if let Some(container_ref) = current_container_ref.search(&String::from(name), &guard) {
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
    /// use tss::{Container, DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// let snapshot = storage.snapshot(Some(&transaction));
    ///
    /// let result = storage.create_directory(&String::from("/thomas/eats/apples"), &transaction);
    /// assert!(result.is_ok());
    ///
    /// let new_data_container = Container::<DefaultSequencer>::new_default_container();
    /// storage.link(&String::from("/thomas/eats/apples"), new_data_container,  &String::from("apple1"), &transaction, &snapshot);
    ///
    /// let result = storage.get(&String::from("/thomas/eats/apples/apple1"), &snapshot);
    /// assert!(result.is_some());
    ///
    /// drop(snapshot);
    /// transaction.commit();
    /// ```
    pub fn link(
        &self,
        path: &String,
        container: ContainerHandle<S>,
        name: &String,
        transaction: &Transaction<S>,
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
    /// use tss::{Container, DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// let snapshot = storage.snapshot(Some(&transaction));
    ///
    /// let result = storage.create_directory(&String::from("/thomas/eats/apples"), &transaction);
    /// assert!(result.is_ok());
    ///
    /// let new_data_container = Container::<DefaultSequencer>::new_default_container();
    /// storage.link(&String::from("/thomas/eats/apples"), new_data_container,  &String::from("apple1"), &transaction, &snapshot);
    ///
    /// storage.relocate(&String::from("/thomas/eats/apples/apple1"), &String::from("/thomas/eats"), &transaction, &snapshot);
    ///
    /// let result = storage.get(&String::from("/thomas/eats/apples/apple1"), &snapshot);
    /// assert!(result.is_none());
    ///
    /// let result = storage.get(&String::from("/thomas/eats/apple1"), &snapshot);
    /// assert!(result.is_some());
    /// ```
    pub fn relocate(
        &self,
        path: &String,
        target_path: &String,
        transaction: &Transaction<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        if let Some(container_handle) = self.get(&path, snapshot) {
            if let Some(target_directory_container_handle) = self.get(&target_path, snapshot) {
                if let Some((name, _)) = Self::name(&path) {
                    let guard = crossbeam_epoch::pin();
                    if target_directory_container_handle
                        .get(&guard)
                        .link(&String::from(name), container_handle.clone())
                    {
                        self.remove(path, transaction, snapshot);
                        return Ok(container_handle);
                    }
                }
            }
        }
        Err(Error::Fail)
    }

    /// Removes a container.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// let snapshot = storage.snapshot(Some(&transaction));
    ///
    /// let result = storage.create_directory(&String::from("/thomas/eats/apples"), &transaction);
    /// assert!(result.is_ok());
    /// drop(snapshot);
    /// transaction.commit();
    ///
    /// let mut transaction = storage.transaction();
    /// let snapshot = storage.snapshot(Some(&transaction));
    /// let result = storage.remove(&String::from("/thomas/eats/apples"), &transaction, &snapshot);
    /// assert!(result.is_ok());
    /// ```
    pub fn remove(
        &self,
        path: &String,
        transaction: &Transaction<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<ContainerHandle<S>, Error> {
        let split = path.as_str().split('/');
        let guard = crossbeam_epoch::pin();
        let mut current_container_ref = self.root_container.get(&guard);
        let mut parent_container_ref: Option<&Container<S>> = None;
        let mut parent_container_name: Option<&str> = None;
        for name in split {
            if let Some(container_ref) = current_container_ref.search(&String::from(name), &guard) {
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
                if !parent_container_ref.unlink(&String::from(parent_container_name)) {
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
        let mut position = 0;
        for name in split {
            last_token.replace((name, position));
            position += 1;
        }
        last_token
    }
}
