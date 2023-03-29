// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::unused_async, unused)]

use super::{Container, Error, Journal, Logger, Sequencer, Snapshot, Transaction};

use std::time::Instant;

use scc::{ebr, HashIndex};

/// [`Storage`] manages a single stand-alone transactional database.
///
/// [`Storage`] provides the interfaces for users to interact with individual transactional
/// [`Container`]s in the database.
pub struct Storage<S: Sequencer> {
    /// The logical clock generator of the [`Storage`].
    sequencer: S,

    /// The logger of the storage.
    _logger: Option<Box<dyn Logger<S> + Send + Sync>>,

    /// The container map.
    container_map: HashIndex<String, ebr::Arc<Container<S>>>,
}

impl<S: Sequencer> Storage<S> {
    /// Creates a new [`Storage`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, FileLogger, Storage};
    ///
    /// let logger = Box::new(FileLogger::new("/home/dba/db"));
    /// let storage: Storage<AtomicCounter> = Storage::new(Some(logger));
    /// ```
    #[inline]
    #[must_use]
    pub fn new(logger: Option<Box<dyn Logger<S> + Send + Sync>>) -> Storage<S> {
        Storage {
            sequencer: S::default(),
            _logger: logger,
            container_map: HashIndex::default(),
        }
    }

    /// Starts a storage [`Transaction`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Snapshot, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// ```
    #[inline]
    pub fn transaction(&self) -> Transaction<S> {
        Transaction::new(self, &self.sequencer)
    }

    /// Takes a [`Snapshot`] of the [`Storage`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let snapshot = storage.snapshot();
    /// ```
    #[inline]
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::new(&self.sequencer, None, None)
    }

    /// Creates a new [`Container`] directory.
    ///
    /// # Errors
    ///
    /// If a [`Container`] exists under the specified name, returns an [`Error`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let result_future =
    ///     storage.create_container(
    ///         "hello".to_string(),
    ///         &snapshot,
    ///         &mut journal,
    ///         None);
    /// ```
    #[inline]
    pub async fn create_container<'s, 't, 'j>(
        &'s self,
        name: String,
        snapshot: &Snapshot<'s, 't, 'j, S>,
        journal: &'j mut Journal<'s, 't, S>,
        deadline: Option<Instant>,
    ) -> Result<ebr::Arc<Container<S>>, Error> {
        unimplemented!()
    }

    /// Gets the [`Container`] under the specified name.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let snapshot = storage.snapshot();
    /// let result_future = storage.get_container("hello", &snapshot);
    /// ```
    #[inline]
    pub async fn get_container<'s, 't, 'j>(
        &'s self,
        path: &str,
        snapshot: &Snapshot<'s, 't, 'j, S>,
    ) -> Option<ebr::Arc<Container<S>>> {
        None
    }

    /// Reads the [`Container`] under the specified name.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let snapshot = storage.snapshot();
    /// let result_future = storage.read_container("hello", |_| true, &snapshot);
    /// ```
    #[inline]
    pub async fn read_container<'s, 't, 'j, R, F: FnOnce(&Container<S>) -> R>(
        &'s self,
        name: &str,
        reader: F,
        snapshot: &Snapshot<'s, 't, 'j, S>,
    ) -> Option<R> {
        None
    }

    /// Drops a [`Container`] under the specified name.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the container does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Container, RelationalTable, Storage};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let snapshot = transaction.snapshot();
    /// let mut journal = transaction.start();
    /// let result_future = storage.drop_container("hello", &snapshot, &mut journal, None);
    /// ```
    #[inline]
    pub async fn drop_container<'s, 't, 'j>(
        &'s self,
        name: &str,
        snapshot: &Snapshot<'s, 't, 'j, S>,
        journal: &'j mut Journal<'s, 't, S>,
        deadline: Option<Instant>,
    ) -> Result<ebr::Arc<Container<S>>, Error> {
        unimplemented!();
    }
}
