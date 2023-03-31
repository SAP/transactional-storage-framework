// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::unused_async, unused)]

use super::{
    AtomicCounter, Container, Error, Journal, Metadata, PersistenceLayer, Sequencer, Snapshot,
    Transaction,
};

use std::time::Instant;

use scc::{ebr, HashIndex};

/// [`Database`] represents a single stand-alone transactional database.
///
/// [`Database`] provides the interface for users to interact with each individual transactional
/// [`Container`] in it.
pub struct Database<S = AtomicCounter>
where
    S: Sequencer,
{
    /// The logical clock generator of the [`Database`].
    sequencer: S,

    /// The persistence layer of the database.
    persistence_layer: Option<Box<dyn PersistenceLayer<S>>>,

    /// The container map.
    container_map: HashIndex<String, ebr::Arc<Container<S>>>,
}

impl<S: Sequencer> Database<S> {
    /// Creates an empty [`Database`] instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Database, FileLogger};
    ///
    /// let database: Database<AtomicCounter> = Database::new();
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Database<S> {
        Database {
            sequencer: S::default(),
            persistence_layer: None,
            container_map: HashIndex::default(),
        }
    }

    /// Creates a new [`Database`] instance from the specified [`PersistenceLayer`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Database, FileLogger};
    ///
    /// let persistence_layer = Box::new(FileLogger::new("/home/dba/db"));
    /// let database: Database<AtomicCounter> =
    ///     Database::with_persistence_layer(persistence_layer);
    /// ```
    #[inline]
    #[must_use]
    pub fn with_persistence_layer(persistence_layer: Box<dyn PersistenceLayer<S>>) -> Database<S> {
        Database {
            sequencer: S::default(),
            persistence_layer: Some(persistence_layer),
            container_map: HashIndex::default(),
        }
    }

    /// Sets a [`PersistenceLayer`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{FileLogger, Database};
    ///
    /// let mut database = Database::default();
    /// let persistence_layer = Box::new(FileLogger::new("/home/dba/db"));
    /// assert!(database.set_persistence_layer(Some(persistence_layer)).is_none());
    /// ```
    #[inline]
    #[must_use]
    pub fn set_persistence_layer(
        &mut self,
        persistence_layer: Option<Box<dyn PersistenceLayer<S>>>,
    ) -> Option<Box<dyn PersistenceLayer<S>>> {
        let old_persistence_layer = self.persistence_layer.take();
        self.persistence_layer = persistence_layer;
        old_persistence_layer
    }

    /// Starts a [`Transaction`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::Database;
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// ```
    #[inline]
    pub fn transaction(&self) -> Transaction<S> {
        Transaction::new(self, &self.sequencer)
    }

    /// Captures the current state of the [`Database`] as a [`Snapshot`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::Database;
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let snapshot = database.snapshot();
    /// ```
    #[inline]
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::new(&self.sequencer, None, None)
    }

    /// Creates a new empty [`Container`].
    ///
    /// # Errors
    ///
    /// If a [`Container`] exists under the specified name, returns an [`Error`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Metadata};
    ///
    /// let database = Database::default();
    /// let name = "hello".to_string();
    /// let metadata = Metadata {};
    /// let transaction = database.transaction();
    /// let mut journal = transaction.start();
    /// async {
    ///     let result = database.create_container(name, metadata, &mut journal, None).await;
    ///     assert!(result.is_ok());
    /// };
    /// ```
    #[inline]
    pub async fn create_container<'s, 't, 'j>(
        &'s self,
        name: String,
        metadata: Metadata,
        journal: &'j mut Journal<'s, 't, S>,
        deadline: Option<Instant>,
    ) -> Result<&'j mut Container<S>, Error> {
        unimplemented!()
    }

    /// Renames an existing [`Container`].
    ///
    /// # Errors
    ///
    /// If no [`Container`] exists under the specified name, returns an [`Error`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Metadata};
    ///
    /// let database = Database::default();
    /// let name = "hello";
    /// let metadata = Metadata {};
    /// let new_name = "hi".to_string();
    /// let transaction = database.transaction();
    /// let mut journal = transaction.start();
    /// async {
    ///     let create_result =
    ///         database.create_container(name.to_string(), metadata, &mut journal, None).await;
    ///     assert!(create_result.is_ok());
    ///     let rename_result =
    ///         database.rename_container(name, new_name, &mut journal, None).await;
    ///     assert!(rename_result.is_ok());
    /// };
    /// ```
    #[inline]
    pub async fn rename_container<'s, 't, 'j>(
        &'s self,
        name: &str,
        new_name: String,
        journal: &'j mut Journal<'s, 't, S>,
        deadline: Option<Instant>,
    ) -> Result<&'j mut Container<S>, Error> {
        unimplemented!()
    }

    /// Gets a reference to the [`Container`] under the specified name.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Metadata};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let name = "hello";
    /// let metadata = Metadata {};
    /// async {
    ///     let mut journal = transaction.start();
    ///     let create_result =
    ///         database.create_container(name.to_string(), metadata, &mut journal, None).await;
    ///     assert!(create_result.is_ok());
    ///     journal.submit();
    ///     let snapshot = transaction.snapshot();
    ///     let get_result = database.get_container(name, &snapshot).await;
    ///     assert!(get_result.is_some());
    /// };
    /// ```
    #[inline]
    pub async fn get_container<'s, 't, 'j, 'r>(
        &'s self,
        name: &str,
        snapshot: &'r Snapshot<'s, 't, 'j, S>,
    ) -> Option<&'r Container<S>> {
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
    /// use tss::{Database, Metadata};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let name = "hello";
    /// let metadata = Metadata {};
    /// async {
    ///     let mut journal = transaction.start();
    ///     let create_result =
    ///         database.create_container(name.to_string(), metadata, &mut journal, None).await;
    ///     assert!(create_result.is_ok());
    ///     journal.submit();
    ///     let snapshot = transaction.snapshot();
    ///     let mut journal = transaction.start();
    ///     let drop_result =
    ///         database.drop_container(name, &snapshot, &mut journal, None).await;
    ///     assert!(drop_result.is_ok());
    /// };
    /// ```
    #[inline]
    pub async fn drop_container<'s, 't, 'j>(
        &'s self,
        name: &str,
        snapshot: &Snapshot<'s, 't, 'j, S>,
        journal: &'j mut Journal<'s, 't, S>,
        deadline: Option<Instant>,
    ) -> Result<&'j Container<S>, Error> {
        unimplemented!();
    }
}

impl Default for Database<AtomicCounter> {
    /// Creates an empty default [`Database`] instance.
    ///
    /// The type of the [`Sequencer`] is [`AtomicCounter`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::Database;
    ///
    /// let database = Database::default();
    /// ```
    #[inline]
    fn default() -> Self {
        Database {
            sequencer: AtomicCounter::default(),
            persistence_layer: None,
            container_map: HashIndex::default(),
        }
    }
}
