// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::overseer::{Overseer, Task};
use super::{
    AtomicCounter, Container, Error, Journal, LockTable, Metadata, PersistenceLayer, Sequencer,
    Snapshot, Transaction,
};
use scc::{ebr, HashIndex};
use std::time::Instant;

/// [`Database`] represents a single stand-alone transactional database.
///
/// [`Database`] provides the interface for users to interact with each individual transactional
/// [`Container`] in it.
#[derive(Debug)]
pub struct Database<S: Sequencer = AtomicCounter> {
    /// The logical clock generator of the [`Database`].
    sequencer: S,

    /// The persistence layer of the database.
    persistence_layer: Option<Box<dyn PersistenceLayer<S>>>,

    /// The container map.
    container_map: HashIndex<String, ebr::Arc<Container<S>>>,

    /// The lock table.
    lock_table: LockTable,

    /// A background thread waking up timed out tasks and deleting unreachable database objects.
    overseer: Overseer,
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
            lock_table: LockTable::default(),
            overseer: Overseer::spawn(),
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
            lock_table: LockTable::default(),
            overseer: Overseer::spawn(),
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
        Transaction::new(self)
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
        Snapshot::from_parts(&self.sequencer, None, None)
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
    /// let metadata = Metadata::default();
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
        _journal: &'j mut Journal<'s, 't, S>,
        _deadline: Option<Instant>,
    ) -> Result<ebr::Arc<Container<S>>, Error> {
        let _: &LockTable = &self.lock_table;
        let container = ebr::Arc::new(Container::new(metadata));
        match self
            .container_map
            .insert_async(name, container.clone())
            .await
        {
            Ok(_) => Ok(container),
            Err(_) => Err(Error::UniquenessViolation),
        }
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
    /// let metadata = Metadata::default();
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
        _journal: &'j mut Journal<'s, 't, S>,
        _deadline: Option<Instant>,
    ) -> Result<(), Error> {
        if let Some(container) = self.container_map.read(name, |_, c| c.clone()) {
            if self
                .container_map
                .insert_async(new_name, container)
                .await
                .is_ok()
            {
                Ok(())
            } else {
                Err(Error::UniquenessViolation)
            }
        } else {
            Err(Error::NotFound)
        }
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
    /// let metadata = Metadata::default();
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
    #[allow(clippy::unused_async)]
    #[inline]
    pub async fn get_container<'s, 't, 'j, 'r>(
        &'s self,
        name: &str,
        _snapshot: &'r Snapshot<'s, 't, 'j, S>,
    ) -> Option<&'r Container<S>> {
        self.container_map.read(name, |_, c| unsafe {
            // The `Container` survives as long as the `Snapshot` is valid.
            std::mem::transmute::<&Container<S>, &'r Container<S>>(&**c)
        })
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
    /// let metadata = Metadata::default();
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
        _snapshot: &Snapshot<'s, 't, 'j, S>,
        _journal: &'j mut Journal<'s, 't, S>,
        _deadline: Option<Instant>,
    ) -> Result<(), Error> {
        if self.container_map.remove_async(name).await {
            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }

    /// Returns a reference to its [`Sequencer`].
    pub(crate) fn sequencer(&self) -> &S {
        &self.sequencer
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
            lock_table: LockTable::default(),
            overseer: Overseer::spawn(),
        }
    }
}

impl<S: Sequencer> Drop for Database<S> {
    #[inline]
    fn drop(&mut self) {
        while !self.overseer.try_post(Task::Shutdown) {
            // Reaching here means that there is a program logic bug.
            debug_assert!(false, "programming logic error");
        }
    }
}

#[cfg(test)]
mod test {
    use crate::sequencer::AtomicCounter;
    use crate::{Database, Metadata};

    use std::sync::Arc;

    #[tokio::test]
    async fn database() {
        let database: Arc<Database<AtomicCounter>> = Arc::new(Database::default());
        let transaction = database.transaction();
        let snapshot = transaction.snapshot();
        let mut journal = transaction.start();
        let metadata = Metadata::default();
        assert!(database
            .create_container("hello".to_string(), metadata, &mut journal, None)
            .await
            .is_ok());
        let metadata = Metadata::default();
        assert!(database
            .create_container("hello".to_string(), metadata, &mut journal, None)
            .await
            .is_err());
        drop(journal);
        drop(snapshot);
        drop(transaction);
    }
}
