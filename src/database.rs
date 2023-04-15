// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::overseer::{Overseer, Task};
use super::{
    AccessController, AtomicCounter, Container, Error, Journal, MemoryDevice, Metadata,
    PersistenceLayer, Sequencer, Snapshot, Transaction,
};
use scc::{ebr, HashIndex};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Instant;

/// [`Database`] represents a single stand-alone transactional database.
///
/// [`Database`] provides the interface for users to interact with each individual transactional
/// [`Container`] in it.
#[derive(Debug)]
pub struct Database<S: Sequencer = AtomicCounter, P: PersistenceLayer<S> = MemoryDevice<S>> {
    /// The kernel of the database.
    ///
    /// The kernel of the database has to be allocated on the heap in order to provide stable
    /// memory addresses of some data while allowing the [`Database`] to be moved freely.
    kernel: Arc<Kernel<S, P>>,

    /// A background thread waking up timed out tasks and deleting unreachable database objects.
    ///
    /// `overseer` has access to `kernel` by holding a strong reference to it.
    overseer: Overseer,
}

/// The core of [`Database`].
#[derive(Debug)]
pub(super) struct Kernel<S: Sequencer, P: PersistenceLayer<S>> {
    /// The logical clock generator of the [`Database`].
    sequencer: S,

    /// The container map.
    container_map: HashIndex<String, ebr::Arc<Container<S, P>>>,

    /// The database access controller.
    access_controller: AccessController<S>,

    /// The persistence layer of the database.
    persistence_layer: P,
}

impl<S: Sequencer, P: PersistenceLayer<S>> Database<S, P> {
    /// Creates a new [`Database`] instance from the specified [`PersistenceLayer`].
    ///
    /// # Errors
    ///
    /// Returns an error if the persistence layer failed to recover the database, memory allocation
    /// failed, or the deadline was reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{AtomicCounter, Database, MemoryDevice};
    ///
    /// async {
    ///     let database: Database<AtomicCounter> = Database::with_persistence_layer(
    ///         MemoryDevice::default(), None, None).await.unwrap();
    /// };
    /// ```
    #[inline]
    pub async fn with_persistence_layer(
        persistence_layer: P,
        recover_until: Option<S::Instant>,
        deadline: Option<Instant>,
    ) -> Result<Database<S, P>, Error> {
        let kernel = Arc::new(Kernel {
            sequencer: S::default(),
            container_map: HashIndex::default(),
            access_controller: AccessController::default(),
            persistence_layer,
        });
        let overseer = Overseer::spawn(kernel.clone());
        let mut database = Database {
            kernel: kernel.clone(),
            overseer,
        };
        let io_completion =
            kernel
                .persistence_layer
                .recover(&mut database, recover_until, deadline)?;
        let recovered_instant = io_completion.await?;
        let _: Result<S::Instant, S::Instant> = kernel.sequencer.update(recovered_instant, Relaxed);
        Ok(database)
    }

    /// Backs up the current snapshot of the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the persistence layer failed to back up the database, memory allocation
    /// failed, or the deadline was reached.
    #[inline]
    pub async fn backup(
        &self,
        catalog_only: bool,
        path: Option<&str>,
        deadline: Option<Instant>,
    ) -> Result<S::Instant, Error> {
        let io_completion =
            self.kernel
                .persistence_layer
                .backup(self, catalog_only, path, deadline)?;
        io_completion.await
    }

    /// Starts a [`Transaction`].
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::Database;
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// ```
    #[inline]
    #[must_use]
    pub fn transaction(&self) -> Transaction<S, P> {
        Transaction::new(self)
    }

    /// Captures the current state of the [`Database`] as a [`Snapshot`].
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::Database;
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let snapshot = database.snapshot();
    /// ```
    #[inline]
    #[must_use]
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::from_database(self)
    }

    /// Returns a reference to its [`AccessController`].
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::Database;
    ///
    /// let database = Database::default();
    /// let access_controller = database.access_controller();
    /// ```
    #[inline]
    #[must_use]
    pub fn access_controller(&self) -> &AccessController<S> {
        self.kernel.access_controller()
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
    /// use sap_tsf::{Database, Metadata};
    ///
    /// let database = Database::default();
    /// let name = "hello".to_string();
    /// let metadata = Metadata::default();
    /// let transaction = database.transaction();
    /// let mut journal = transaction.journal();
    /// async {
    ///     let result = database.create_container(name, metadata, &mut journal, None).await;
    ///     assert!(result.is_ok());
    /// };
    /// ```
    #[inline]
    pub async fn create_container<'d, 't, 'j>(
        &'d self,
        name: String,
        metadata: Metadata,
        _journal: &'j mut Journal<'d, 't, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<ebr::Arc<Container<S, P>>, Error> {
        let _: &AccessController<S> = &self.kernel.access_controller;
        let container = ebr::Arc::new(Container::new(metadata));
        match self
            .kernel
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
    /// use sap_tsf::{Database, Metadata};
    ///
    /// let database = Database::default();
    /// let name = "hello";
    /// let metadata = Metadata::default();
    /// let new_name = "hi".to_string();
    /// let transaction = database.transaction();
    /// let mut journal = transaction.journal();
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
    pub async fn rename_container<'d, 't, 'j>(
        &'d self,
        name: &str,
        new_name: String,
        _journal: &'j mut Journal<'d, 't, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<(), Error> {
        if let Some(container) = self.kernel.container_map.read(name, |_, c| c.clone()) {
            if self
                .kernel
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
    /// use sap_tsf::{Database, Metadata};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let name = "hello";
    /// let metadata = Metadata::default();
    /// async {
    ///     let mut journal = transaction.journal();
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
    pub async fn get_container<'d, 't, 'j, 'r>(
        &'d self,
        name: &str,
        _snapshot: &'r Snapshot<'d, 't, 'j, S>,
    ) -> Option<&'r Container<S, P>> {
        self.kernel.container_map.read(name, |_, c|
            // Safety: `snapshot` is the proof that the returned reference stays valid at least for
            // the lifetime of `snapshot`; even though the container is dropped, the data remains
            // until it is garbage collected.
            unsafe { std::mem::transmute::<&Container<S, P>, &'r Container<S, P>>(&**c) })
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
    /// use sap_tsf::{Database, Metadata};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let name = "hello";
    /// let metadata = Metadata::default();
    /// async {
    ///     let mut journal = transaction.journal();
    ///     let create_result =
    ///         database.create_container(name.to_string(), metadata, &mut journal, None).await;
    ///     assert!(create_result.is_ok());
    ///     journal.submit();
    ///     let snapshot = transaction.snapshot();
    ///     let mut journal = transaction.journal();
    ///     let drop_result =
    ///         database.drop_container(name, &snapshot, &mut journal, None).await;
    ///     assert!(drop_result.is_ok());
    /// };
    /// ```
    #[inline]
    pub async fn drop_container<'d, 't, 'j>(
        &'d self,
        name: &str,
        _snapshot: &Snapshot<'d, 't, 'j, S>,
        _journal: &'j mut Journal<'d, 't, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<(), Error> {
        if self.kernel.container_map.remove_async(name).await {
            Ok(())
        } else {
            Err(Error::NotFound)
        }
    }

    /// Returns a reference to its [`Sequencer`].
    pub(super) fn sequencer(&self) -> &S {
        &self.kernel.sequencer
    }

    /// Returns a reference to the [`PersistenceLayer`].
    pub(super) fn persistence_layer(&self) -> &P {
        &self.kernel.persistence_layer
    }

    /// Returns a reference to its [`Overseer`].
    pub(super) fn overseer(&self) -> &Overseer {
        &self.overseer
    }
}

impl Default for Database<AtomicCounter, MemoryDevice<AtomicCounter>> {
    /// Creates an empty default [`Database`] instance.
    ///
    /// The type of the [`Sequencer`] is [`AtomicCounter`], and the persistence layer is of
    /// [`MemoryDevice`].
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::Database;
    ///
    /// let database = Database::default();
    /// ```
    #[inline]
    fn default() -> Self {
        let kernel = Arc::new(Kernel {
            sequencer: AtomicCounter::default(),
            container_map: HashIndex::default(),
            access_controller: AccessController::default(),
            persistence_layer: MemoryDevice::default(),
        });
        let overseer = Overseer::spawn(kernel.clone());
        Database { kernel, overseer }
    }
}

impl<S: Sequencer, P: PersistenceLayer<S>> Drop for Database<S, P> {
    #[inline]
    fn drop(&mut self) {
        while !self.overseer.send_task(Task::Shutdown) {}
    }
}

impl<S: Sequencer, P: PersistenceLayer<S>> Kernel<S, P> {
    /// Returns a reference to its [`AccessController`].
    pub(super) fn access_controller(&self) -> &AccessController<S> {
        &self.access_controller
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
        let mut journal = transaction.journal();
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
