// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::sequencer::ToInstant;
use super::task_processor::TaskProcessor;
use super::{Database, JournalID, PersistenceLayer, Sequencer, TransactionID};
use std::cmp;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::ptr;
use std::sync::atomic::Ordering::Acquire;

/// [`Snapshot`] provides a consistent view on the [`Database`](super::Database).
///
/// There are three types of [`Snapshot`] which can be created using different methods.
///  1. [`Database::snapshot`](super::Database::snapshot) creates a [`Snapshot`] which does not
///     contain uncommitted data.
///  2. [`Transaction::snapshot`](super::Transaction::snapshot) creates a [`Snapshot`] which only
///     contains uncommitted changes to the database in submitted journals in the transaction.
///  3. [`Journal::snapshot`](super::Journal::snapshot) creates a [`Snapshot`] which only contains
///     changes to the database that are pending in the [`Journal`](super::Journal).
///
/// Two or more types of [`Snapshot`] can be combined into a single [`Snapshot`] via
/// [`Snapshot::combine`] as long as they belong to the same database.
#[derive(Clone, Debug)]
pub struct Snapshot<'d, 't, 'j, S: Sequencer> {
    /// The logical instant of the database system being tracked by [`Database`].
    ///
    /// This being `Some` allows the owner of the [`Snapshot`] to observe any committed changes to
    /// the database that are not newer than the instant.
    tracker: Option<S::Tracker>,

    /// The logical instant of the transaction.
    ///
    /// This being `Some` allows the owner of the [`Snapshot`] to observe any changes to the
    /// database made by the transaction that are not newer than the transaction local instant.
    transaction_snapshot: Option<TransactionSnapshot<'t>>,

    /// The associated [`Journal`](super::Journal).
    ///
    /// This being `Some` allows the owner of the [`Snapshot`] to observe any changes to the
    /// database made by the [`Journal`](super::Journal).
    journal_snapshot: Option<JournalSnapshot<'j>>,

    /// Enables the [`Snapshot`] to silently sleep until a transaction to be committed or rolled
    /// back.
    task_processor: &'d TaskProcessor,
}

/// Data representing the current state of the [`Transaction`](super::Transaction).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct TransactionSnapshot<'t> {
    /// The transaction identifier.
    id: TransactionID,

    /// The logical instant of the transaction.
    instant: Option<NonZeroU32>,

    /// Limits the lifetime to that of the transaction.
    _phantom: PhantomData<&'t ()>,
}

/// Data representing the current state of the [`Journal`](super::Transaction).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct JournalSnapshot<'j> {
    /// The journal identifier.
    id: JournalID,

    /// Limits the lifetime to that of the journal.
    _phantom: PhantomData<&'j ()>,
}

impl<'d, 't, 'j, S: Sequencer> Snapshot<'d, 't, 'j, S> {
    /// Combines two [`Snapshot`] instances into a single [`Snapshot`].
    ///
    /// If the supplied [`Snapshot`] conflicts with `self`, `self` is prioritized. For instance,
    /// if they are created by different [`Database`] instances, `self` is returned, or if they
    /// both have transaction snapshots, that of `self` is taken.
    ///
    /// This method allows a journal snapshot taken from a different transaction to be combined as
    /// long as the transaction is from the same [`Database`].
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::Database;
    /// use std::path::Path;
    ///
    /// async {
    ///     let database = Database::with_path(Path::new("combine")).await.unwrap();
    ///     let transaction = database.transaction();
    ///     let database_snapshot = database.snapshot();
    ///     let transaction_snapshot = transaction.snapshot();
    ///     let journal = transaction.journal();
    ///     let journal_snapshot = journal.snapshot();
    ///     let combined_snapshot =
    ///         journal_snapshot.combine(transaction_snapshot).combine(database_snapshot);
    /// };
    /// ````
    #[inline]
    #[must_use]
    pub fn combine(mut self, mut other: Snapshot<'d, 't, 'j, S>) -> Snapshot<'d, 't, 'j, S> {
        if !ptr::eq(self.task_processor, other.task_processor) {
            // They are from different `Database` instances of the same lifetime.
            return self;
        }
        let tracker = self.tracker.take().or_else(|| other.tracker.take());
        let transaction_snapshot = self
            .transaction_snapshot
            .take()
            .or_else(|| other.transaction_snapshot.take());
        let journal_snapshot = self
            .journal_snapshot
            .take()
            .or_else(|| other.journal_snapshot.take());
        Self {
            tracker,
            transaction_snapshot,
            journal_snapshot,
            task_processor: self.task_processor,
        }
    }

    /// Creates a new [`Snapshot`] from a [`Database`]
    pub(super) fn from_database<P: PersistenceLayer<S>>(
        database: &'d Database<S, P>,
    ) -> Snapshot<'d, 't, 'j, S> {
        let tracker = database.sequencer().track(Acquire);
        Snapshot {
            tracker: Some(tracker),
            transaction_snapshot: None,
            journal_snapshot: None,
            task_processor: database.task_processor(),
        }
    }

    /// Creates a new [`Snapshot`] from a [`TransactionSnapshot`]
    pub(super) fn from_journal<P: PersistenceLayer<S>>(
        database: &'d Database<S, P>,
        journal_snapshot: JournalSnapshot<'j>,
    ) -> Snapshot<'d, 't, 'j, S> {
        Snapshot {
            tracker: None,
            transaction_snapshot: None,
            journal_snapshot: Some(journal_snapshot),
            task_processor: database.task_processor(),
        }
    }

    /// Creates a new [`Snapshot`] from a [`TransactionSnapshot`]
    pub(super) fn from_transaction<P: PersistenceLayer<S>>(
        database: &'d Database<S, P>,
        transaction_snapshot: TransactionSnapshot<'t>,
    ) -> Snapshot<'d, 't, 'j, S> {
        Snapshot {
            tracker: None,
            transaction_snapshot: Some(transaction_snapshot),
            journal_snapshot: None,
            task_processor: database.task_processor(),
        }
    }

    /// Returns the time point value of the database snapshot.
    ///
    /// Returns `S::Instant::default()` if no database snapshot is set.
    pub(super) fn database_snapshot(&self) -> S::Instant {
        self.tracker
            .as_ref()
            .map_or_else(S::Instant::default, ToInstant::to_instant)
    }

    /// Returns a reference to its [`TransactionSnapshot`].
    pub(super) fn transaction_snapshot(&self) -> Option<&TransactionSnapshot<'t>> {
        self.transaction_snapshot.as_ref()
    }

    /// Returns a reference to its [`JournalSnapshot`].
    pub(super) fn journal_snapshot(&self) -> Option<&JournalSnapshot<'j>> {
        self.journal_snapshot.as_ref()
    }

    /// Returns the corresponding [`TaskProcessor`].
    pub(super) fn task_processor(&self) -> &TaskProcessor {
        self.task_processor
    }
}

impl<'d, 't, 'j, S: Sequencer> PartialEq<S::Instant> for Snapshot<'d, 't, 'j, S> {
    #[inline]
    fn eq(&self, other: &S::Instant) -> bool {
        self.database_snapshot().eq(other)
    }
}

impl<'d, 't, 'j, S: Sequencer> PartialOrd<S::Instant> for Snapshot<'d, 't, 'j, S> {
    #[inline]
    fn partial_cmp(&self, other: &S::Instant) -> Option<cmp::Ordering> {
        self.database_snapshot().partial_cmp(other)
    }
}

impl<'t> TransactionSnapshot<'t> {
    /// Creates a new [`TransactionSnapshot`].
    pub(super) fn new(id: TransactionID, instant: Option<NonZeroU32>) -> TransactionSnapshot<'t> {
        TransactionSnapshot {
            id,
            instant,
            _phantom: PhantomData,
        }
    }
}

impl<'t> PartialOrd for TransactionSnapshot<'t> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if self.id != other.id {
            return None;
        }
        self.instant.partial_cmp(&other.instant)
    }
}

impl<'j> JournalSnapshot<'j> {
    /// Creates a new [`JournalSnapshot`].
    pub(super) fn new(id: JournalID) -> JournalSnapshot<'j> {
        JournalSnapshot {
            id,
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use tokio::fs::remove_dir_all;

    use crate::Database;

    #[tokio::test]
    async fn combine() {
        const DIR: &str = "snapshot_combine_test";
        const DIR_OTHER: &str = "snapshot_combine_test_other";

        let path = Path::new(DIR);
        let database = Database::with_path(path).await.unwrap();

        let path_other = Path::new(DIR_OTHER);
        let database_other = Database::with_path(path_other).await.unwrap();

        assert!(database.transaction().commit().await.is_ok());

        let database_snapshot = database.snapshot();
        assert_eq!(
            database_snapshot.database_snapshot(),
            database.snapshot().database_snapshot()
        );
        let transaction_other = database_other.transaction();
        let transaction_snapshot_other = transaction_other.snapshot();
        let database_snapshot = database_snapshot.combine(transaction_snapshot_other);
        assert_eq!(
            database_snapshot.database_snapshot(),
            database.snapshot().database_snapshot()
        );
        assert!(database_snapshot.transaction_snapshot().is_none());

        let transaction = database.transaction();
        let transaction_snapshot = transaction.snapshot();

        let combined_snapshot = database_snapshot.combine(transaction_snapshot);
        assert_eq!(
            combined_snapshot.database_snapshot(),
            database.snapshot().database_snapshot()
        );
        assert!(combined_snapshot.transaction_snapshot().is_some());

        let journal = transaction.journal();
        let journal_snapshot = journal.snapshot();

        let combined_snapshot = combined_snapshot.combine(journal_snapshot);
        assert_eq!(
            combined_snapshot.database_snapshot(),
            database.snapshot().database_snapshot()
        );
        assert!(combined_snapshot.transaction_snapshot().is_some());
        assert!(combined_snapshot.journal_snapshot().is_some());

        // TODO: check why it is not automatically dropped here.
        drop(combined_snapshot);

        let transaction_snapshot = transaction.snapshot();
        let journal_snapshot = journal.snapshot();
        let combined_snapshot = journal_snapshot.combine(transaction_snapshot);
        assert_eq!(combined_snapshot.database_snapshot(), 0);
        assert!(combined_snapshot.transaction_snapshot().is_some());
        assert!(combined_snapshot.journal_snapshot().is_some());

        drop(combined_snapshot);
        drop(journal);
        drop(transaction);
        drop(database);
        drop(transaction_other);
        drop(database_other);

        assert!(remove_dir_all(path).await.is_ok());
        assert!(remove_dir_all(path_other).await.is_ok());
    }
}
