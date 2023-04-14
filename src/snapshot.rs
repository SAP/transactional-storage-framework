// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::overseer::Overseer;
use super::sequencer::ToInstant;
use super::{Database, JournalID, PersistenceLayer, Sequencer, TransactionID};
use std::cmp;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::Ordering::Acquire;

/// [`Snapshot`] represents a consistent view on the [`Database`](super::Database).
///
/// A [`Snapshot`] has a [`Instant`](Sequencer::Instant) value corresponding to a database
/// snapshot, and the database snapshot stays stable until the [`Snapshot`] is dropped.
///
/// There are three types of [`Snapshot`] which can be created using different methods.
///  1. [`Database::snapshot`](super::Database::snapshot) creates a [`Snapshot`] which only
///     contains globally committed data.
///  2. [`Transaction::snapshot`](super::Transaction::snapshot) creates a [`Snapshot`] which
///     contains globally committed data and changes in submitted [`Journal`](super::Journal)
///     instances in the same transaction.
///  3. [`Journal::snapshot`](super::Journal::snapshot) creates a [`Snapshot`] which contains
///     globally committed data, changes in submitted [`Journal`](super::Journal) instances in the
///     same transaction, and changes that are pending in the [`Journal`](super::Journal).
#[derive(Clone, Debug)]
pub struct Snapshot<'d, 't, 'j, S: Sequencer> {
    /// The logical instant of the database system being tracked by [`Database`].
    ///
    /// This being `Some` allows the owner of the [`Snapshot`] to observe any changes to the
    /// database until the instant.
    tracker: Option<S::Tracker>,

    /// The logical instant of the transaction.
    ///
    /// This being `Some` allows the owner of the [`Snapshot`] to observe any changes to the
    /// database made by the transaction until the instant.
    transaction_snapshot: Option<TransactionSnapshot<'t>>,

    /// The associated [`Journal`](super::Journal).
    ///
    /// This being `Some` allows the owner of the [`Snapshot`] to observe any changes to the
    /// database made by the [`Journal`].
    journal_snapshot: Option<JournalSnapshot<'j>>,

    /// Enables the [`Snapshot`] to silently sleep until a transaction to be committed or rolled
    /// back.
    overseer: &'d Overseer,
}

/// Data representing the current state of the [`Transaction`](super::Transaction).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct TransactionSnapshot<'t> {
    /// The transaction identifier.
    id: TransactionID,

    /// The logical instant of the transaction.
    instant: usize,

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
    /// Combines two [`Snapshot`] instances to form a single [`Snapshot`].
    ///
    /// If the supplied [`Snapshot`] conflicts with `self`, `self` is prioritized. For instance,
    /// if they are both created by different [`Database`] instances, `self` is returned, or if
    /// they both have transaction snapshots, that of `self` is taken.
    ///
    /// This method allows a journal snapshot taken from a different transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::Database;
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let database_snapshot = database.snapshot();
    /// let transaction_snapshot = transaction.snapshot();
    /// let combined_snapshot = transaction_snapshot.combine(database_snapshot);
    /// ````
    #[inline]
    #[must_use]
    pub fn combine(mut self, mut other: Snapshot<'d, 't, 'j, S>) -> Snapshot<'d, 't, 'j, S> {
        if !ptr::eq(self.overseer, other.overseer) {
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
            overseer: self.overseer,
        }
    }

    /// Returns the time point value of the database snapshot.
    pub(super) fn database_snapshot(&self) -> S::Instant {
        self.tracker
            .as_ref()
            .map_or_else(S::Instant::default, ToInstant::to_instant)
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
            overseer: database.overseer(),
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
            overseer: database.overseer(),
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
            overseer: database.overseer(),
        }
    }

    /// Returns a reference to its [`JournalSnapshot`].
    pub(super) fn journal_snapshot(&self) -> Option<&JournalSnapshot<'j>> {
        self.journal_snapshot.as_ref()
    }

    /// Returns the corresponding [`Overseer`].
    pub(super) fn overseer(&self) -> &Overseer {
        self.overseer
    }

    /// Returns a reference to its [`TransactionSnapshot`].
    pub(super) fn transaction_snapshot(&self) -> Option<&TransactionSnapshot<'t>> {
        self.transaction_snapshot.as_ref()
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
    pub(super) fn new(id: TransactionID, instant: usize) -> TransactionSnapshot<'t> {
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
