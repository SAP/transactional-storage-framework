// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::overseer::Task;
use super::sequencer::ToInstant;
use super::Sequencer;
use std::cmp;
use std::marker::PhantomData;
use std::sync::atomic::Ordering::Acquire;
use std::sync::mpsc::SyncSender;

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
    tracker: S::Tracker,
    #[allow(dead_code)]
    message_sender: &'d SyncSender<Task>,
    transaction_snapshot: Option<TransactionSnapshot<'t>>,
    journal_snapshot: Option<JournalSnapshot<'t, 'j>>,
}

/// Data representing the current state of the [`Transaction`](super::Transaction).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct TransactionSnapshot<'t> {
    anchor_addr: usize,
    instant: usize,
    _phantom: PhantomData<&'t ()>,
}

/// Data representing the current state of the [`Journal`](super::Transaction).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct JournalSnapshot<'t, 'j> {
    anchor_addr: usize,
    _phantom: PhantomData<(&'t (), &'j ())>,
}

impl<'d, 't, 'j, S: Sequencer> Snapshot<'d, 't, 'j, S> {
    /// Creates a new [`Snapshot`].
    pub(super) fn from_parts(
        sequencer: &'d S,
        message_sender: &'d SyncSender<Task>,
        transaction_snapshot: Option<TransactionSnapshot<'t>>,
        journal_snapshot: Option<JournalSnapshot<'t, 'j>>,
    ) -> Snapshot<'d, 't, 'j, S> {
        let tracker = sequencer.track(Acquire);
        Snapshot {
            tracker,
            message_sender,
            transaction_snapshot,
            journal_snapshot,
        }
    }

    /// Returns the time point value of the database snapshot.
    pub(super) fn database_snapshot(&self) -> S::Instant {
        self.tracker.to_instant()
    }

    /// Returns a reference to the message sender.
    pub(super) fn message_sender(&self) -> &SyncSender<Task> {
        self.message_sender
    }

    /// Returns a reference to its [`TransactionSnapshot`].
    pub(super) fn transaction_snapshot(&self) -> Option<&TransactionSnapshot<'t>> {
        self.transaction_snapshot.as_ref()
    }

    /// Returns a reference to its [`JournalSnapshot`].
    pub(super) fn journal_snapshot(&self) -> Option<&JournalSnapshot<'t, 'j>> {
        self.journal_snapshot.as_ref()
    }
}

impl<'d, 't, 'j, S: Sequencer> PartialEq<S::Instant> for Snapshot<'d, 't, 'j, S> {
    #[inline]
    fn eq(&self, other: &S::Instant) -> bool {
        self.tracker.to_instant().eq(other)
    }
}

impl<'d, 't, 'j, S: Sequencer> PartialOrd<S::Instant> for Snapshot<'d, 't, 'j, S> {
    #[inline]
    fn partial_cmp(&self, other: &S::Instant) -> Option<cmp::Ordering> {
        self.tracker.to_instant().partial_cmp(other)
    }
}

impl<'t> TransactionSnapshot<'t> {
    /// Creates a new [`TransactionSnapshot`].
    pub(super) fn new(anchor_addr: usize, instant: usize) -> TransactionSnapshot<'t> {
        TransactionSnapshot {
            anchor_addr,
            instant,
            _phantom: PhantomData,
        }
    }
}

impl<'t> PartialOrd for TransactionSnapshot<'t> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if self.anchor_addr != other.anchor_addr {
            return None;
        }
        self.instant.partial_cmp(&other.instant)
    }
}

impl<'t, 'j> JournalSnapshot<'t, 'j> {
    /// Creates a new [`JournalSnapshot`].
    pub(super) fn new(anchor_addr: usize) -> JournalSnapshot<'t, 'j> {
        JournalSnapshot {
            anchor_addr,
            _phantom: PhantomData,
        }
    }
}
