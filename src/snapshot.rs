// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::Sequencer;
use std::marker::PhantomData;
use std::sync::atomic::Ordering::Acquire;

/// [`Snapshot`] represents a consistent view on the [`Database`](super::Database).
///
/// A [`Snapshot`] has a [`Clock`](Sequencer::Clock) value corresponding to a database snapshot,
/// and the database snapshot stays stable until the [`Snapshot`] is dropped.
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
pub struct Snapshot<'s, 't, 'j, S: Sequencer> {
    #[allow(dead_code)]
    tracker: S::Tracker,
    #[allow(dead_code)]
    transaction_snapshot: Option<TransactionSnapshot<'t>>,
    #[allow(dead_code)]
    journal_snapshot: Option<JournalSnapshot<'t, 'j>>,
    _phantom: PhantomData<&'s ()>,
}

/// Data representing the current state of the [`Transaction`].
#[derive(Clone, Debug)]
pub(super) struct TransactionSnapshot<'t> {
    #[allow(dead_code)]
    anchor_addr: usize,
    #[allow(dead_code)]
    transaction_clock: usize,
    _phantom: PhantomData<&'t ()>,
}

/// Data representing the current state of the [`Journal`].
#[derive(Clone, Debug)]
pub(super) struct JournalSnapshot<'t, 'j> {
    #[allow(dead_code)]
    anchor_addr: usize,
    _phantom: PhantomData<(&'t (), &'j ())>,
}

impl<'s, 't, 'j, S: Sequencer> Snapshot<'s, 't, 'j, S> {
    /// Creates a new [`Snapshot`].
    pub(super) fn from_parts(
        sequencer: &'s S,
        transaction_snapshot: Option<TransactionSnapshot<'t>>,
        journal_snapshot: Option<JournalSnapshot<'t, 'j>>,
    ) -> Snapshot<'s, 't, 'j, S> {
        let tracker = sequencer.track(Acquire);
        Snapshot {
            tracker,
            transaction_snapshot,
            journal_snapshot,
            _phantom: PhantomData,
        }
    }
}

impl<'t> TransactionSnapshot<'t> {
    /// Creates a new [`TransactionSnapshot`].
    pub(super) fn new(anchor_addr: usize, transaction_clock: usize) -> TransactionSnapshot<'t> {
        TransactionSnapshot {
            anchor_addr,
            transaction_clock,
            _phantom: PhantomData,
        }
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
