// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{DeriveClock, Sequencer, Transaction, TransactionRecord, TransactionRecordAnchor};

/// Snapshot represents a state of the entire storage system at a point of time.
///
/// A snapshot of data in a storage system at a point of time is represented as Snapshot.
///
/// There are three ways of taking a snapshot.
///  1. Storage::snapshot.
///     The snapshot consists of globally committed changes at the moment.
///  2. Transaction::snapshot.
///     The snapshot additionally includes uncommitted changes that are submitted to the transaction.
///     The changes can be rolled back, therefore the snapshot cannot outlive the transaction.
///  3. TransactionRecord::snapshot.
///     The snapshot includes changes that have not been submitted to the transaction.
///     The changes can be discarded if not submitted, therefore the snapshot cannot outlive the transaction record.
///
/// All the cases, Snapshot cannot outlive the storage, transaction, or transaction record.
pub struct Snapshot<'s, 't, 'r, S: Sequencer> {
    sequencer: &'s S,
    tracker: Option<S::Tracker>,
    transaction: Option<(&'t Transaction<'s, S>, usize)>,
    transaction_record: Option<&'r TransactionRecord<'s, 't, S>>,
    snapshot: S::Clock,
}

impl<'s, 't, 'r, S: Sequencer> Snapshot<'s, 't, 'r, S> {
    /// Takes a new snapshot of the storage.
    ///
    /// The clock value that the new Snapshot instance owns is tracked by the Sequencer.
    pub fn new(
        sequencer: &'s S,
        transaction: Option<&'t Transaction<'s, S>>,
        transaction_record: Option<&'r TransactionRecord<'s, 't, S>>,
    ) -> Snapshot<'s, 't, 'r, S> {
        let tracker = sequencer.issue();
        let snapshot = tracker
            .as_ref()
            .map_or_else(S::invalid, |tracker| tracker.derive());
        Snapshot {
            sequencer,
            tracker,
            transaction: transaction.map(|transaction| (transaction, transaction.clock())),
            transaction_record,
            snapshot,
        }
    }

    /// Returns the clock value of the Snapshot.
    pub fn clock(&self) -> &S::Clock {
        &self.snapshot
    }

    /// Returns true if the given transaction record is visible.
    pub fn visible(&self, transaction_record_anchor: &TransactionRecordAnchor<S>) -> bool {
        self.transaction.as_ref().map_or_else(
            || false,
            |(_, transaction_clock)| {
                let anchor_ref = self
                    .transaction_record
                    .as_ref()
                    .map(|record| (*record).anchor_ref());
                transaction_record_anchor.predate(*transaction_clock, anchor_ref)
            },
        )
    }
}

impl<'s, 't, 'r, S: Sequencer> Drop for Snapshot<'s, 't, 'r, S> {
    /// It has a clock value that is tracked by the sequencer, therefore it must be confiscated.
    fn drop(&mut self) {
        if let Some(tracker) = self.tracker.take() {
            self.sequencer.confiscate(tracker);
        }
    }
}
