// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{DeriveClock, Journal, JournalAnchor, Sequencer, Transaction};
use crossbeam_epoch::Guard;

/// Snapshot represents the state of the entire storage system at a point of time.
///
/// There are three ways of taking a snapshot.
///  1. Storage::snapshot.
///     The snapshot consists of globally committed data at the moment.
///  2. Transaction::snapshot.
///     The snapshot additionally includes changes in the submitted Journal instances in the Transaction.
///     The changes can be rolled back, therefore the snapshot cannot outlive the Transaction.
///  3. Journal::snapshot.
///     The snapshot additionally includes changes in a Journal that has yet to be submitted.
///     The changes can be discarded if the Journal is not submitted, therefore the snapshot cannot outlive the Journal.
pub struct Snapshot<'s, 't, 'r, S: Sequencer> {
    sequencer: &'s S,
    tracker: Option<S::Tracker>,
    transaction: Option<(&'t Transaction<'s, S>, usize)>,
    journal: Option<&'r Journal<'s, 't, S>>,
    snapshot: S::Clock,
}

impl<'s, 't, 'r, S: Sequencer> Snapshot<'s, 't, 'r, S> {
    /// Creates a new Snapshot.
    ///
    /// The clock value that the new Snapshot instance owns is tracked by the given Sequencer.
    pub fn new(
        sequencer: &'s S,
        transaction: Option<&'t Transaction<'s, S>>,
        journal: Option<&'r Journal<'s, 't, S>>,
    ) -> Snapshot<'s, 't, 'r, S> {
        let tracker = sequencer.issue();
        let snapshot = tracker
            .as_ref()
            .map_or_else(S::invalid, |tracker| tracker.derive());
        Snapshot {
            sequencer,
            tracker,
            transaction: transaction.map(|transaction| (transaction, transaction.clock())),
            journal,
            snapshot,
        }
    }

    /// Returns the clock value of the Snapshot.
    pub fn clock(&self) -> &S::Clock {
        &self.snapshot
    }

    /// Returns true if the given transaction record is visible.
    pub fn visible(&self, journal_anchor: &JournalAnchor<S>, guard: &Guard) -> bool {
        self.transaction.as_ref().map_or_else(
            || false,
            |(transaction, transaction_clock)| {
                let journal_ref = self.journal.as_ref().copied();
                journal_anchor.predate(transaction, *transaction_clock, journal_ref, guard)
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
