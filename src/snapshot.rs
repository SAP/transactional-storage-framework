// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{DeriveClock, Sequencer, Transaction, TransactionCell};
use crossbeam_epoch::{Guard, Shared};

/// Snapshot represents a state of a Storage at a point of time.
///
/// Snapshot is a snapshot of data in a Storage at the point of time the snapshot is taken.
/// The data stored in a Storage instance is only accessible through a storage snapshot.
pub struct Snapshot<'s, S: Sequencer> {
    sequencer: &'s S,
    tracker: Option<S::Tracker>,
    transaction_clock: Option<(&'s Transaction<'s, S>, usize)>,
    snapshot: S::Clock,
}

impl<'s, S: Sequencer> Snapshot<'s, S> {
    /// Takes a new snapshot of the storage.
    ///
    /// The clock value that the new Snapshot instance owns is tracked by the Sequencer.
    pub fn new(sequencer: &'s S, transaction: Option<&'s Transaction<S>>) -> Snapshot<'s, S> {
        let tracker = sequencer.issue();
        let snapshot = tracker
            .as_ref()
            .map_or_else(S::invalid, |tracker| tracker.derive());
        Snapshot {
            sequencer,
            tracker,
            transaction_clock: transaction.map(|transaction| (transaction, transaction.clock())),
            snapshot,
        }
    }

    /// Returns the clock value of the Snapshot.
    pub fn clock(&self) -> &S::Clock {
        &self.snapshot
    }

    /// Returns the transaction-local clock value of the Snapshot.
    pub fn transaction_clock<'g>(
        &self,
        guard: &'g Guard,
    ) -> Option<(Shared<'g, TransactionCell<S>>, usize)> {
        self.transaction_clock.as_ref().map(|transaction_clock| {
            (
                transaction_clock.0.transaction_cell(guard),
                transaction_clock.1,
            )
        })
    }
}

impl<'s, S: Sequencer> Drop for Snapshot<'s, S> {
    /// It has a clock value that is tracked by the sequencer, therefore it must be confiscated.
    fn drop(&mut self) {
        if let Some(tracker) = self.tracker.take() {
            self.sequencer.confiscate(tracker);
        }
    }
}
