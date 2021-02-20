// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{DeriveClock, Sequencer, TransactionCell};

/// Snapshot represents a state of a Storage at a point of time.
///
/// Snapshot is a snapshot of data in a Storage at the point of time the snapshot is taken.
/// The data stored in a Storage instance is only accessible through a storage snapshot.
pub struct Snapshot<'s, S: Sequencer> {
    sequencer: &'s S,
    tracker: Option<S::Tracker>,
    transaction_cell: Option<&'s TransactionCell<S>>,
    snapshot: S::Clock,
}

impl<'s, S: Sequencer> Snapshot<'s, S> {
    /// Takes a new snapshot of the storage.
    ///
    /// The clock value that the new Snapshot instance owns is tracked by the Sequencer.
    pub fn new(
        sequencer: &'s S,
        transaction_cell: Option<&'s TransactionCell<S>>,
    ) -> Snapshot<'s, S> {
        let tracker = sequencer.issue();
        let snapshot = tracker
            .as_ref()
            .map_or_else(S::invalid, |tracker| tracker.derive());
        Snapshot {
            sequencer,
            tracker,
            transaction_cell,
            snapshot,
        }
    }

    /// Returns the clock value of the Snapshot.
    pub fn clock(&self) -> &S::Clock {
        &self.snapshot
    }

    /// Returns true if the snapshot belongs to the transaction.
    pub fn own(&self, transaction_cell: &TransactionCell<S>) -> bool {
        self.transaction_cell.as_ref().map_or_else(
            || false,
            |&owner| owner as *const _ == transaction_cell as *const _,
        )
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
