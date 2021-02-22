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
    transaction_clock: Option<(&'s TransactionCell<S>, usize)>,
    snapshot: S::Clock,
}

impl<'s, S: Sequencer> Snapshot<'s, S> {
    /// Takes a new snapshot of the storage.
    ///
    /// The clock value that the new Snapshot instance owns is tracked by the Sequencer.
    pub fn new(
        sequencer: &'s S,
        transaction_clock: Option<(&'s TransactionCell<S>, usize)>,
    ) -> Snapshot<'s, S> {
        let tracker = sequencer.issue();
        let snapshot = tracker
            .as_ref()
            .map_or_else(S::invalid, |tracker| tracker.derive());
        Snapshot {
            sequencer,
            tracker,
            transaction_clock,
            snapshot,
        }
    }

    /// Returns the clock value of the Snapshot.
    pub fn clock(&self) -> &S::Clock {
        &self.snapshot
    }

    /// Returns true if the given transaction clock is visible.
    pub fn visible(&self, transaction_clock: (&TransactionCell<S>, usize)) -> bool {
        self.transaction_clock.as_ref().map_or_else(
            || false,
            |(owner, clock)| {
                owner as *const _ == &transaction_clock.0 as *const _
                    && *clock >= transaction_clock.1
            },
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
