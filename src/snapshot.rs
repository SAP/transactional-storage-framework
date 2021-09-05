// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor;
use super::{DeriveClock, Journal, Sequencer, Transaction};

use std::sync::atomic::Ordering::Acquire;

use scc::ebr;

/// [Snapshot] represents the state of the entire storage system at a point of time.
///
/// There are three ways of taking a [Snapshot].
///  1. The `snapshot` method of [Storage](super::Storage).
///     The snapshot consists of globally committed data at the moment.
///  2. The `snapshot` method in [Transaction].
///     The snapshot additionally includes changes in submitted [Journal] instances in the
///     [Transaction]. Since the changes can be rolled back, therefore the snapshot cannot
///     outlive the [Transaction].
///  3. The `snapshot` method in [Journal].
///     The snapshot additionally includes changes in a [Journal] that has yet to be submitted.
///     The changes can be discarded if the [Journal] is not submitted, therefore the snapshot
///     cannot outlive it.
#[derive(Clone)]
pub struct Snapshot<'s, 't, 'r, S: Sequencer> {
    sequencer: &'s S,
    tracker: S::Tracker,
    transaction: Option<(&'t Transaction<'s, S>, usize)>,
    journal: Option<&'r Journal<'s, 't, S>>,
    snapshot: S::Clock,
}

impl<'s, 't, 'r, S: Sequencer> Snapshot<'s, 't, 'r, S> {
    /// Creates a new [Snapshot].
    ///
    /// The clock value that the [Snapshot] owns is tracked by the given [Sequencer].
    pub(super) fn new(
        sequencer: &'s S,
        transaction: Option<&'t Transaction<'s, S>>,
        journal: Option<&'r Journal<'s, 't, S>>,
    ) -> Snapshot<'s, 't, 'r, S> {
        let tracker = sequencer.issue(Acquire);
        let snapshot = tracker.clock();
        Snapshot {
            sequencer,
            tracker,
            transaction: transaction.map(|transaction| (transaction, transaction.clock())),
            journal,
            snapshot,
        }
    }

    /// Creates a new [Snapshot] using the [Clock](super::Sequencer::Clock) value stored in the
    /// supplied [Snapshot].
    pub(super) fn from(
        sequencer: &'s S,
        snapshot: &'s Snapshot<S>,
        transaction: Option<&'t Transaction<'s, S>>,
        journal: Option<&'r Journal<'s, 't, S>>,
    ) -> Snapshot<'s, 't, 'r, S> {
        let tracker = snapshot.tracker.clone();
        Snapshot {
            sequencer,
            tracker,
            transaction: transaction.map(|transaction| (transaction, transaction.clock())),
            journal,
            snapshot: *snapshot.clock(),
        }
    }

    /// Returns the clock value of the [Snapshot].
    pub(super) fn clock(&self) -> &S::Clock {
        &self.snapshot
    }

    /// Returns `true` if the given transaction record is visible.
    pub(super) fn visible(&self, journal_anchor: &Anchor<S>, barrier: &ebr::Barrier) -> bool {
        journal_anchor.visible(self.snapshot, self.transaction, self.journal, barrier)
    }
}
