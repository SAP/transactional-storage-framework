// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor;
use super::sequencer::DeriveClock;
use super::{Journal, Sequencer, Transaction};

use std::sync::atomic::Ordering::Acquire;

use scc::ebr;

/// [Snapshot] represents the state of the entire storage system at a point of time.
///
/// A [Snapshot] has a [Clock](Sequencer::Clock) value corresponding to a database snapshot,
/// and the database snapshot stays stable until the [Snapshot] is dropped.
///
/// There are three types of [Snapshot], and three ways of taking a [Snapshot].
///  1. The `snapshot` method of [Database](super::Database).
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
pub struct Snapshot<'s, 't, 'j, S: Sequencer> {
    sequencer: &'s S,
    tracker: S::Tracker,
    transaction: Option<(&'t Transaction<'s, S>, usize)>,
    journal: Option<&'j Journal<'s, 't, S>>,
    snapshot: S::Clock,
}

impl<'s, 't, 'j, S: Sequencer> Snapshot<'s, 't, 'j, S> {
    /// Creates a new [Snapshot] using the [Clock](super::Sequencer::Clock) value stored in the
    /// supplied [Snapshot].
    pub fn from(
        snapshot: &'s Snapshot<S>,
        transaction: Option<&'t Transaction<'s, S>>,
        journal: Option<&'j Journal<'s, 't, S>>,
    ) -> Snapshot<'s, 't, 'j, S> {
        let tracker = snapshot.tracker.clone();
        Snapshot {
            sequencer: snapshot.sequencer,
            tracker,
            transaction: transaction.map(|transaction| (transaction, transaction.clock())),
            journal,
            snapshot: *snapshot.clock(),
        }
    }

    /// Creates a new [Snapshot].
    ///
    /// The [Clock](Sequencer::Clock) value that the [Snapshot] has is tracked by the given
    /// [Sequencer].
    pub(super) fn new(
        sequencer: &'s S,
        transaction: Option<&'t Transaction<'s, S>>,
        journal: Option<&'j Journal<'s, 't, S>>,
    ) -> Snapshot<'s, 't, 'j, S> {
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

    /// Returns the [Clock](Sequencer::Clock) value of the [Snapshot].
    pub(super) fn clock(&self) -> &S::Clock {
        &self.snapshot
    }

    /// Returns `true` if the given transaction record is visible.
    pub(super) fn visible(&self, journal_anchor: &Anchor<S>, barrier: &ebr::Barrier) -> bool {
        journal_anchor.visible(self.snapshot, self.transaction, self.journal, barrier)
    }
}
