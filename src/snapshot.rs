// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor;
use super::sequencer::DeriveClock;
use super::{Journal, Sequencer, Transaction};

use std::sync::atomic::Ordering::Acquire;

use scc::ebr;

/// [`Snapshot`] represents a consistent view on the [`Database`](super::Database).
///
/// A [`Snapshot`] has a [`Clock`](Sequencer::Clock) value corresponding to a database snapshot,
/// and the database snapshot stays stable until the [`Snapshot`] is dropped.
///
/// There are three types of [`Snapshot`] which can be created using different methods.
///  1. [`Database::snapshot`](super::Database::snapshot) creates a [`Snapshot`] which only contains globally
///     committed data.
///  2. [`Transaction::snapshot`](super::Transaction::snapshot) creates a [`Snapshot`] which
///     contains globally committed data and changes in submitted [`Journal`] instances in the same
///     transaction.
///  3. [`Journal::snapshot`](super::Journal::snapshot) creates a [`Snapshot`] which contains
///     globally committed data, changes in submitted [`Journal`] instances in the same
///     transaction, and changes that are pending in the [`Journal`](super::Journal).
#[derive(Clone, Debug)]
pub struct Snapshot<'s, 't, 'j, S: Sequencer> {
    tracker: S::Tracker,
    transaction: Option<(&'t Transaction<'s, S>, usize)>,
    journal: Option<&'j Journal<'s, 't, S>>,
}

impl<'s, 't, 'j, S: Sequencer> Snapshot<'s, 't, 'j, S> {
    /// Creates a new [`Snapshot`].
    pub(super) fn from_parts(
        sequencer: &'s S,
        transaction: Option<&'t Transaction<'s, S>>,
        journal: Option<&'j Journal<'s, 't, S>>,
    ) -> Snapshot<'s, 't, 'j, S> {
        let tracker = sequencer.issue(Acquire);
        Snapshot {
            tracker,
            transaction: transaction.map(|transaction| (transaction, transaction.clock())),
            journal,
        }
    }

    /// Returns `true` if the given transaction record is visible.
    pub(super) fn visible(&self, journal_anchor: &Anchor<S>, barrier: &ebr::Barrier) -> bool {
        journal_anchor.visible(
            self.tracker.clock(),
            self.transaction,
            self.journal,
            barrier,
        )
    }
}
