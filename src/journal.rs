// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::snapshot::JournalSnapshot;
use super::transaction::Anchor as TransactionAnchor;
use super::{PersistenceLayer, Sequencer, Snapshot, Transaction};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, Release};

use scc::ebr;

/// [`Journal`] keeps the change history.
#[derive(Debug)]
pub struct Journal<'s, 't, S: Sequencer, P: PersistenceLayer<S>> {
    /// [`Journal`] borrows [`Transaction`].
    transaction: &'t Transaction<'s, S, P>,

    /// [`Anchor`] may outlive the [`Journal`].
    anchor: ebr::Arc<Anchor<S>>,
}

impl<'s, 't, S: Sequencer, P: PersistenceLayer<S>> Journal<'s, 't, S, P> {
    /// Submits the [`Journal`] to the [`Transaction`].
    ///
    /// The logical clock of the corresponding [`Transaction`] advances towards the next time
    /// point, thereby allowing every future [`Journal`] sees the changes in it.
    ///
    /// It returns the updated transaction clock value.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal = transaction.journal();
    /// assert_eq!(journal.submit(), 1);
    /// ```
    #[inline]
    #[must_use]
    pub fn submit(self) -> usize {
        self.transaction.record(&self.anchor)
    }

    /// Captures the current state of the [`Database`](super::Database), the [`Transaction`], and
    /// the [`Journal`] as a [`Snapshot`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::Database;
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal = transaction.journal();
    /// let snapshot = journal.snapshot();
    /// ```
    #[must_use]
    pub fn snapshot<'r>(&'r self) -> Snapshot<'s, 't, 'r, S> {
        Snapshot::from_parts(
            self.transaction.sequencer(),
            Some(
                self.transaction
                    .transaction_snapshot(self.anchor.creation_clock),
            ),
            Some(self.journal_snapshot()),
        )
    }

    /// Creates a new [`Journal`].
    pub(super) fn new(
        transaction: &'t Transaction<'s, S, P>,
        transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
    ) -> Journal<'s, 't, S, P> {
        Journal {
            transaction,
            anchor: ebr::Arc::new(Anchor::new(transaction_anchor, transaction.clock())),
        }
    }

    /// Creates a new [`JournalSnapshot`].
    fn journal_snapshot<'j>(&'j self) -> JournalSnapshot<'t, 'j> {
        JournalSnapshot::new(self.anchor.as_ref() as *const _ as usize)
    }
}

impl<'s, 't, S: Sequencer, P: PersistenceLayer<S>> Drop for Journal<'s, 't, S, P> {
    fn drop(&mut self) {
        if self.anchor.submit_clock() == 0 {
            // Send `anchor` to the garbage collector.
        }
    }
}

/// [`Anchor`] is a piece of data that outlives its associated [`Journal`] allowing asynchronous
/// operations.
#[derive(Debug)]
pub struct Anchor<S: Sequencer> {
    #[allow(unused)]
    transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
    creation_clock: usize,
    submit_clock: AtomicUsize,
    next: ebr::AtomicArc<Anchor<S>>,
}

impl<S: Sequencer> Anchor<S> {
    /// Returns a reference to the `next` field.
    pub(super) fn next(&self) -> &ebr::AtomicArc<Anchor<S>> {
        &self.next
    }

    /// Assigns the transaction local clock when it is submitted.
    pub(super) fn assign_submit_clock(&self, clock: usize) {
        debug_assert_ne!(clock, 0);
        self.submit_clock.store(clock, Release);
    }

    /// Reads its submit clock.
    pub(super) fn submit_clock(&self) -> usize {
        self.submit_clock.load(Relaxed)
    }

    /// Creates a new [`Anchor`].
    fn new(transaction_anchor: ebr::Arc<TransactionAnchor<S>>, creation_clock: usize) -> Anchor<S> {
        Anchor {
            transaction_anchor,
            creation_clock,
            submit_clock: AtomicUsize::new(0),
            next: ebr::AtomicArc::null(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;

    #[tokio::test]
    async fn journal() {
        let storage = Database::default();
        let transaction = storage.transaction();
        let journal_1 = transaction.journal();
        assert_eq!(journal_1.submit(), 1);
        let journal_2 = transaction.journal();
        assert_eq!(journal_2.submit(), 2);
        let journal_3 = transaction.journal();
        let journal_4 = transaction.journal();
        assert_eq!(journal_4.submit(), 3);
        assert_eq!(journal_3.submit(), 4);
    }
}
