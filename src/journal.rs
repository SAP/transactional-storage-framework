// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::snapshot::JournalSnapshot;
use super::transaction::Anchor as TransactionAnchor;
use super::{PersistenceLayer, Sequencer, Snapshot, Transaction};
use scc::ebr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, Release};

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
    /// use sap_tsf::{Database, Transaction};
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
    /// use sap_tsf::Database;
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
                    .transaction_snapshot(self.anchor.creation_instant),
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
            anchor: ebr::Arc::new(Anchor::new(transaction_anchor, transaction.now())),
        }
    }

    /// Creates a new [`JournalSnapshot`].
    fn journal_snapshot<'j>(&'j self) -> JournalSnapshot<'t, 'j> {
        JournalSnapshot::new(self.anchor.as_ref() as *const _ as usize)
    }
}

impl<'s, 't, S: Sequencer, P: PersistenceLayer<S>> Drop for Journal<'s, 't, S, P> {
    fn drop(&mut self) {
        if self.anchor.submit_instant() == 0 {
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
    creation_instant: usize,
    submit_instant: AtomicUsize,
    next: ebr::AtomicArc<Anchor<S>>,
}

impl<S: Sequencer> Anchor<S> {
    /// Returns a reference to the `next` field.
    pub(super) fn next(&self) -> &ebr::AtomicArc<Anchor<S>> {
        &self.next
    }

    /// Assigns the instant when the it was submitted to the transaction.
    pub(super) fn assign_submit_instant(&self, instant: usize) {
        debug_assert_ne!(instant, 0);
        self.submit_instant.store(instant, Release);
    }

    /// Reads its submit instant.
    pub(super) fn submit_instant(&self) -> usize {
        self.submit_instant.load(Relaxed)
    }

    /// Creates a new [`Anchor`].
    fn new(
        transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
        creation_instant: usize,
    ) -> Anchor<S> {
        Anchor {
            transaction_anchor,
            creation_instant,
            submit_instant: AtomicUsize::new(0),
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
