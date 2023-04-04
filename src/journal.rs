// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::access_controller::PromotedAccess;
use super::overseer::Task;
use super::snapshot::{JournalSnapshot, TransactionSnapshot};
use super::transaction::Anchor as TransactionAnchor;
use super::transaction::{UNFINISHED_TRANSACTION_INSTANT, UNREACHABLE_TRANSACTION_INSTANT};
use super::{Error, PersistenceLayer, Sequencer, Snapshot, Transaction};
use scc::{ebr, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::SyncSender;
use std::task::{Context, Poll};
use std::time::Instant;

/// [`Journal`] keeps the change history.
#[derive(Debug)]
pub struct Journal<'d, 't, S: Sequencer, P: PersistenceLayer<S>> {
    /// [`Journal`] borrows [`Transaction`].
    transaction: &'t Transaction<'d, S, P>,

    /// [`Anchor`] may outlive the [`Journal`].
    anchor: ebr::Arc<Anchor<S>>,
}

/// [`Anchor`] is a piece of data that outlives its associated [`Journal`] allowing asynchronous
/// operations.
#[derive(Debug)]
pub(super) struct Anchor<S: Sequencer> {
    /// Points to the key fields of the [`Transaction`].
    transaction_anchor: ebr::Arc<TransactionAnchor<S>>,

    /// The time point when the [`Journal`] was created.
    creation_instant: usize,

    /// The transaction instant when the [`Journal`] was submitted.
    ///
    /// This being `zero` represents a state where the changes made by the [`Journal`] cannot be
    /// exposed since the [`Journal`] has yet to be submitted or has been rolled back.
    submit_instant: AtomicUsize,

    /// Promoted access permission is kept in the [`Journal`] to roll back the access when the
    /// [`Journal`] has to be rolled back.
    ///
    /// ## Warning
    ///
    /// `promoted_access` cannot be mutably accessed with a mutable reference to an entry in an
    /// [`AccessController`](super::AccessController) held, otherwise a deadlock may occur.
    #[allow(dead_code)]
    promoted_access: HashMap<usize, PromotedAccess<S>>,

    /// [`Anchor`] itself is formed as a linked list of [`Anchor`] by the [`Transaction`].
    next: ebr::AtomicArc<Anchor<S>>,
}

/// [`AwaitEOT`] is returned by an [`Anchor`] for the caller to await the final transaction state
/// if the transaction is being committed.
#[derive(Debug)]
pub(super) struct AwaitEOT<'d, S: Sequencer> {
    transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
    message_sender: &'d SyncSender<Task>,
    deadline: Instant,
}

impl<'d, 't, S: Sequencer, P: PersistenceLayer<S>> Journal<'d, 't, S, P> {
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
    #[inline]
    #[must_use]
    pub fn snapshot<'r>(&'r self) -> Snapshot<'d, 't, 'r, S> {
        Snapshot::from_parts(
            self.transaction.sequencer(),
            self.transaction.sender(),
            Some(
                self.transaction
                    .transaction_snapshot(self.anchor.creation_instant),
            ),
            Some(self.journal_snapshot()),
        )
    }

    /// Returns a reference to its [`Anchor`].
    #[inline]
    #[must_use]
    #[allow(dead_code)]
    pub(super) fn anchor(&self) -> &ebr::Arc<Anchor<S>> {
        &self.anchor
    }

    /// Creates a new [`Journal`].
    pub(super) fn new(
        transaction: &'t Transaction<'d, S, P>,
        transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
    ) -> Journal<'d, 't, S, P> {
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

impl<'d, 't, S: Sequencer, P: PersistenceLayer<S>> Drop for Journal<'d, 't, S, P> {
    fn drop(&mut self) {
        if self.anchor.submit_instant() == 0 {
            // Send `anchor` to the garbage collector.
        }
    }
}

impl<S: Sequencer> Anchor<S> {
    /// Checks if the reader represented by the [`Snapshot`] can read changes made by the [`Journal`].
    ///
    /// # Errors
    ///
    /// An [`AwaitEOT`] is returned if the reader might be able to read the database object if the
    /// transaction is committed.
    pub(super) fn grant_read_access<'d>(
        &self,
        snapshot: &'d Snapshot<'_, '_, '_, S>,
        deadline: Option<Instant>,
    ) -> Result<bool, AwaitEOT<'d, S>> {
        if let Some(journal_snapshot) = snapshot.journal_snapshot() {
            if JournalSnapshot::new(self as *const _ as usize) == *journal_snapshot {
                // It comes from the same transaction, and same journal.
                return Ok(true);
            }
        }

        if let Some(transaction_snapshot) = snapshot.transaction_snapshot() {
            if TransactionSnapshot::new(
                self.transaction_anchor.as_ptr() as usize,
                self.creation_instant,
            ) <= *transaction_snapshot
            {
                // It comes from the same transaction, and a newer journal.
                return Ok(true);
            }
        }

        if let Some(eot_instant) = self.transaction_anchor.eot_instant() {
            return Ok(
                eot_instant != S::Instant::default() && eot_instant <= snapshot.database_snapshot()
            );
        }

        if let Some(prepare_instant) = self.transaction_anchor.prepare_instant() {
            if prepare_instant != S::Instant::default()
                && prepare_instant >= snapshot.database_snapshot()
            {
                return Ok(false);
            }

            if let Some(deadline) = deadline {
                return Err(self.await_eot(snapshot.message_sender(), deadline));
            }
        }

        Ok(false)
    }

    /// Checks if the supplied [`Journal`] is able to update a piece of data created by the
    /// [`Journal`] represented by `self`.
    ///
    /// Returns the instant when the transaction was committed or rolled back if the transaction is
    /// not active anymore, otherwise `None` is returned.
    pub(super) fn grant_write_access<P: PersistenceLayer<S>>(
        &self,
        _journal: &mut Journal<'_, '_, S, P>,
    ) -> Option<S::Instant> {
        // TODO: intra-transaction visibility control.
        self.transaction_anchor.eot_instant()
    }

    /// Returns an [`AwaitEOT`] for the caller to await the end of transaction.
    pub(super) fn await_eot<'d>(
        &self,
        message_sender: &'d SyncSender<Task>,
        deadline: Instant,
    ) -> AwaitEOT<'d, S> {
        AwaitEOT {
            transaction_anchor: self.transaction_anchor.clone(),
            message_sender,
            deadline,
        }
    }

    /// Sets the next [`Anchor`].
    pub(super) fn set_next(
        &self,
        next: Option<ebr::Arc<Anchor<S>>>,
        order: Ordering,
    ) -> Option<ebr::Arc<Anchor<S>>> {
        let new_submit_instant = next
            .as_ref()
            .map_or(UNFINISHED_TRANSACTION_INSTANT + 1, |a| {
                a.submit_instant().min(UNREACHABLE_TRANSACTION_INSTANT - 1) + 1
            });
        self.submit_instant.store(new_submit_instant, order);
        self.next.swap((next, ebr::Tag::None), order).0
    }

    /// Rolls back the changes contained in the associated [`Journal`].
    pub(super) fn rollback(&self) {
        self.submit_instant
            .store(UNFINISHED_TRANSACTION_INSTANT, Release);
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
            submit_instant: AtomicUsize::new(UNFINISHED_TRANSACTION_INSTANT),
            promoted_access: HashMap::default(),
            next: ebr::AtomicArc::null(),
        }
    }
}

impl<'d, S: Sequencer> Future for AwaitEOT<'d, S> {
    type Output = Result<(), Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.transaction_anchor.eot_instant().is_some() {
            // The transaction has been committed or rolled back.
            return Poll::Ready(Ok(()));
        } else if self.deadline < Instant::now() {
            // The deadline was reached.
            return Poll::Ready(Err(Error::Timeout));
        } else if self
            .transaction_anchor
            .wait_eot(cx.waker().clone())
            .is_some()
        {
            // The transaction has just been committed or rolled back right after the `Waker` was
            // pushed into the transaction.
            return Poll::Ready(Ok(()));
        }

        if self
            .message_sender
            .try_send(Task::WakeUp(self.deadline, cx.waker().clone()))
            .is_err()
        {
            // The message channel is congested.
            cx.waker().wake_by_ref();
        }
        Poll::Pending
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
