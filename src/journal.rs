// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::access_controller::{ObjectState, Owner};
use super::overseer::Task;
use super::snapshot::{JournalSnapshot, TransactionSnapshot};
use super::transaction::Anchor as TransactionAnchor;
use super::transaction::{UNFINISHED_TRANSACTION_INSTANT, UNREACHABLE_TRANSACTION_INSTANT};
use super::{Error, PersistenceLayer, Sequencer, Snapshot, Transaction};
use scc::ebr;
use scc::hash_map::OccupiedEntry;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
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

    /// A placeholder for asynchronous resource acquisition requests.
    ///
    /// The field must be reset before making a new asynchronous request to an
    /// [`AccessController`](super::AccessController). The data is protected by a [`Mutex`],
    /// however the [`Mutex`] is only try-locked, and if it fails, the task immediately yields the
    /// current executor.
    access_request_result_placeholder: Mutex<AccessRequestResult>,

    /// A flag indicating that the changes in the [`Journal`] are is being rolled back.
    rolled_back: AtomicBool,

    /// A flag indicating that there has been another transaction waiting for any resources this
    /// [`Journal`] has acquired.
    wake_up_others: AtomicBool,

    /// [`Anchor`] itself is formed as a linked list of [`Anchor`] by the [`Transaction`].
    next: ebr::AtomicArc<Anchor<S>>,
}

/// [`AwaitResponse`] is a [`Future`] to await any response to the request to acquire the specified
/// resource.
#[derive(Debug)]
pub(super) struct AwaitResponse<'d, S: Sequencer> {
    /// The owner.
    owner: Owner<S>,

    /// The object identifier of the desired resource.
    object_id: usize,

    /// The message sender to which send a wake up message.
    message_sender: &'d SyncSender<Task>,

    /// The deadline.
    deadline: Instant,
}

/// The result of the current access permission request.
#[derive(Debug, Default)]
pub(super) struct AccessRequestResult {
    /// [`Waker`] to wake up when the result is ready.
    waker: Option<Waker>,

    /// Result of the resource acquisition attempt.
    ///
    /// `true` is set if the resource is newly acquired. `false` is set if the transaction already
    /// has ownership.
    result: Option<Result<bool, Error>>,
}

/// [`AwaitEOT`] is returned by an [`Anchor`] for the caller to await the final transaction state
/// if the transaction is being committed.
#[derive(Debug)]
pub(super) struct AwaitEOT<'d, S: Sequencer> {
    /// The transaction to be committed or rolled back.
    transaction_anchor: ebr::Arc<TransactionAnchor<S>>,

    /// The message sender to which send a wake up message.
    message_sender: &'d SyncSender<Task>,

    /// The deadline.
    deadline: Instant,
}

/// Relationship between the access requester and the database object owner.
#[derive(Debug)]
pub(super) enum Relationship<S: Sequencer> {
    /// The owner was committed.
    ///
    /// The requester can take ownership as long as the commit instant is kept in the access
    /// controller.
    Committed(S::Instant),

    /// The owner was rolled back.
    ///
    /// The requester can take ownership.
    RolledBack,

    /// The ownership can be transferred.
    ///
    /// The requester belongs to the same transaction, and they are linearizable.
    Linearizable,

    /// The ownership cannot be transferred.
    ///
    /// The requester belongs to the same transaction, but they are not linearizable.
    Concurrent,

    /// The requester has to wait to correctly determine the relationship.
    Unknown,
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
    pub fn snapshot<'j>(&'j self) -> Snapshot<'d, 't, 'j, S> {
        Snapshot::from_parts(
            self.transaction.sequencer(),
            self.transaction.message_sender(),
            Some(
                self.transaction
                    .transaction_snapshot(self.anchor.creation_instant),
            ),
            Some(self.journal_snapshot()),
        )
    }

    /// Returns a reference to the message sender owned by the [`Database`](super::Database).
    pub(super) fn message_sender(&self) -> &'d SyncSender<Task> {
        self.transaction.message_sender()
    }

    /// Returns a reference to its [`Anchor`].
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
        if self.anchor.submit_instant() == UNFINISHED_TRANSACTION_INSTANT {
            self.anchor.rollback(self.transaction.message_sender());
        }
    }
}

impl<S: Sequencer> Anchor<S> {
    /// The transaction identifier is returned.
    pub(super) fn transaction_id(&self) -> usize {
        self.transaction_anchor.as_ptr() as usize
    }

    /// Checks if the state of the [`Anchor`] is fixed after the transaction was ended or the
    /// associated [`Journal`] was rolled back.
    pub(super) fn is_terminated(&self) -> bool {
        // The transaction was ended.
        self.transaction_anchor.eot_instant().is_some() ||
        // The anchor was rolled back.
        self.rolled_back.load(Relaxed)
    }

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
                // It comes from the same transaction and journal.
                return Ok(true);
            }
        }

        if let Some(transaction_snapshot) = snapshot.transaction_snapshot() {
            let submit_instant = self.submit_instant();
            if submit_instant != UNFINISHED_TRANSACTION_INSTANT
                && TransactionSnapshot::new(self.transaction_id(), submit_instant)
                    <= *transaction_snapshot
            {
                // `snapshot` comes from the same transaction, but a newer journal.
                return Ok(true);
            }
        }

        if let Some(eot_instant) = self.transaction_anchor.eot_instant() {
            if self.rolled_back.load(Relaxed) {
                // The journal was rolled back.
                //
                // `rolled_back` has to be checked after checking the transaction state.
                return Ok(false);
            }
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
    /// Returns the relationship between the requester and `self`.
    pub(super) fn grant_write_access(&self, anchor: &Anchor<S>) -> Relationship<S> {
        if let Some(eot_instant) = self.transaction_anchor.eot_instant() {
            // The transaction was ended.
            if eot_instant == S::Instant::default() || self.rolled_back.load(Relaxed) {
                // The transaction or the journal was rolled back.
                //
                // `rolled_back` has to be checked after checking the transaction state.
                Relationship::RolledBack
            } else {
                Relationship::Committed(eot_instant)
            }
        } else if self.transaction_id() == anchor.transaction_id() {
            // They are from the same transaction.
            let submit_instant = self.submit_instant();
            if submit_instant != UNFINISHED_TRANSACTION_INSTANT
                && submit_instant <= anchor.creation_instant
            {
                // The requester is a newer journal in the transaction, or the same with the owner.
                Relationship::Linearizable
            } else if self.rolled_back.load(Relaxed) {
                // The owner which belongs to the same transaction was rolled back.
                Relationship::RolledBack
            } else {
                Relationship::Concurrent
            }
        } else if self.rolled_back.load(Relaxed) {
            // The owner was rolled back.
            Relationship::RolledBack
        } else {
            Relationship::Unknown
        }
    }

    /// Clears its [`AccessRequestResult`] field.
    pub(super) fn clear_access_request_result_placeholder(&self) {
        // Locking is infallible.
        let placeholder = self.access_request_result_placeholder.try_lock();
        debug_assert!(placeholder.is_ok());

        if let Ok(mut placeholder) = placeholder {
            *placeholder = AccessRequestResult::default();
        }
    }

    /// Returns a reference to its [`AccessRequestResult`] field.
    pub(super) fn access_request_result_placeholder(&self) -> &Mutex<AccessRequestResult> {
        &self.access_request_result_placeholder
    }

    /// Tells the [`Anchor`] that there is a transaction waiting for a recourse owned by the
    /// [`Anchor`].
    pub(super) fn need_to_wake_up_others(&self) {
        self.wake_up_others.store(true, Release);
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

    /// Wakes up any waiting transactions.
    pub(super) fn commit(&self, message_sender: &SyncSender<Task>) {
        self.wake_up_others(message_sender);
    }

    /// Rolls back the changes contained in the associated [`Journal`].
    pub(super) fn rollback(&self, message_sender: &SyncSender<Task>) {
        self.rolled_back.store(true, Release);
        self.wake_up_others(message_sender);
    }

    /// Reads its submit instant.
    pub(super) fn submit_instant(&self) -> usize {
        self.submit_instant.load(Acquire)
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
            access_request_result_placeholder: Mutex::default(),
            rolled_back: AtomicBool::new(false),
            wake_up_others: AtomicBool::new(false),
            next: ebr::AtomicArc::null(),
        }
    }

    fn wake_up_others(&self, message_sender: &SyncSender<Task>) {
        if self.wake_up_others.load(Acquire) {
            // The result can be ignored since messages pending in the queue means that the access
            // controller will be scanned in the future.
            let _: Result<(), TrySendError<Task>> =
                message_sender.try_send(Task::ScanAccessController);
        }
    }
}

impl<'d, S: Sequencer> AwaitResponse<'d, S> {
    /// Creates a new [`AwaitResponse`].
    pub(super) fn new(
        owner: Owner<S>,
        entry: OccupiedEntry<usize, ObjectState<S>>,
        message_sender: &'d SyncSender<Task>,
        deadline: Instant,
    ) -> AwaitResponse<'d, S> {
        let object_id = *entry.key();
        drop(entry);
        owner.clear_access_request_result_placeholder();
        AwaitResponse {
            owner,
            object_id,
            message_sender,
            deadline,
        }
    }
}

impl<'d, S: Sequencer> Future for AwaitResponse<'d, S> {
    type Output = Result<bool, Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Ok(mut placeholder) = self.owner.access_request_result_placeholder().try_lock() {
            if let Some(result) = placeholder.result.take() {
                return Poll::Ready(result);
            }
            if self.deadline < Instant::now() {
                // The deadline was reached.
                let result = placeholder.set_result(Err(Error::Timeout));
                debug_assert!(result.is_ok());
                return Poll::Ready(Err(Error::Timeout));
            }
            placeholder.waker.replace(cx.waker().clone());
        } else {
            cx.waker().wake_by_ref();
        }
        if self
            .message_sender
            .try_send(Task::Monitor(self.object_id))
            .is_err()
        {
            // The message channel is congested.
            cx.waker().wake_by_ref();
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

impl AccessRequestResult {
    /// Sets the result.
    pub(super) fn set_result(&mut self, result: Result<bool, Error>) -> Result<(), Error> {
        if let Some(result) = self.result.as_ref() {
            // `Error::Timeout` can be set by the requester.
            debug_assert_eq!(*result, Err(Error::Timeout));
            return Err(Error::Timeout);
        }
        self.result.replace(result);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        Ok(())
    }

    /// Checks if a result is set.
    pub(super) fn is_result_set(&self) -> bool {
        self.result.is_some()
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

    #[test]
    fn journal() {
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
