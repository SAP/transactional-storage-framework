// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::access_controller::ObjectState;
use super::overseer::{Overseer, Task};
use super::snapshot::{JournalSnapshot, TransactionSnapshot};
use super::transaction::Anchor as TransactionAnchor;
use super::transaction::{UNFINISHED_TRANSACTION_INSTANT, UNREACHABLE_TRANSACTION_INSTANT};
use super::{Error, PersistenceLayer, Sequencer, Snapshot, Transaction};
use scc::ebr;
use scc::hash_map::OccupiedEntry;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

/// [`Journal`] keeps the change history.
#[derive(Debug)]
pub struct Journal<'d, 't, S: Sequencer, P: PersistenceLayer<S>> {
    /// [`Journal`] borrows [`Transaction`].
    transaction: &'t Transaction<'d, S, P>,

    /// Own log buffer.
    _log_buffer: Option<Box<P::LogBuffer>>,

    /// [`Anchor`] may outlive the [`Journal`].
    anchor: ebr::Arc<Anchor<S>>,
}

/// The type of journal identifiers.
///
/// The identifier of a journal is only within the transaction, and the same identifier can be used
/// after the journal was rolled back.
pub type ID = usize;

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

    /// A flag indicating that the changes in the [`Journal`] are is being rolled back.
    rolled_back: AtomicBool,

    /// A flag indicating that there has been another transaction waiting for any resources this
    /// [`Journal`] has acquired.
    wake_up_others: AtomicBool,

    /// [`Anchor`] itself is formed as a linked list of [`Anchor`] by the [`Transaction`].
    next: ebr::AtomicArc<Anchor<S>>,
}

/// The result of the current access permission request.
#[derive(Debug, Default)]
pub(super) struct AccessRequestResult {
    /// The result can be accessed by both the requester and the processor, therefore the data is
    /// protected by a [`Mutex`].
    result_waker: Mutex<ResultWakerPair>,
}

/// Access request result and [`Waker`] pair.
pub(super) type ResultWakerPair = (Option<Result<bool, Error>>, Option<Waker>);

/// [`AwaitResponse`] is a [`Future`] to await any response to the request to acquire the specified
/// resource.
#[derive(Debug)]
pub(super) struct AwaitResponse<'d> {
    /// The object identifier of the desired resource.
    object_id: usize,

    /// Indicates that the object identifier was sent to the [`Overseer`].
    object_id_registered: bool,

    /// The corresponding [`Overseer`].
    overseer: &'d Overseer,

    /// The deadline.
    deadline: Instant,

    /// The placeholder for the result and [`Waker`].
    result_placeholder: Arc<AccessRequestResult>,
}

/// [`AwaitEOT`] is returned by an [`Anchor`] for the caller to await the final transaction state
/// if the transaction is being committed.
#[derive(Debug)]
pub(super) struct AwaitEOT<'d, S: Sequencer> {
    /// The transaction to be committed or rolled back.
    transaction_anchor: ebr::Arc<TransactionAnchor<S>>,

    /// The associated [`Overseer`] that handles end-of-transaction messages.
    overseer: &'d Overseer,

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
    /// The journal identifier.
    ///
    /// The identifier is only unique in the same transaction, and the same identifier can be used
    /// later if the journal was rolled back.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal_1 = transaction.journal();
    /// let journal_2 = transaction.journal();
    /// assert_ne!(journal_1.id(), journal_2.id());
    /// ```
    #[inline]
    #[must_use]
    pub fn id(&self) -> ID {
        self.anchor.id()
    }

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

    /// Captures the current state of the [`Journal`] as a [`Snapshot`].
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
        Snapshot::from_journal(self.transaction.database(), self.journal_snapshot())
    }

    /// Returns a reference to the [`Overseer`].
    pub(super) fn overseer(&self) -> &'d Overseer {
        self.transaction.database().overseer()
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
            _log_buffer: None,
            anchor: ebr::Arc::new(Anchor::new(transaction_anchor, transaction.now())),
        }
    }

    /// Creates a new [`JournalSnapshot`].
    fn journal_snapshot(&self) -> JournalSnapshot {
        JournalSnapshot::new(self.anchor.id())
    }
}

impl<'d, 't, S: Sequencer, P: PersistenceLayer<S>> Drop for Journal<'d, 't, S, P> {
    #[inline]
    fn drop(&mut self) {
        if self.anchor.submit_instant() == UNFINISHED_TRANSACTION_INSTANT {
            self.anchor.rollback(self.transaction.database().overseer());
        }
    }
}

impl<S: Sequencer> Anchor<S> {
    /// The identifier of the corresponding journal is returned.
    pub(super) fn id(&self) -> ID {
        self as *const Anchor<S> as usize
    }

    /// The transaction identifier is returned.
    pub(super) fn transaction_id(&self) -> usize {
        self.transaction_anchor.as_ptr() as usize
    }

    /// Gets the end-of-transaction time instant.
    ///
    /// Returns `None` if the transaction is not ended.
    pub(super) fn eot_instant(&self) -> Option<S::Instant> {
        self.transaction_anchor.eot_instant()
    }

    /// Checks if the [`Journal`] was rolled back.
    pub(super) fn is_rolled_back(&self) -> bool {
        // The anchor was rolled back.
        self.rolled_back.load(Relaxed)
    }

    /// Checks if the state of the [`Anchor`] is fixed after the transaction was ended or the
    /// associated [`Journal`] was rolled back.
    pub(super) fn is_terminated(&self) -> bool {
        // The transaction was ended.
        self.transaction_anchor.eot_instant().is_some() ||
        // The anchor was rolled back.
        self.is_rolled_back()
    }

    /// Checks if the reader represented by the [`Snapshot`] can read changes made by the
    /// [`Journal`].
    ///
    /// # Errors
    ///
    /// An [`AwaitEOT`] is returned if the reader might be able to read the database object since
    /// the transaction is being committed.
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
            if self.is_rolled_back() {
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
                return Err(self.await_eot(snapshot.overseer(), deadline));
            }
        }

        Ok(false)
    }

    /// Checks if the supplied [`Journal`] is able to modify any outcomes of `self`.
    ///
    /// Returns the relationship between the requester and `self`.
    pub(super) fn grant_write_access(&self, anchor: &Anchor<S>) -> Relationship<S> {
        if ptr::eq(self, anchor) {
            // Requester and the owner are the same.
            Relationship::Linearizable
        } else if let Some(eot_instant) = self.transaction_anchor.eot_instant() {
            // The transaction was ended.
            if eot_instant == S::Instant::default() || self.is_rolled_back() {
                // The transaction or the journal was rolled back.
                //
                // `is_rolled_back()` has to be checked after checking the transaction state.
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
            } else if self.is_rolled_back() {
                // The owner which belongs to the same transaction was rolled back.
                Relationship::RolledBack
            } else {
                Relationship::Concurrent
            }
        } else if self.is_rolled_back() {
            // The owner was rolled back.
            Relationship::RolledBack
        } else {
            Relationship::Unknown
        }
    }

    /// Tells the [`Anchor`] that there is a transaction waiting for a recourse owned by the
    /// [`Anchor`].
    pub(super) fn set_wake_up_others(&self) {
        self.wake_up_others.store(true, Release);
    }

    /// Returns an [`AwaitEOT`] for the caller to await the end of transaction.
    pub(super) fn await_eot<'d>(
        &self,
        overseer: &'d Overseer,
        deadline: Instant,
    ) -> AwaitEOT<'d, S> {
        AwaitEOT {
            transaction_anchor: self.transaction_anchor.clone(),
            overseer,
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
    pub(super) fn commit(&self, overseer: &Overseer) {
        self.wake_up_others(overseer);
    }

    /// Rolls back the changes contained in the associated [`Journal`].
    pub(super) fn rollback(&self, overseer: &Overseer) {
        self.rolled_back.store(true, Release);
        self.wake_up_others(overseer);
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
            rolled_back: AtomicBool::new(false),
            wake_up_others: AtomicBool::new(false),
            next: ebr::AtomicArc::null(),
        }
    }

    fn wake_up_others(&self, overseer: &Overseer) {
        if self.wake_up_others.load(Acquire) {
            // The result can be ignored since messages pending in the queue mean that the access
            // controller will be scanned in the future.
            overseer.send_task(Task::ScanAccessController);
        }
    }
}

impl AccessRequestResult {
    /// Gives full access to the inner data with the mutex acquired.
    ///
    /// It may block the thread; the only component that can be blocked is
    /// [`Overseer`], therefore this must be called by [`Overseer`].
    ///
    /// `None` is returned if the [`Mutex`] is poisoned.
    pub(super) fn lock_sync(&self) -> Option<MutexGuard<ResultWakerPair>> {
        self.result_waker.lock().ok()
    }
}

impl<'d> AwaitResponse<'d> {
    /// Creates a new [`AwaitResponse`].
    pub(super) fn new<S: Sequencer>(
        entry: OccupiedEntry<usize, ObjectState<S>>,
        overseer: &'d Overseer,
        deadline: Instant,
        result_placeholder: Arc<AccessRequestResult>,
    ) -> AwaitResponse<'d> {
        let object_id = *entry.key();
        drop(entry);
        AwaitResponse {
            object_id,
            object_id_registered: false,
            overseer,
            deadline,
            result_placeholder,
        }
    }
}

impl<'d> Future for AwaitResponse<'d> {
    type Output = Result<bool, Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Ok(mut result_waker) = self.result_placeholder.result_waker.try_lock() {
            if let Some(result) = result_waker.0.as_ref() {
                return Poll::Ready(result.clone());
            }
            if self.deadline < Instant::now() {
                // The deadline was reached.
                result_waker.0.replace(Err(Error::Timeout));
                return Poll::Ready(Err(Error::Timeout));
            }
            result_waker.1.replace(cx.waker().clone());
        } else {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        if !self.object_id_registered {
            if self.overseer.send_task(Task::Monitor(self.object_id)) {
                self.get_mut().object_id_registered = true;
            }
            cx.waker().wake_by_ref();
        } else if !self
            .overseer
            .send_task(Task::WakeUp(self.deadline, cx.waker().clone()))
        {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
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
        } else if !self
            .overseer
            .send_task(Task::WakeUp(self.deadline, cx.waker().clone()))
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
