// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::persistence_layer::BufferredLogger;

use super::access_controller::ObjectState;
use super::snapshot::{JournalSnapshot, TransactionSnapshot};
use super::task_processor::{Task, TaskProcessor};
use super::transaction::Anchor as TransactionAnchor;
use super::transaction::ID as TransactionID;
use super::{Error, PersistenceLayer, Sequencer, Snapshot, Transaction};
use scc::ebr;
use scc::hash_map::OccupiedEntry;
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

/// [`Journal`] keeps the change history.
#[derive(Debug)]
pub struct Journal<'d, 't, S: Sequencer, P: PersistenceLayer<S>> {
    /// [`Journal`] borrows [`Transaction`].
    transaction: &'t Transaction<'d, S, P>,

    /// Own log buffer.
    log_buffer: Option<P::LogBuffer>,

    /// [`Anchor`] may outlive the [`Journal`].
    anchor: ebr::Arc<Anchor<S>>,
}

/// The type of journal identifiers.
///
/// The identifier of a journal is only within the transaction, and the same identifier can be used
/// after the journal was rolled back.
pub type ID = u64;

/// [`Anchor`] is a piece of data that outlives its associated [`Journal`] allowing asynchronous
/// operations.
#[derive(Debug)]
pub(super) struct Anchor<S: Sequencer> {
    /// Points to the key fields of the [`Transaction`].
    transaction_anchor: ebr::Arc<TransactionAnchor<S>>,

    /// The time point when the [`Journal`] was created.
    creation_instant: Option<NonZeroU32>,

    /// The transaction instant when the [`Journal`] was submitted.
    ///
    /// This being `zero` represents a state where the changes made by the [`Journal`] cannot be
    /// exposed since the [`Journal`] has yet to be submitted or has been rolled back.
    submit_instant: AtomicU32,

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

    /// Indicates that the object identifier was sent to the [`TaskProcessor`].
    object_id_registered: bool,

    /// The corresponding [`TaskProcessor`] that monitors database resources being released.
    task_processor: &'d TaskProcessor,

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

    /// The associated [`TaskProcessor`] that handles end-of-transaction messages.
    task_processor: &'d TaskProcessor,

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
    /// # Errors
    ///
    /// Returns an [`Error`] if the persistence layer failed to process the log records.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Transaction};
    /// use std::num::NonZeroU32;
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal = transaction.journal();
    /// assert_eq!(journal.submit().ok(), NonZeroU32::new(1));
    /// ```
    #[inline]
    pub fn submit(mut self) -> Result<NonZeroU32, Error> {
        let submit_instant = self.transaction.record(&self.anchor);
        if let Some(log_buffer) = self.log_buffer.take() {
            log_buffer.flush(
                self.transaction.database().persistence_layer(),
                Some(submit_instant),
                None,
            )?;
        }
        Ok(submit_instant)
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

    /// Returns a reference to the [`TaskProcessor`].
    pub(super) fn task_processor(&self) -> &'d TaskProcessor {
        self.transaction.database().task_processor()
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
            log_buffer: None,
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
        if self.anchor.submit_instant().is_none() {
            self.anchor
                .rollback(self.transaction.database().task_processor());
        }
    }
}

impl<S: Sequencer> Anchor<S> {
    /// The identifier of the corresponding journal is returned.
    pub(super) fn id(&self) -> ID {
        self as *const Anchor<S> as ID
    }

    /// The transaction identifier is returned.
    pub(super) fn transaction_id(&self) -> TransactionID {
        self.transaction_anchor.as_ptr() as TransactionID
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
            if JournalSnapshot::new(self as *const _ as u64) == *journal_snapshot {
                // It comes from the same transaction and journal.
                return Ok(true);
            }
        }

        if let Some(transaction_snapshot) = snapshot.transaction_snapshot() {
            let submit_instant = self.submit_instant();
            if submit_instant.is_some()
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
                return Err(self.await_eot(snapshot.task_processor(), deadline));
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
            if submit_instant.map_or(false, |i| anchor.creation_instant.map_or(false, |a| i <= a)) {
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
        task_processor: &'d TaskProcessor,
        deadline: Instant,
    ) -> AwaitEOT<'d, S> {
        AwaitEOT {
            transaction_anchor: self.transaction_anchor.clone(),
            task_processor,
            deadline,
        }
    }

    /// Sets the next [`Anchor`].
    pub(super) fn set_next(
        &self,
        next: Option<ebr::Arc<Anchor<S>>>,
        order: Ordering,
    ) -> (Option<ebr::Arc<Anchor<S>>>, NonZeroU32) {
        let new_submit_instant = next.as_ref().and_then(|a| a.submit_instant()).map_or(
            // Safety: `1` is definitely non-zero.
            unsafe { NonZeroU32::new_unchecked(1) },
            |i| i.saturating_add(1),
        );
        self.submit_instant.store(new_submit_instant.into(), order);
        (
            self.next.swap((next, ebr::Tag::None), order).0,
            new_submit_instant,
        )
    }

    /// Wakes up any waiting transactions.
    pub(super) fn commit(&self, task_processor: &TaskProcessor) {
        self.wake_up_others(task_processor);
    }

    /// Rolls back the changes contained in the associated [`Journal`].
    pub(super) fn rollback(&self, task_processor: &TaskProcessor) {
        self.rolled_back.store(true, Release);
        self.wake_up_others(task_processor);
    }

    /// Reads its submit instant.
    pub(super) fn submit_instant(&self) -> Option<NonZeroU32> {
        NonZeroU32::new(self.submit_instant.load(Acquire))
    }

    /// Creates a new [`Anchor`].
    fn new(
        transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
        creation_instant: Option<NonZeroU32>,
    ) -> Anchor<S> {
        Anchor {
            transaction_anchor,
            creation_instant,
            submit_instant: AtomicU32::new(0),
            rolled_back: AtomicBool::new(false),
            wake_up_others: AtomicBool::new(false),
            next: ebr::AtomicArc::null(),
        }
    }

    fn wake_up_others(&self, task_processor: &TaskProcessor) {
        if self.wake_up_others.load(Acquire) {
            // The result can be ignored since messages pending in the queue mean that the access
            // controller will be scanned in the future.
            task_processor.send_task(Task::ScanAccessController);
        }
    }
}

impl AccessRequestResult {
    /// Gives full access to the inner data with the mutex acquired.
    ///
    /// It may block the thread; the only component that can be blocked is [`TaskProcessor`],
    /// therefore this must be called by [`TaskProcessor`].
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
        task_processor: &'d TaskProcessor,
        deadline: Instant,
        result_placeholder: Arc<AccessRequestResult>,
    ) -> AwaitResponse<'d> {
        let object_id = *entry.key();
        drop(entry);
        AwaitResponse {
            object_id,
            object_id_registered: false,
            task_processor,
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
            if self
                .task_processor
                .send_task(Task::MonitorObject(self.object_id))
            {
                self.get_mut().object_id_registered = true;
            }
            cx.waker().wake_by_ref();
        } else if !self
            .task_processor
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
            .task_processor
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
    use std::num::NonZeroU32;

    #[test]
    fn journal() {
        let storage = Database::default();
        let transaction = storage.transaction();
        let journal_1 = transaction.journal();
        assert_eq!(journal_1.submit().ok(), NonZeroU32::new(1));
        let journal_2 = transaction.journal();
        assert_eq!(journal_2.submit().ok(), NonZeroU32::new(2));
        let journal_3 = transaction.journal();
        let journal_4 = transaction.journal();
        assert_eq!(journal_4.submit().ok(), NonZeroU32::new(3));
        assert_eq!(journal_3.submit().ok(), NonZeroU32::new(4));
    }
}
