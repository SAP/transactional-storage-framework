// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor as JournalAnchor;
use super::snapshot::TransactionSnapshot;
use super::{AwaitIO, Database, Error, Journal, PersistenceLayer, Sequencer, Snapshot};
use scc::ebr;
use scc::Bag;
use std::future::Future;
use std::pin::Pin;
use std::ptr::addr_of;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::task::Waker;
use std::task::{Context, Poll};

/// [`Transaction`] is the atomic unit of work in a [`Database`].
///
/// A single strand of [`Journal`] constitutes a [`Transaction`], and an on-going transaction can
/// be rewound to a certain instant by rolling back submitted [`Journal`] instances in reverse
/// order.
#[derive(Debug)]
pub struct Transaction<'d, S: Sequencer, P: PersistenceLayer<S>> {
    /// The transaction refers to the corresponding [`Database`] to persist pending changes at
    /// commit.
    database: &'d Database<S, P>,

    /// The changes made by the transaction.
    ///
    /// [`Transaction`] assigns each submitted [`Journal`] a logical time point value in increasing
    /// order.
    journal_strand: ebr::AtomicArc<JournalAnchor<S>>,

    /// The identifier of the [`Transaction`] as part of a distributed transaction.
    ///
    /// It is `None` if the transaction is not part of a distributed transaction.
    xid: Option<Vec<u8>>,

    /// A piece of data that is shared between [`Journal`] and [`Transaction`].
    ///
    /// It outlives the [`Transaction`], and it is dropped when no database objects refer to it.
    anchor: ebr::Arc<Anchor<S>>,
}

/// The type of transaction identifiers.
///
/// The identifier of a transaction is only valid during the lifetime of the transaction. The same
/// identifier can be used by an unrelated transaction afterwards.
pub type ID = u64;

/// Possible [`Transaction`] states.
#[derive(Clone, Copy, Eq, Debug, Ord, PartialEq, PartialOrd)]
pub enum State {
    /// The transaction is active.
    Active,

    /// The transaction is being committed.
    Committing,

    /// The transaction is committed.
    Committed,

    /// The transaction is being rolled back.
    RollingBack,

    /// The transaction is rolled back.
    RolledBack,
}

/// [`Committable`] gives one last chance of rolling back the transaction.
///
/// The transaction is bound to be rolled back if no actions are taken before dropping the
/// [`Committable`] instance. On the other hands, the transaction stays uncommitted until the
/// [`Committable`] instance is dropped or awaited.
pub struct Committable<'d, S: Sequencer, P: PersistenceLayer<S>> {
    /// The corresponding transaction.
    transaction: Option<Transaction<'d, S, P>>,

    /// [`AwaitIO`] for its own commit log record.
    commit_log_io: Option<(AwaitIO<'d, S, P>, S::Instant)>,
}

/// `0` as a transaction logical time point value represents an unfinished job.
pub const UNFINISHED_TRANSACTION_INSTANT: u32 = 0;

/// `usize::MAX` as a transaction logical time point is regarded as an unreachable instant for
/// transactions, thus disallowing readers to see changes corresponding to the time point.
///
/// [`Transaction`] cannot generated a clock value that is equal to or greater than
/// [`UNREACHABLE_TRANSACTION_INSTANT`], and changes made at [`UNREACHABLE_TRANSACTION_INSTANT`]
/// can never be visible to any other jobs in the same transaction.
pub const UNREACHABLE_TRANSACTION_INSTANT: u32 = u32::MAX;

/// [Anchor] contains data that is required to outlive the [Transaction] instance.
#[derive(Debug)]
pub(super) struct Anchor<S: Sequencer> {
    /// The transaction state.
    ///
    /// An integer represents a transaction state.
    ///  * 0: active.
    ///  * 1: commit started.
    ///  * 2: committed.
    ///  * 3: abort started.
    ///  * 4: aborted.
    state: AtomicUsize,

    /// The instant when the commit has begun.
    prepare_instant: S::Instant,

    /// The instant when the commit is completed.
    commit_instant: S::Instant,

    /// An unordered bag of [`Waker`] for readers.
    waiting_readers: Bag<Waker, 4>,
}

impl<'d, S: Sequencer, P: PersistenceLayer<S>> Transaction<'d, S, P> {
    /// The transaction identifier.
    ///
    /// The identifier is unique in the process, however the same identifier can be used after the
    /// transaction is committed or rolled back by an unrelated database transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// assert_ne!(transaction.id(), 0);
    /// ```
    #[inline]
    pub fn id(&self) -> ID {
        self.anchor.as_ptr() as ID
    }

    /// Creates a new [`Journal`].
    ///
    /// A [`Journal`] keeps database changes until it is dropped. In order to make the changes
    /// permanent, the [`Journal`] has to be submitted to the [`Transaction`].
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal = transaction.journal();
    /// journal.submit();
    /// ```
    #[inline]
    pub fn journal<'t>(&'t self) -> Journal<'d, 't, S, P> {
        Journal::new(self, self.anchor.clone())
    }

    /// Captures the current state of the [`Transaction`] as a [`Snapshot`].
    ///
    /// If the number of submitted [`Journal`] instances is equal to or greater than
    /// `usize::MAX`, recent changes in the transaction will not be visible to the [`Snapshot`]
    /// since they cannot be expressed as a `usize` value.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let snapshot = transaction.snapshot();
    /// ```
    #[inline]
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::from_transaction(self.database, self.transaction_snapshot(self.now()))
    }

    /// Participates in a distributed transaction.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the transaction is currently participating in a distributed
    /// transaction, or persisting the supplied identifier failed.
    #[inline]
    pub async fn participate(&mut self, xid: &[u8]) -> Result<(), Error> {
        if let Some(own_xid) = self.xid.as_ref() {
            if own_xid == xid {
                Ok(())
            } else {
                Err(Error::UnexpectedState)
            }
        } else {
            let io_completion =
                self.database
                    .persistence_layer()
                    .participate(self.id(), xid, None)?;
            io_completion.await?;
            self.xid.replace(xid.into());
            Ok(())
        }
    }

    /// Gets the current local clock value of the [`Transaction`].
    ///
    /// The returned value amounts to the number of submitted [`Journal`] instances in the
    /// [`Transaction`] if the number is less than `usize::MAX`. This implies that, if more than
    /// `usize::MAX` [`Journal`] instances have been submitted to the transaction, recent changes
    /// will never be visible to any other jobs in the transaction, since this method cannot return
    /// any value that is equal to or greater than `usize::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Journal, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal = transaction.journal();
    /// let instant = journal.submit();
    ///
    /// assert_eq!(transaction.now(), 1);
    /// assert_eq!(instant, Ok(1));
    /// ```
    #[inline]
    pub fn now(&self) -> u32 {
        self.journal_strand
            .load(Acquire, &ebr::Barrier::new())
            .as_ref()
            .map_or(UNFINISHED_TRANSACTION_INSTANT, |j| {
                let submit_instant = j.submit_instant().min(UNREACHABLE_TRANSACTION_INSTANT - 1);
                debug_assert_ne!(submit_instant, UNFINISHED_TRANSACTION_INSTANT);
                submit_instant
            })
    }

    /// Rewinds the [`Transaction`] to the given point of time.
    ///
    /// All the changes made after the specified instant are rolled back and returns the updated
    /// clock value. It requires a mutable reference to the [`Transaction`], thus ensuring
    /// exclusivity.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the corresponding log record could not be constructed.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let mut transaction = database.transaction();
    /// assert_eq!(transaction.rewind(1), Ok(0));
    ///
    /// for _ in 0..3 {
    ///     let journal = transaction.journal();
    ///     journal.submit();
    /// }
    ///
    /// assert_eq!(transaction.now(), 3);
    /// assert_eq!(transaction.rewind(4), Ok(3));
    /// assert_eq!(transaction.rewind(3), Ok(3));
    /// assert_eq!(transaction.rewind(1), Ok(1));
    /// ```
    #[inline]
    pub fn rewind(&mut self, instant: u32) -> Result<u32, Error> {
        let mut current = self.journal_strand.swap((None, ebr::Tag::None), Acquire).0;
        while let Some(record) = current {
            if record.submit_instant() <= instant {
                current = Some(record);
                break;
            }
            record.rollback(self.database.task_processor());
            current = record.set_next(None, Relaxed);
        }
        let new_instant = current.as_ref().map_or(0, |r| r.submit_instant());
        self.journal_strand.swap((current, ebr::Tag::None), Relaxed);

        let io_completion =
            self.database
                .persistence_layer()
                .rewind(self.id(), new_instant, None)?;

        // Do not wait for an IO completion.
        io_completion.forget();

        Ok(new_instant)
    }

    /// Prepares the [Transaction] for commit.
    ///
    /// It returns a [`Committable`], giving one last chance to roll back the prepared
    /// transaction.
    ///
    /// # Errors
    ///
    /// If the transaction could not be prepared for commit, an [`Error`] is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let mut transaction = database.transaction();
    /// async {
    ///     if let Ok(indoubt_transaction) = transaction.prepare().await {
    ///         assert!(indoubt_transaction.await.is_ok());
    ///     }
    /// };
    /// ```
    #[inline]
    pub async fn prepare(self) -> Result<Committable<'d, S, P>, Error> {
        debug_assert_eq!(self.anchor.state.load(Relaxed), State::Active.into());

        let prepare_instant = self.sequencer().now(Relaxed);

        // Safety: it is the sole writer of its own `anchor`.
        unsafe {
            let anchor_mut_ref = &mut *(addr_of!(*self.anchor) as *mut Anchor<S>);
            anchor_mut_ref.prepare_instant = prepare_instant;
            anchor_mut_ref
                .state
                .store(State::Committing.into(), Release);
        }

        let io_completion =
            self.database
                .persistence_layer()
                .prepare(self.id(), prepare_instant, None)?;

        if self.xid.is_some() {
            io_completion.await?;
        }

        Ok(Committable {
            transaction: Some(self),
            commit_log_io: None,
        })
    }

    /// Commits the [Transaction].
    ///
    /// # Errors
    ///
    /// If the transaction cannot be committed, an [`Error`] is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let mut transaction = database.transaction();
    /// async {
    ///     assert!(transaction.commit().await.is_ok());
    /// };
    /// ```
    #[inline]
    pub async fn commit(self) -> Result<S::Instant, Error> {
        let indoubt_transaction = self.prepare().await?;
        indoubt_transaction.await
    }

    /// Rolls back the changes made by the [Transaction].
    ///
    /// # Panics
    ///
    /// Any failure when rolling back the transaction, e.g., memory allocation failure or an IO
    /// error, will lead to a panic.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let mut transaction = database.transaction();
    /// transaction.rollback();
    /// ```
    #[inline]
    pub fn rollback(mut self) {
        self.rollback_internal();
        drop(self);
    }

    /// Creates a new [`Transaction`].
    pub(crate) fn new(database: &'d Database<S, P>) -> Transaction<'d, S, P> {
        Transaction {
            database,
            journal_strand: ebr::AtomicArc::null(),
            xid: None,
            anchor: ebr::Arc::new(Anchor::new()),
        }
    }

    /// Returns a reference to its associated [`Sequencer`].
    pub(super) fn sequencer(&self) -> &'d S {
        self.database.sequencer()
    }

    /// Returns a reference to the corresponding [`Database`].
    pub(super) fn database(&self) -> &'d Database<S, P> {
        self.database
    }

    /// Takes [`Anchor`].
    pub(super) fn record(&self, record: &ebr::Arc<JournalAnchor<S>>) -> u32 {
        let barrier = ebr::Barrier::new();
        let mut current = self.journal_strand.load(Relaxed, &barrier);
        loop {
            record.set_next(current.get_arc(), Relaxed);
            match self.journal_strand.compare_exchange(
                current,
                (Some(record.clone()), ebr::Tag::None),
                Release,
                Relaxed,
                &barrier,
            ) {
                Ok(_) => return record.submit_instant(),
                Err((_, actual)) => current = actual,
            }
        }
    }

    /// Returns the memory address of its [`Anchor`].
    pub(super) fn transaction_snapshot(&self, instant: u32) -> TransactionSnapshot {
        debug_assert!(instant <= self.now());
        TransactionSnapshot::new(self.id(), instant)
    }

    /// Generates a commit log record.
    fn generate_commit_log_record(&mut self) -> Result<(AwaitIO<'d, S, P>, S::Instant), Error> {
        let commit_instant = self.sequencer().advance(Release);
        let io_completion =
            self.database
                .persistence_layer()
                .commit(self.id(), commit_instant, None)?;
        Ok((io_completion, commit_instant))
    }

    /// Post-processes its transaction commit.
    ///
    /// Only [`Committable`] is allowed to call this function.
    fn post_commit(&mut self, commit_instant: S::Instant) {
        debug_assert_ne!(commit_instant, S::Instant::default());
        debug_assert_eq!(self.anchor.state.load(Relaxed), State::Committing.into());

        // Safety: it is the sole writer of its own `anchor`.
        let anchor_mut_ref = unsafe { &mut *(addr_of!(*self.anchor) as *mut Anchor<S>) };
        anchor_mut_ref.commit_instant = commit_instant;
        anchor_mut_ref.state.store(State::Committed.into(), Release);
        self.anchor.wake_up();
        debug_assert_eq!(self.anchor.state.load(Relaxed), 2);

        let mut current = self.journal_strand.swap((None, ebr::Tag::None), Acquire).0;
        while let Some(record) = current {
            record.commit(self.database.task_processor());
            current = record.set_next(None, Relaxed);
        }
    }

    /// Rolls back all the changes.
    fn rollback_internal(&mut self) {
        debug_assert_ne!(self.anchor.state.load(Relaxed), State::Committed.into());
        debug_assert_ne!(self.anchor.state.load(Relaxed), State::RollingBack.into());
        debug_assert_ne!(self.anchor.state.load(Relaxed), State::RolledBack.into());

        self.anchor.state.store(State::RollingBack.into(), Release);
        self.anchor.wake_up();

        let result = self.rewind(0).unwrap();
        debug_assert_eq!(result, 0);

        self.anchor.state.store(State::RolledBack.into(), Release);
    }
}

impl<'d, S: Sequencer, P: PersistenceLayer<S>> Drop for Transaction<'d, S, P> {
    #[inline]
    fn drop(&mut self) {
        let state = self.anchor.state.load(Relaxed);
        if state == State::Active.into() || state == State::Committing.into() {
            self.rollback_internal();
        }
    }
}

impl<'d, S: Sequencer, P: PersistenceLayer<S>> Future for Committable<'d, S, P> {
    type Output = Result<S::Instant, Error>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(mut transaction) = self.transaction.take() {
            if let Some((mut io_completion, commit_instant)) = self.commit_log_io.take() {
                match Pin::new(&mut io_completion).poll(cx) {
                    Poll::Ready(Ok(_)) => {
                        // All done, returning the commit instant after post-processing.
                        transaction.post_commit(commit_instant);
                        return Poll::Ready(Ok(commit_instant));
                    }
                    Poll::Ready(Err(error)) => {
                        // Something bad happened during persisting the log record.
                        return Poll::Ready(Err(error));
                    }
                    Poll::Pending => {
                        // Need to wait for IO completion.
                        self.transaction.replace(transaction);
                        self.commit_log_io.replace((io_completion, commit_instant));
                        return Poll::Pending;
                    }
                }
            }
            match transaction.generate_commit_log_record() {
                Ok(commit_log_io) => {
                    self.transaction.replace(transaction);
                    self.commit_log_io.replace(commit_log_io);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Err(error) => {
                    // Failed to create a log record.
                    return Poll::Ready(Err(error));
                }
            };
        }

        // Already awaited.
        Poll::Ready(Err(Error::UnexpectedState))
    }
}

impl From<State> for usize {
    #[inline]
    fn from(v: State) -> usize {
        match v {
            State::Active => 0,
            State::Committing => 1,
            State::Committed => 2,
            State::RollingBack => 3,
            State::RolledBack => 4,
        }
    }
}

impl<S: Sequencer> Anchor<S> {
    fn new() -> Anchor<S> {
        Anchor {
            state: AtomicUsize::new(0),
            prepare_instant: S::Instant::default(),
            commit_instant: S::Instant::default(),
            waiting_readers: Bag::new(),
        }
    }

    /// Returns the instant when the transaction was being prepared for commit.
    pub(super) fn prepare_instant(&self) -> Option<S::Instant> {
        let state = self.state.load(Acquire);
        if state == State::Committing.into()
            || state == State::Committed.into()
            || state == State::RollingBack.into()
            || state == State::RolledBack.into()
        {
            Some(self.prepare_instant)
        } else {
            None
        }
    }

    /// Returns the instant when the transaction has been committed or rolled back.
    pub(super) fn eot_instant(&self) -> Option<S::Instant> {
        let state = self.state.load(Acquire);
        if state == State::Committed.into() || state == State::RolledBack.into() {
            Some(self.commit_instant)
        } else {
            None
        }
    }

    /// Waiting for the transaction to be committed or rolled back.
    pub(super) fn wait_eot(&self, waker: Waker) -> Option<S::Instant> {
        self.waiting_readers.push(waker);
        if let Some(commit_instant) = self.eot_instant() {
            self.wake_up();
            Some(commit_instant)
        } else {
            None
        }
    }

    /// Wakes up every [`Waker`] stored in the [`Anchor`].
    fn wake_up(&self) {
        while let Some(waker) = self.waiting_readers.pop() {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    /// Helper that prolongs the lifetime of a [`Transaction`] to send it to a spawned task.
    fn prolong_transaction<S: Sequencer, P: PersistenceLayer<S>>(
        t: Transaction<S, P>,
    ) -> Transaction<'static, S, P> {
        // Safety: test-only.
        unsafe { std::mem::transmute(t) }
    }

    #[tokio::test]
    async fn transaction() {
        let database = Database::default();
        let transaction = database.transaction();
        assert!(transaction.commit().await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn rewind() {
        let num_tasks = 16_u32;
        let barrier = Arc::new(Barrier::new(num_tasks as usize));
        let database = Arc::new(Database::default());
        let mut transaction = Arc::new(prolong_transaction(database.transaction()));
        let mut task_handles = Vec::with_capacity(num_tasks as usize);
        for _ in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let transaction_clone = transaction.clone();
            task_handles.push(tokio::spawn(async move {
                barrier_clone.wait().await;
                for i in 0..num_tasks {
                    assert!(transaction_clone.now() >= i);
                    let journal = transaction_clone.journal();
                    assert!(journal.submit().unwrap() > i);
                }
            }));
        }
        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }

        let num_submitted_journals = transaction.now();
        for i in 0..num_submitted_journals {
            assert_eq!(
                Arc::get_mut(&mut transaction)
                    .unwrap()
                    .rewind(num_submitted_journals - i - 1),
                Ok(num_submitted_journals - i - 1)
            );
            assert_eq!(
                Arc::get_mut(&mut transaction)
                    .unwrap()
                    .rewind(UNREACHABLE_TRANSACTION_INSTANT),
                Ok(num_submitted_journals - i - 1)
            );
        }
        assert_eq!(transaction.now(), UNFINISHED_TRANSACTION_INSTANT);
    }
}
