// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Annals;
use super::{Database, Error, Journal, Sequencer, Snapshot};
use scc::ebr;
use std::future::Future;
use std::pin::Pin;
use std::ptr::addr_of;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use std::task::{Context, Poll};

/// [`Transaction`] is the atomic unit of work for all types of database operations.
///
/// A single strand of [`Journal`] constitutes a [`Transaction`], and an on-going transaction can
/// be rewound to a certain point of time by reverting submitted [`Journal`] instances.
#[derive(Debug)]
pub struct Transaction<'s, S: Sequencer> {
    /// The transaction refers to the corresponding [`Database`] to persist pending changes at
    /// commit.
    database: &'s Database<S>,

    /// The changes made by the transaction.
    annals: AtomicPtr<Annals<S>>,

    /// A piece of data that is shared among [`Journal`] instances in the [`Transaction`].
    ///
    /// It outlives the [`Transaction`].
    anchor: ebr::Arc<Anchor<S>>,

    /// The transaction-local clock generator.
    ///
    /// The clock value is updated whenever a [`Journal`] is submitted.
    clock: AtomicUsize,
}

impl<'s, S: Sequencer> Transaction<'s, S> {
    /// Starts a new [`Journal`].
    ///
    /// A [`Journal`] keeps database changes until it is dropped. In order to make the changes
    /// permanent, the [`Journal`] has to be submitted to the [`Transaction`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal = transaction.start();
    /// journal.submit();
    /// ```
    #[inline]
    pub fn start<'t>(&'t self) -> Journal<'s, 't, S> {
        Journal::new(self, self.anchor.clone())
    }

    /// Captures the current state of the [`Database`] and the [`Transaction`] as a [`Snapshot`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let snapshot = transaction.snapshot();
    /// ```
    #[inline]
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::from_parts(self.database.sequencer(), Some(self), None)
    }

    /// Gets the current local clock value of the [`Transaction`].
    ///
    /// The returned value amounts to the number of submitted [`Journal`] instances in the
    /// [`Transaction`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Journal, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal = transaction.start();
    /// let clock = journal.submit();
    ///
    /// assert_eq!(transaction.clock(), 1);
    /// assert_eq!(clock, 1);
    /// ```
    #[inline]
    pub fn clock(&self) -> usize {
        self.clock.load(Acquire)
    }

    /// Rewinds the [`Transaction`] to the given point of time.
    ///
    /// All the changes made between the latest transaction clock and the given one are rolled
    /// back. It requires a mutable reference, thus ensuring exclusivity.
    ///
    /// # Errors
    ///
    /// If an invalid clock value is supplied, an error is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let mut transaction = database.transaction();
    /// let result = transaction.rewind(1);
    /// assert!(result.is_err());
    ///
    /// for _ in 0..3 {
    ///     let journal = transaction.start();
    ///     journal.submit();
    /// }
    ///
    /// let result = transaction.rewind(1);
    /// assert!(result.is_ok());
    /// assert_eq!(transaction.clock(), 1);
    /// ```
    #[inline]
    pub fn rewind(&mut self, clock: usize) -> Result<usize, Error> {
        let current_clock = self.clock.load(Acquire);
        if current_clock <= clock {
            return Err(Error::WrongParameter);
        }
        let mut current = self.annals.load(Acquire);
        for _ in clock..current_clock {
            let current_boxed = unsafe { Box::from_raw(current) };
            current = current_boxed.next.load(Relaxed);
        }
        self.annals.store(current, Relaxed);
        self.clock.store(clock, Release);
        Ok(clock)
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
    /// use tss::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let mut transaction = database.transaction();
    /// async {
    ///     if let Ok(indoubt_transaction) = transaction.prepare().await {
    ///         assert!(indoubt_transaction.await.is_ok());
    ///     }
    /// };
    /// ```
    #[allow(clippy::unused_async)]
    #[inline]
    pub async fn prepare(self) -> Result<Committable<'s, S>, Error> {
        debug_assert_eq!(self.anchor.state.load(Relaxed), State::Active.into());

        // Assigns a new logical clock.
        let anchor_mut_ref = unsafe { &mut *(addr_of!(*self.anchor) as *mut Anchor<S>) };
        anchor_mut_ref.prepare_clock = self.sequencer().get(Relaxed);
        anchor_mut_ref.state.store(1, Release);
        Ok(Committable {
            transaction: Some(self),
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
    /// use tss::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let mut transaction = database.transaction();
    /// async {
    ///     assert!(transaction.commit().await.is_ok());
    /// };
    /// ```
    #[inline]
    pub async fn commit(self) -> Result<S::Clock, Error> {
        let indoubt_transaction = self.prepare().await?;
        indoubt_transaction.await
    }

    /// Rolls back the changes made by the [Transaction].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let mut transaction = database.transaction();
    /// async {
    ///     transaction.rollback().await;
    /// };
    /// ```
    #[allow(clippy::unused_async)]
    #[inline]
    pub async fn rollback(mut self) {
        self.anchor.state.store(State::RollingBack.into(), Release);
        let _result = self.rewind(0);
        self.anchor.state.store(State::RolledBack.into(), Release);
        drop(self);
    }

    /// Creates a new [`Transaction`].
    pub(crate) fn new(database: &'s Database<S>) -> Transaction<'s, S> {
        Transaction {
            database,
            annals: AtomicPtr::default(),
            anchor: ebr::Arc::new(Anchor::new()),
            clock: AtomicUsize::new(0),
        }
    }

    /// Returns a reference to its associated [Sequencer].
    pub(super) fn sequencer(&self) -> &'s S {
        self.database.sequencer()
    }

    /// Takes [Annals], and records them.
    pub(super) fn record(&self, record: Box<Annals<S>>) -> usize {
        let mut current = self.annals.load(Relaxed);
        let desired = Box::into_raw(record);
        loop {
            unsafe {
                (*desired).next.store(current, Relaxed);
            }
            match self
                .annals
                .compare_exchange(current, desired, Release, Relaxed)
            {
                Ok(_) => {
                    // Transaction-local clock is updated after contents are submitted.
                    let current_clock = self.clock.fetch_add(1, Release) + 1;
                    unsafe {
                        (*desired).assign_clock(current_clock);
                    }
                    return current_clock;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Returns a reference to its [Anchor].
    pub(super) fn anchor_ptr<'b>(&self, barrier: &'b ebr::Barrier) -> ebr::Ptr<'b, Anchor<S>> {
        self.anchor.ptr(barrier)
    }

    /// Post-processes its transaction commit.
    ///
    /// Only a `Committable` instance is allowed to call this function.
    /// Once the transaction is post-processed, the transaction cannot be rolled back.
    fn post_process(self) -> S::Clock {
        debug_assert_eq!(self.anchor.state.load(Relaxed), State::Committing.into());

        let anchor_mut_ref = unsafe { &mut *(addr_of!(*self.anchor) as *mut Anchor<S>) };
        let commit_clock = self.sequencer().advance(Release);
        anchor_mut_ref.commit_clock = commit_clock;
        anchor_mut_ref.state.store(State::Committed.into(), Release);

        debug_assert_eq!(self.anchor.state.load(Relaxed), 2);
        drop(self);

        commit_clock
    }
}

impl<'s, S: Sequencer> Drop for Transaction<'s, S> {
    #[inline]
    fn drop(&mut self) {
        let _result = self.rewind(0);
    }
}

/// [`Committable`] gives one last chance of rolling back the transaction.
///
/// The transaction is bound to be rolled back if no actions are taken before dropping the
/// [`Committable`] instance. On the other hands, the transaction stays uncommitted until the
/// [`Committable`] instance is dropped or awaited.
pub struct Committable<'s, S: Sequencer> {
    transaction: Option<Transaction<'s, S>>,
}

impl<'s, S: Sequencer> Future for Committable<'s, S> {
    type Output = Result<S::Clock, Error>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(commit_clock) = self.transaction.take().map(Transaction::post_process) {
            Poll::Ready(Ok(commit_clock))
        } else {
            Poll::Pending
        }
    }
}

/// [`Transaction`] state.
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

    /// The clock value when a commit is issued.
    prepare_clock: S::Clock,

    /// The clock value when the commit is completed.
    commit_clock: S::Clock,
}

impl<S: Sequencer> Anchor<S> {
    fn new() -> Anchor<S> {
        Anchor {
            state: AtomicUsize::new(0),
            prepare_clock: S::Clock::default(),
            commit_clock: S::Clock::default(),
        }
    }

    /// Returns the clock value when the transaction starts to commit.
    pub(super) fn commit_start_clock(&self) -> S::Clock {
        if self.state.load(Acquire) == State::Active.into() {
            S::Clock::default()
        } else {
            self.prepare_clock
        }
    }

    /// Returns the final commit clock value of the transaction.
    pub(super) fn commit_snapshot_clock(&self) -> S::Clock {
        if self.state.load(Acquire) == State::Committed.into() {
            self.commit_clock
        } else {
            S::Clock::default()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::sequencer::AtomicCounter;
    use crate::version::RecordVersion;
    use crate::Version;

    use std::sync::{Arc, Barrier, Once};
    use std::thread;
    use std::time::Duration;

    #[tokio::test]
    async fn visibility() {
        static mut DATABASE: Option<Database<AtomicCounter>> = None;
        static INIT: Once = Once::new();

        INIT.call_once(|| unsafe {
            DATABASE.replace(Database::default());
        });

        let database_ref = unsafe { DATABASE.as_ref().unwrap() };
        let versioned_object: Arc<RecordVersion<usize>> = Arc::new(RecordVersion::default());
        let transaction = Arc::new(database_ref.transaction());
        let barrier = Arc::new(Barrier::new(2));

        let versioned_object_clone = versioned_object.clone();
        let transaction_clone = transaction.clone();
        let barrier_clone = barrier.clone();
        let thread_handle = thread::spawn(move || {
            barrier_clone.wait();

            // Step 1. Tries to acquire the lock acquired by an active transaction journal.
            let mut journal = transaction_clone.start();
            assert!(journal
                .create(&*versioned_object_clone, |_| Ok(None), None)
                .is_err());
            drop(journal);

            // Step 2. Tries to acquire the lock acquired by a submitted transaction journal.
            barrier_clone.wait();
            barrier_clone.wait();

            let mut journal = transaction_clone.start();
            assert!(journal
                .create(&*versioned_object_clone, |_| Ok(None), None)
                .is_ok());
            assert_eq!(journal.submit(), 2);
        });

        let mut journal = transaction.start();
        assert!(journal
            .create(&*versioned_object, |_| Ok(None), None)
            .is_ok());

        barrier.wait();
        barrier.wait();
        assert_eq!(journal.submit(), 1);
        barrier.wait();

        assert!(thread_handle.join().is_ok());

        if let Ok(transaction) = Arc::try_unwrap(transaction) {
            assert!(transaction.commit().await.is_ok());
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn rewind() {
        static mut DATABASE: Option<Database<AtomicCounter>> = None;
        static INIT: Once = Once::new();

        INIT.call_once(|| unsafe {
            DATABASE.replace(Database::default());
        });

        let database_ref = unsafe { DATABASE.as_ref().unwrap() };
        let versioned_object: Arc<RecordVersion<usize>> = Arc::new(RecordVersion::default());
        let mut transaction = database_ref.transaction();
        let barrier = Arc::new(Barrier::new(2));

        let versioned_object_clone = versioned_object.clone();
        let barrier_clone = barrier.clone();
        let thread_handle = thread::spawn(move || {
            let transaction = database_ref.transaction();
            // Step 1. Tries to acquire the lock acquired by an active transaction journal.
            for _ in 1..8 {
                barrier_clone.wait();
                let mut journal = transaction.start();
                assert!(journal
                    .create(
                        &*versioned_object_clone,
                        |_| Ok(None),
                        Some(Duration::from_millis(1))
                    )
                    .is_err());
                drop(journal);
                barrier_clone.wait();
            }

            // Step 2. Tries to acquire the lock after reverting some changes.
            barrier_clone.wait();
            let mut journal = transaction.start();
            assert!(journal
                .create(
                    &*versioned_object_clone,
                    |_| Ok(None),
                    Some(Duration::from_millis(1))
                )
                .is_err());
            drop(journal);
            barrier_clone.wait();

            // Step 3. Tries to acquire the lock after reverting all the changes.
            barrier_clone.wait();
            let mut journal = transaction.start();
            assert!(journal
                .create(&*versioned_object_clone, |_| Ok(None), None)
                .is_ok());
            assert_eq!(journal.submit(), 1);
        });

        // Step 1. Acquires the lock several times.
        let journal = transaction.start();
        assert_eq!(journal.submit(), 1);
        for i in 1..8 {
            let mut journal = transaction.start();
            assert!(journal
                .create(&*versioned_object, |_| Ok(None), None)
                .is_ok());
            assert_eq!(journal.submit(), i + 1);
            barrier.wait();
            barrier.wait();
        }

        // Step 2. Rewinds to half the transaction.
        assert!(transaction.rewind(4).is_ok());
        barrier.wait();
        barrier.wait();

        // Step 3. Rewinds all.
        assert!(transaction.rewind(6).is_err());
        assert!(transaction.rewind(0).is_ok());
        barrier.wait();

        assert!(thread_handle.join().is_ok());

        let mut journal = transaction.start();
        assert!(journal
            .create(
                &*versioned_object,
                |_| Ok(None),
                Some(Duration::from_millis(1))
            )
            .is_ok());
        assert_eq!(journal.submit(), 1);

        assert!(transaction.commit().await.is_ok());
    }

    #[tokio::test]
    async fn wait_queue() {
        let database: Arc<Database<AtomicCounter>> = Arc::new(Database::default());
        let versioned_object: Arc<RecordVersion<usize>> = Arc::new(RecordVersion::default());
        let num_threads = 16;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let mut thread_handles = Vec::new();
        for _ in 0..num_threads {
            let database_clone = database.clone();
            let versioned_object_clone = versioned_object.clone();
            let barrier_clone = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_clone.wait();
                let snapshot = database_clone.snapshot();
                assert!(!versioned_object_clone.predate(&snapshot, &ebr::Barrier::new()));
                barrier_clone.wait();
                barrier_clone.wait();
                let snapshot = database_clone.snapshot();
                assert!(versioned_object_clone.predate(&snapshot, &ebr::Barrier::new()));
            }));
        }
        barrier.wait();
        let transaction = database.transaction();
        let mut journal = transaction.start();
        let result = journal.create(&*versioned_object, |_| Ok(None), None);
        assert!(result.is_ok());
        assert_eq!(journal.submit(), 1);
        barrier.wait();
        assert!(transaction.commit().await.is_ok());
        barrier.wait();

        thread_handles
            .into_iter()
            .for_each(|t| assert!(t.join().is_ok()));

        let snapshot = database.snapshot();
        assert!(versioned_object.predate(&snapshot, &ebr::Barrier::new()));
    }

    #[tokio::test]
    async fn time_out() {
        let database: Arc<Database<AtomicCounter>> = Arc::new(Database::default());
        let versioned_object: Arc<RecordVersion<usize>> = Arc::new(RecordVersion::default());

        let transaction = database.transaction();
        let mut journal = transaction.start();
        assert!(journal
            .create(&*versioned_object, |_| Ok(None), None)
            .is_ok());

        let num_threads = 16;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let mut thread_handles = Vec::new();
        for _ in 0..num_threads {
            let database_clone = database.clone();
            let versioned_object_clone = versioned_object.clone();
            let barrier_clone = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_clone.wait();
                let transaction = database_clone.transaction();
                let mut journal = transaction.start();
                assert!(journal
                    .create(
                        &*versioned_object_clone,
                        |_| Ok(None),
                        Some(Duration::from_millis(100))
                    )
                    .is_err());

                barrier_clone.wait();
                barrier_clone.wait();

                let mut journal = transaction.start();
                assert!(journal
                    .create(
                        &*versioned_object_clone,
                        |_| Ok(None),
                        Some(Duration::from_millis(100))
                    )
                    .is_err());
            }));
        }

        barrier.wait();
        barrier.wait();

        assert_eq!(journal.submit(), 1);
        let database_clone = database.clone();
        let versioned_object_clone = versioned_object.clone();
        let thread = thread::spawn(move || {
            let transaction = database_clone.transaction();
            let mut journal = transaction.start();
            assert!(journal
                .create(
                    &*versioned_object_clone,
                    |_| Ok(None),
                    Some(Duration::from_secs(u64::MAX))
                )
                .is_ok());
        });

        barrier.wait();

        let mut journal = transaction.start();
        assert!(journal
            .create(&*versioned_object, |_| Ok(None), None)
            .is_ok());
        assert_eq!(journal.submit(), 2);

        thread_handles
            .into_iter()
            .for_each(|t| assert!(t.join().is_ok()));

        let mut journal = transaction.start();
        assert!(journal
            .create(&*versioned_object, |_| Ok(None), None)
            .is_ok());
        drop(journal);

        transaction.rollback().await;

        assert!(thread.join().is_ok());
    }
}
