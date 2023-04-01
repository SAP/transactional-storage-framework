// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor as JournalAnchor;
use super::{Database, Error, Journal, PersistenceLayer, Sequencer, Snapshot};
use scc::ebr;
use std::future::Future;
use std::pin::Pin;
use std::ptr::addr_of;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::task::{Context, Poll};

/// [`Transaction`] is the atomic unit of work for all types of database operations.
///
/// A single strand of [`Journal`] constitutes a [`Transaction`], and an on-going transaction can
/// be rewound to a certain point of time by reverting submitted [`Journal`] instances.
#[derive(Debug)]
pub struct Transaction<'s, S: Sequencer, P: PersistenceLayer<S>> {
    /// The transaction refers to the corresponding [`Database`] to persist pending changes at
    /// commit.
    database: &'s Database<S, P>,

    /// The changes made by the transaction.
    journal_strand: ebr::AtomicArc<JournalAnchor<S>>,

    /// A piece of data that is shared among [`Journal`] instances in the [`Transaction`].
    ///
    /// It outlives the [`Transaction`].
    anchor: ebr::Arc<Anchor<S>>,

    /// The transaction-local clock generator.
    ///
    /// The clock value is updated whenever a [`Journal`] is submitted.
    clock: AtomicUsize,
}

impl<'s, S: Sequencer, P: PersistenceLayer<S>> Transaction<'s, S, P> {
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
    pub fn start<'t>(&'t self) -> Journal<'s, 't, S, P> {
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
        Snapshot::from_parts(
            self.database.sequencer(),
            Some((self.anchor_addr(), self.clock())),
            None,
        )
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
        let mut current = self.journal_strand.swap((None, ebr::Tag::None), Acquire).0;
        while let Some(record) = current {
            if record.submit_clock() <= clock {
                current = Some(record);
                break;
            }
            current = record.next().swap((None, ebr::Tag::None), Acquire).0;
        }
        self.journal_strand.swap((current, ebr::Tag::None), Relaxed);
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
    pub async fn prepare(self) -> Result<Committable<'s, S, P>, Error> {
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
    pub(crate) fn new(database: &'s Database<S, P>) -> Transaction<'s, S, P> {
        Transaction {
            database,
            journal_strand: ebr::AtomicArc::null(),
            anchor: ebr::Arc::new(Anchor::new()),
            clock: AtomicUsize::new(0),
        }
    }

    /// Returns a reference to its associated [Sequencer].
    pub(super) fn sequencer(&self) -> &'s S {
        self.database.sequencer()
    }

    /// Takes [`Anchor`].
    pub(super) fn record(&self, record: &ebr::Arc<JournalAnchor<S>>) -> usize {
        let barrier = ebr::Barrier::new();
        let mut current = self.journal_strand.load(Relaxed, &barrier);
        loop {
            record
                .next()
                .swap((current.get_arc(), ebr::Tag::None), Relaxed);
            match self.journal_strand.compare_exchange(
                current,
                (Some(record.clone()), ebr::Tag::None),
                Release,
                Relaxed,
                &barrier,
            ) {
                Ok(_) => {
                    // Transaction-local clock is updated after contents are submitted.
                    let current_clock = self.clock.fetch_add(1, Release) + 1;
                    record.assign_submit_clock(current_clock);
                    return current_clock;
                }
                Err((_, actual)) => current = actual,
            }
        }
    }

    /// Returns the memory address of its [`Anchor`].
    pub(super) fn anchor_addr(&self) -> usize {
        self.anchor.as_ref() as *const _ as usize
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

impl<'s, S: Sequencer, P: PersistenceLayer<S>> Drop for Transaction<'s, S, P> {
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
pub struct Committable<'s, S: Sequencer, P: PersistenceLayer<S>> {
    transaction: Option<Transaction<'s, S, P>>,
}

impl<'s, S: Sequencer, P: PersistenceLayer<S>> Future for Committable<'s, S, P> {
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
    #[allow(unused)]
    pub(super) fn commit_start_clock(&self) -> S::Clock {
        if self.state.load(Acquire) == State::Active.into() {
            S::Clock::default()
        } else {
            self.prepare_clock
        }
    }

    /// Returns the final commit clock value of the transaction.
    #[allow(unused)]
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

    #[tokio::test]
    async fn transaction() {
        let database = Database::default();
        let transaction = database.transaction();
        assert!(transaction.commit().await.is_ok());
    }
}
