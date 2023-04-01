// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::transaction::Anchor as TransactionAnchor;
use super::{PersistenceLayer, Sequencer, Snapshot, Transaction};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use scc::ebr;

/// [`Journal`] keeps the change history.
#[derive(Debug)]
pub struct Journal<'s, 't, S: Sequencer, P: PersistenceLayer<S>> {
    transaction: &'t Transaction<'s, S, P>,
    anchor: ebr::Arc<Anchor<S>>,
}

impl<'s, 't, S: Sequencer, P: PersistenceLayer<S>> Journal<'s, 't, S, P> {
    /// Submits the [`Journal`], thereby advancing the logical clock of the corresponding
    /// [`Transaction`].
    ///
    /// It returns the updated transaction-local clock value.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{Database, Transaction};
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal = transaction.start();
    /// assert_eq!(journal.submit(), 1);
    /// ```
    #[must_use]
    pub fn submit(self) -> usize {
        self.transaction.record(&self.anchor)
    }

    /// Takes a snapshot including changes in the [Journal].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::Database;
    ///
    /// let database = Database::default();
    /// let transaction = database.transaction();
    /// let journal = transaction.start();
    /// let snapshot = journal.snapshot();
    /// ```
    #[must_use]
    pub fn snapshot<'r>(&'r self) -> Snapshot<'s, 't, 'r, S> {
        Snapshot::from_parts(
            self.transaction.sequencer(),
            Some((self.transaction.anchor_addr(), self.transaction.clock())),
            Some(self.anchor.as_ref() as *const _ as usize),
        )
    }

    /// Creates a new [Journal].
    pub(super) fn new(
        transaction: &'t Transaction<'s, S, P>,
        transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
    ) -> Journal<'s, 't, S, P> {
        Journal {
            transaction,
            anchor: ebr::Arc::new(Anchor::new(transaction_anchor, transaction.clock())),
        }
    }
}

impl<'s, 't, S: Sequencer, P: PersistenceLayer<S>> Drop for Journal<'s, 't, S, P> {
    fn drop(&mut self) {
        // Send `anchor` to the garbage collector.
    }
}

/// [`Anchor`] is a piece of data that outlives its associated [`Journal`].
#[derive(Debug)]
pub struct Anchor<S: Sequencer> {
    #[allow(unused)]
    transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
    #[allow(unused)]
    wait_queue: (Mutex<(bool, usize)>, Condvar),
    #[allow(unused)]
    creation_clock: usize,
    submit_clock: AtomicUsize,
    next: ebr::AtomicArc<Anchor<S>>,
}

impl<S: Sequencer> Anchor<S> {
    /// Creates a new [`Anchor`].
    pub(super) fn new(
        transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
        creation_clock: usize,
    ) -> Anchor<S> {
        Anchor {
            transaction_anchor,
            wait_queue: (Mutex::new((false, 0)), Condvar::new()),
            creation_clock,
            submit_clock: AtomicUsize::new(usize::MAX),
            next: ebr::AtomicArc::null(),
        }
    }

    /// Returns a reference to the `next` field.
    pub(super) fn next(&self) -> &ebr::AtomicArc<Anchor<S>> {
        &self.next
    }

    /// Reads its submit clock.
    pub(super) fn submit_clock(&self) -> usize {
        self.submit_clock.load(Relaxed)
    }

    /// Assigns the transaction local clock when it is submitted.
    #[allow(clippy::unused_self)]
    pub(super) fn assign_submit_clock(&self, clock: usize) {
        self.submit_clock.store(clock, Release);
    }

    /// Waits for the final state of the [`Journal`] to be determined.
    #[allow(unused)]
    pub(super) fn wait<R, F: FnOnce(S::Clock) -> R>(
        &self,
        f: F,
        timeout: Option<Duration>,
    ) -> Option<R> {
        if let Ok(mut wait_queue) = self.wait_queue.0.lock() {
            if wait_queue.0 {
                return Some(f(self.transaction_anchor.commit_snapshot_clock()));
            }

            // Starts waiting by incrementing the counter.
            wait_queue.1 += 1;
            let (wait_queue_after_wait, timed_out) = if let Some(timeout) = timeout {
                if let Ok((locker, wait_result)) =
                    self.wait_queue.1.wait_timeout(wait_queue, timeout)
                {
                    (Some(locker), wait_result.timed_out())
                } else {
                    (None, false)
                }
            } else if let Ok(locker) = self.wait_queue.1.wait(wait_queue) {
                (Some(locker), false)
            } else {
                (None, false)
            };

            if let Some(mut wait_queue) = wait_queue_after_wait {
                wait_queue.1 -= 1;
                if !timed_out {
                    // Before waking up the next waiting thread, calls the supplied closure
                    // with the mutex acquired.
                    //
                    // For instance, if the version is owned by the transaction, ownership can
                    // be transferred.
                    let result = f(self.transaction_anchor.commit_snapshot_clock());

                    // Once the thread wakes up, it is mandated to wake the next thread up.
                    if wait_queue.1 > 0 {
                        self.wait_queue.1.notify_one();
                    }
                    return Some(result);
                }
            } else {
                // It does not expect a locking failure.
                unreachable!();
            }
        }
        None
    }

    /// The transaction record has either been committed or rolled back.
    #[allow(dead_code)]
    fn end(&self) {
        if let Ok(mut wait_queue) = self.wait_queue.0.lock() {
            if !wait_queue.0 {
                // Setting the flag `true` has an immediate effect on all the versioned
                // database objects owned by the `RecordData`.
                //
                // It allows all the other transaction to have a chance to take ownership of
                // the versioned objects.
                wait_queue.0 = true;
                self.wait_queue.1.notify_one();
            }
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
        let journal = transaction.start();
        assert_eq!(journal.submit(), 1);
    }
}
