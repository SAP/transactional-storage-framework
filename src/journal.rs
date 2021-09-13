// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::transaction::Anchor as TransactionAnchor;
use super::version::Locker;
use super::{Error, Log, Sequencer, Snapshot, Transaction, Version};

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use scc::ebr;
use scc::LinkedList;

/// [Journal] keeps the change history.
///
/// Locks and log records are accumulated in a [Journal].
pub struct Journal<'s, 't, S: Sequencer> {
    transaction: &'t Transaction<'s, S>,
    record: Option<ebr::Arc<Annals<S>>>,
}

impl<'s, 't, S: Sequencer> Journal<'s, 't, S> {
    /// Submits the [Journal], thereby advancing the logical clock of the corresponding
    /// [Transaction], making its changes possible to be committed to the database.
    ///
    /// It returns the updated transaction-local clock value.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Log, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let journal = transaction.start();
    /// assert_eq!(journal.submit(), 1);
    /// ```
    #[must_use]
    pub fn submit(mut self) -> usize {
        if let Some(record) = self.record.take() {
            self.transaction.record(record)
        } else {
            0
        }
    }

    /// Takes a snapshot including changes in the [Journal].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, RecordVersion, Storage, Version};
    ///
    /// let versioned_object: RecordVersion<usize> = RecordVersion::default();
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    ///
    /// let mut journal = transaction.start();
    /// assert!(journal.create(&versioned_object, |_| Ok(None), None).is_ok());
    /// let snapshot = journal.snapshot();
    /// assert!(versioned_object.predate(&snapshot, &scc::ebr::Barrier::new()));
    /// journal.submit();
    ///
    /// assert!(versioned_object.consolidate());
    /// ```
    #[must_use]
    pub fn snapshot<'r>(&'r self) -> Snapshot<'s, 't, 'r, S> {
        Snapshot::new(
            self.transaction.sequencer(),
            Some(self.transaction),
            Some(self),
        )
    }

    /// Creates a versioned database object.
    ///
    /// The acquired lock is never released until the [Journal] is dropped. If the lock is
    /// released without a valid clock value assigned to the [Journal], the version is either
    /// be properly initialized by another [Journal], or garbage-collected later.
    ///
    /// # Errors
    ///
    /// If the versioned database object cannot locked, or invisible to the transaction, an
    /// error is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, RecordVersion, Storage, Version};
    ///
    /// let versioned_object: RecordVersion<usize> = RecordVersion::default();
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    ///
    /// let mut journal = transaction.start();
    /// assert!(journal.create(&versioned_object, |_| Ok(None), None).is_ok());
    /// journal.submit();
    ///
    /// transaction.commit();
    ///
    /// let snapshot = storage.snapshot();
    /// assert!(versioned_object.predate(&snapshot, &scc::ebr::Barrier::new()));
    /// assert!(versioned_object.consolidate());
    /// ```
    pub fn create<V: Version<S>, F: FnOnce(&mut V::Data) -> Result<Option<Log>, Error>>(
        &mut self,
        version: &V,
        writer: F,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        if let Some(record_mut) = self.record.as_mut().map(|r| r.get_mut()).and_then(|r| r) {
            let barrier = ebr::Barrier::new();
            if let Some(mut locker) =
                version.create(record_mut.anchor_ptr(&barrier), timeout, &barrier)
            {
                let log = version.write(&mut locker, writer, &barrier)?;
                record_mut.locks.push(locker);
                if let Some(log) = log {
                    record_mut.logs.push(log);
                }
                return Ok(());
            }
        }

        // The versioned object is not ready for versioning.
        Err(Error::Fail)
    }

    /// Creates a new [Journal].
    pub(super) fn new(
        transaction: &'t Transaction<'s, S>,
        transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
    ) -> Journal<'s, 't, S> {
        Journal {
            transaction,
            record: Some(ebr::Arc::new(Annals::new(transaction, transaction_anchor))),
        }
    }
}

impl<'s, 't, S: Sequencer> Drop for Journal<'s, 't, S> {
    fn drop(&mut self) {
        // Implementing `Drop` is necessary for the compiler to enforce the `Transaction` to
        // outlive it.
        if let Some(mut record) = self.record.take() {
            if let Some(record_mut) = record.get_mut() {
                record_mut.rollback();
            }
            drop(record);
        }
    }
}

/// [Annals] consists of locks acquired and log records generated with the [Journal].
pub(super) struct Annals<S: Sequencer> {
    anchor: ebr::Arc<Anchor<S>>,
    locks: Vec<Locker<S>>,
    logs: Vec<Log>,
    next: ebr::AtomicArc<Self>,
}

impl<S: Sequencer> Annals<S> {
    /// Returns an [`ebr::Ptr`] of its [Anchor].
    pub(super) fn anchor_ptr<'b>(&self, barrier: &'b ebr::Barrier) -> ebr::Ptr<'b, Anchor<S>> {
        self.anchor.ptr(barrier)
    }

    /// Assigns a clock value to its [Anchor].
    pub(super) fn assign_clock(&self, clock: usize) {
        debug_assert_eq!(self.anchor.submit_clock.load(Relaxed), usize::MAX);
        self.anchor.submit_clock.store(clock, Relaxed);
    }

    /// Creates a new [Annals].
    fn new(
        transaction: &Transaction<S>,
        transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
    ) -> Annals<S> {
        Annals {
            anchor: ebr::Arc::new(Anchor::new(transaction_anchor, transaction.clock())),
            locks: Vec::new(),
            logs: Vec::new(),
            next: ebr::AtomicArc::null(),
        }
    }

    /// Rolls back changes synchronously.
    pub(super) fn rollback(&mut self) {
        self.logs.clear();
        self.locks.clear();
        self.anchor.end();
    }
}

impl<S: Sequencer> Drop for Annals<S> {
    fn drop(&mut self) {
        self.rollback();
    }
}

impl<S: Sequencer> LinkedList for Annals<S> {
    fn link_ref(&self) -> &ebr::AtomicArc<Self> {
        &self.next
    }
}

/// [Anchor] is a piece of data that outlives its associated [Journal] and [Annals].
///
/// [Cell](super::version::Cell) may point to it if the [Journal] owns the
/// [Version].
pub struct Anchor<S: Sequencer> {
    transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
    wait_queue: (Mutex<(bool, usize)>, Condvar),
    creation_clock: usize,
    submit_clock: AtomicUsize,
}

impl<S: Sequencer> Anchor<S> {
    /// Creates a new [Anchor].
    pub(super) fn new(
        transaction_anchor: ebr::Arc<TransactionAnchor<S>>,
        creation_clock: usize,
    ) -> Anchor<S> {
        Anchor {
            transaction_anchor,
            wait_queue: (Mutex::new((false, 0)), Condvar::new()),
            creation_clock,
            submit_clock: AtomicUsize::new(usize::MAX),
        }
    }

    /// Returns the clock of the transaction snapshot.
    pub(super) fn commit_snapshot(&self) -> S::Clock {
        self.transaction_anchor.commit_snapshot_clock()
    }

    /// Checks if the lock it has acquired can be transferred to the [Journal].
    ///
    /// It returns `(true, true)` if the given record has started after its data was submitted
    /// to the transaction.
    pub fn lockable(&self, journal_anchor: &Anchor<S>, barrier: &ebr::Barrier) -> (bool, bool) {
        if self.transaction_anchor.ptr(barrier) != journal_anchor.transaction_anchor.ptr(barrier) {
            // Different transactions.
            return (false, false);
        }
        // `self` predates the given one.
        (
            true,
            journal_anchor.submit_clock.load(Relaxed) <= self.creation_clock,
        )
    }

    /// Checks whether the [Journal] is visible to the given [Snapshot].
    pub fn visible(
        &self,
        snapshot: S::Clock,
        transaction: Option<(&Transaction<S>, usize)>,
        journal: Option<&Journal<S>>,
        barrier: &ebr::Barrier,
    ) -> bool {
        // The given anchor is itself.
        if let Some(journal_ref) = journal {
            if let Some(record_ref) = journal_ref.record.as_ref() {
                return record_ref.anchor_ptr(barrier).as_raw() == self as *const _;
            }
        }

        // The given transaction journal is ordered after `self`.
        let submit_clock = self.submit_clock.load(Relaxed);
        if let Some((transaction, transaction_clock)) = transaction {
            if self.transaction_anchor.ptr(barrier) == transaction.anchor_ptr(barrier)
                && submit_clock != usize::MAX
                && submit_clock <= transaction_clock
            {
                // It was submitted and it predates the given transaction local clock.
                return true;
            }
        }

        let anchor_ref = &*self.transaction_anchor;
        if anchor_ref.commit_start_clock() == S::Clock::default()
            || anchor_ref.commit_start_clock() >= snapshot
        {
            return false;
        }

        // The transaction will either be committed or rolled back soon.
        if anchor_ref.commit_snapshot_clock() == S::Clock::default() {
            self.wait(|_| (), None);
        }
        // Checks the final snapshot.
        anchor_ref.commit_snapshot_clock() != S::Clock::default()
            && anchor_ref.commit_snapshot_clock() <= snapshot
    }

    /// Waits for the final state of the [Journal] to be determined.
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
    use crate::{AtomicCounter, RecordVersion, Storage, Version};

    #[test]
    fn journal() {
        let versioned_object = RecordVersion::default();
        let storage: Storage<AtomicCounter> = Storage::new(None);
        let transaction = storage.transaction();

        let mut journal = transaction.start();
        assert!(journal
            .create(
                &versioned_object,
                |d| {
                    *d = 10;
                    Ok(None)
                },
                None
            )
            .is_ok());

        let mut journal_inner = transaction.start();
        assert!(journal_inner
            .create(
                &versioned_object,
                |d| {
                    *d = 12;
                    Ok(None)
                },
                None
            )
            .is_err());

        assert_eq!(journal.submit(), 1);
        assert_eq!(*versioned_object.data_ref(), 10);

        assert!(journal_inner
            .create(
                &versioned_object,
                |d| {
                    *d = 2;
                    Ok(None)
                },
                None
            )
            .is_err());
        assert_eq!(journal_inner.submit(), 2);

        assert!(transaction.commit().is_ok());

        let snapshot = storage.snapshot();
        assert!(versioned_object.predate(&snapshot, &scc::ebr::Barrier::new()));
        assert!(versioned_object.consolidate());

        assert_eq!(*versioned_object.data_ref(), 10);
    }
}
