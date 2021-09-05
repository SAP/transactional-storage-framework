// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor as JournalAnchor;
use super::{Error, Journal, Log, Sequencer, Snapshot, Storage, Version, VersionLocker};

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::Mutex;

use scc::ebr;

/// [Transaction] is the atomic unit of work for all types of the storage operations.
///
/// A single strand of [Journal] constitutes a [Transaction]. An on-going transaction can be
/// rewound to a certain point of time by reverting submitted [Journal] instances.
pub struct Transaction<'s, S: Sequencer> {
    /// The transaction refers to a [Storage] to persist pending changes at commit.
    _storage: &'s Storage<S>,

    /// The transaction refers to a [Sequencer] in order to assign a [Clock](Sequencer::Clock).
    sequencer: &'s S,

    /// A piece of data that is shared among [Journal] instances in the [Transaction].
    ///
    /// It outlives the [Transaction].
    anchor: ebr::Arc<Anchor<S>>,

    /// A transaction-local clock generator.
    ///
    /// The clock value is updated whenever a [Journal] is submitted.
    clock: AtomicUsize,
}

impl<'s, S: Sequencer> Transaction<'s, S> {
    /// Creates a new [Transaction].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// ```
    pub fn new(storage: &'s Storage<S>, sequencer: &'s S) -> Transaction<'s, S> {
        Transaction {
            _storage: storage,
            sequencer,
            anchor: ebr::Arc::new(Anchor::new()),
            clock: AtomicUsize::new(0),
        }
    }

    /// Starts a new [Journal].
    ///
    /// A [Journal] keeps storage changes until it is dropped. In order to make the changes
    /// permanent, the [Journal] has to be submitted.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let journal = transaction.start();
    /// journal.submit();
    /// ```
    pub fn start<'t>(&'t self) -> Journal<'s, 't, S> {
        Journal::new(self)
    }

    /// Takes a snapshot of the [Storage] including changes pending in the submitted [Journal]
    /// instances.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let snapshot = transaction.snapshot();
    /// ```
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::new(&self.sequencer, Some(self), None)
    }

    /// Gets the current local clock value of the [Transaction].
    ///
    /// It returns the number of submitted Journal instances.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Journal, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let journal = transaction.start();
    /// let clock = journal.submit();
    ///
    /// assert_eq!(transaction.clock(), 1);
    /// assert_eq!(clock, 1);
    /// ```
    pub fn clock(&self) -> usize {
        self.clock.load(Acquire)
    }

    /// Rewinds the [Transaction] to the given point of time.
    ///
    /// All the changes made between the latest transaction clock and the given one are
    /// reverted. It requires a mutable reference, thus ensuring exclusivity.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Log, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    /// let result = transaction.rewind(1);
    /// assert!(result.is_err());
    ///
    /// let journal = transaction.start();
    /// journal.submit();
    ///
    /// let result = transaction.rewind(0);
    /// assert!(result.is_ok());
    /// ```
    pub fn rewind(&mut self, clock: usize) -> Result<usize, Error> {
        let mut change_records = self.anchor_ref().submitted_journals.lock().unwrap();
        if change_records.len() <= clock {
            Err(Error::Fail)
        } else {
            while change_records.len() > clock {
                drop(change_records.pop());
            }
            let new_clock = change_records.len();
            self.clock.store(new_clock, Release);
            Ok(new_clock)
        }
    }

    /// Commits the changes made by the [Transaction].
    ///
    /// It returns a [Rubicon], giving one last chance to roll back the transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    /// transaction.commit();
    /// ```
    pub fn commit(self) -> Result<Rubicon<'s, S>, Error> {
        // Assigns a new logical clock.
        let anchor_mut_ref = unsafe { &mut *(&*self.anchor as *const Anchor<S> as *mut Anchor<S>) };
        anchor_mut_ref.preliminary_snapshot = self.sequencer.get(Relaxed);
        std::sync::atomic::fence(Release);
        anchor_mut_ref.final_snapshot = self.sequencer.advance(Acquire);
        Ok(Rubicon {
            transaction: Some(self),
        })
    }

    /// Rolls back the changes made by the [Transaction].
    ///
    /// # Examples
    ///
    /// ```
    /// use tss::{AtomicCounter, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    /// transaction.rollback();
    /// ```
    pub fn rollback(self) {
        // Dropping the instance entails a synchronous transaction rollback.
        drop(self);
    }

    /// Returns a reference to its associated [Sequencer].
    pub(super) fn sequencer(&self) -> &'s S {
        self.sequencer
    }

    /// Takes a [RecordData], and records it.
    pub(super) fn record(&self, record: RecordData<S>) -> usize {
        let mut change_records = self.anchor.submitted_journals.lock().unwrap();
        change_records.push(record);
        let new_clock = change_records.len();
        // submit_clock is updated after the contents are moved to the anchor.
        change_records[new_clock - 1].anchor.submit_clock = new_clock;
        self.clock.store(new_clock, Release);
        new_clock
    }

    /// Returns a reference to its [Anchor].
    pub(super) fn anchor_ptr<'b>(&self, barrier: &'b ebr::Barrier) -> ebr::Ptr<'b, Anchor<S>> {
        self.anchor.ptr(barrier)
    }

    /// Post-processes its transaction commit.
    ///
    /// Only a Rubicon instance is allowed to call this function.
    /// Once the transaction is post-processed, the transaction cannot be rolled back.
    fn post_process(self) {
        let barrier = ebr::Barrier::new();
        self.anchor.end(&barrier);
        std::mem::forget(self);
    }
}

impl<'s, S: Sequencer> Drop for Transaction<'s, S> {
    fn drop(&mut self) {
        // Rolls back the Transaction if not committed.
        if self
            .anchor
            .load(Relaxed, unsafe { crossbeam_epoch::unprotected() })
            .is_null()
        {
            // Committed.
            return;
        }
        let guard = crossbeam_epoch::pin();
        let mut anchor_shared = self.anchor.load(Relaxed, &guard);
        if !anchor_shared.is_null() {
            let anchor_ref = unsafe { anchor_shared.deref_mut() };
            anchor_ref.preliminary_snapshot = S::invalid();
            anchor_ref.end(&guard);
            unsafe { guard.defer_destroy(anchor_shared) };
        }
    }
}

/// Rubicon gives one last chance of rolling back the Transaction.
///
/// The Transaction is bound to be committed if no actions are taken before dropping the Rubicon instance.
/// On the other hands, the transaction stays uncommitted until the Rubicon instance is dropped.
///
/// The fact that the Transaction is stopped just before being fully committed enables developers to implement
/// a distributed transaction commit protocols, such as the two-phase-commit protocol, or X/Open XA.
/// Developers will be able to regard a state where a piece of code holding a Rubicon instance as being a state
/// where the distributed transaction is prepared.
pub struct Rubicon<'s, S: Sequencer> {
    transaction: Option<Transaction<'s, S>>,
}

impl<'s, S: Sequencer> Rubicon<'s, S> {
    /// Rolls back a Transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    /// if let Ok(rubicon) = transaction.commit() {
    ///     rubicon.rollback();
    /// };
    /// ```
    pub fn rollback(mut self) {
        if let Some(transaction) = self.transaction.take() {
            transaction.rollback();
        }
    }

    /// Gets the assigned snapshot of the transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Sequencer, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    /// if let Ok(rubicon) = transaction.commit() {
    ///     assert!(rubicon.snapshot() != AtomicCounter::invalid());
    ///     rubicon.rollback();
    /// };
    /// ```
    pub fn snapshot(&self) -> S::Clock {
        unsafe {
            self.transaction
                .as_ref()
                .unwrap()
                .anchor
                .load(Relaxed, crossbeam_epoch::unprotected())
                .deref()
                .final_snapshot
        }
    }
}

impl<'s, S: Sequencer> Drop for Rubicon<'s, S> {
    /// Post-processes the transaction that is not explicitly rolled back.
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            transaction.post_process();
        }
    }
}

/// TransactionAnchor contains data that is required to outlive the Transaction instance.
pub(super) struct Anchor<S: Sequencer> {
    /// The changes made by the transaction.
    submitted_journals: Mutex<Vec<RecordData<S>>>,
    /// The clock value at the beginning of commit.
    preliminary_snapshot: S::Clock,
    /// The clock value at the end of commit.
    final_snapshot: S::Clock,
}

impl<S: Sequencer> Anchor<S> {
    fn new() -> Anchor<S> {
        Anchor {
            submitted_journals: Mutex::new(Vec::new()),
            /// A valid clock value is assigned when the transaction starts to commit.
            preliminary_snapshot: S::invalid(),
            /// A valid clock value is assigned while the transaction is committed.
            final_snapshot: S::invalid(),
        }
    }

    /// Post-processes the logs and locks after the transaction is terminated.
    fn end(&self, barrier: &ebr::Barrier) {
        let mut change_records = self.submitted_journals.lock().unwrap();
        while let Some(record) = change_records.pop() {
            record.end(self.final_snapshot, barrier);
        }
    }

    /// Returns the final snapshot of the Transaction.
    pub fn snapshot(&self) -> S::Clock {
        self.final_snapshot
    }

    pub(super) fn preliminary_snapshot(&self) -> S::Clock {
        self.preliminary_snapshot
    }
}

/// RecordData consists of locks acquired and logs generated with the Journal.
///
/// The data structure is not exposed publicly, and its internal state can indirectly be shown through JournalAnchor.
pub(super) struct RecordData<S: Sequencer> {
    anchor: ebr::Arc<JournalAnchor<S>>,
    locks: Vec<VersionLocker<S>>,
    logs: Vec<Log>,
}

impl<S: Sequencer> RecordData<S> {
    /// Returns an [ebr::Ptr] of its [JournalAnchor].
    pub(super) fn anchor_ptr<'b>(
        &self,
        barrier: &'b ebr::Barrier,
    ) -> ebr::Ptr<'b, JournalAnchor<S>> {
        self.anchor.ptr(barrier)
    }

    /// Appends a version creation change log record.
    pub(super) fn record<V: Version<S>>(
        &mut self,
        version: &V,
        locker: VersionLocker<S>,
        payload: Option<V::Data>,
        barrier: &ebr::Barrier,
    ) {
        if let Some(payload) = payload {
            if let Ok(log_record) = locker.write(version, payload, barrier) {
                self.locks.push(locker);
                if let Some(log_record) = log_record {
                    self.logs.push(log_record);
                }
            }
        } else {
            self.locks.push(locker);
        }
    }

    fn new(transaction: &Transaction<S>) -> RecordData<S> {
        RecordData {
            anchor: ebr::Arc::new(JournalAnchor::new(
                transaction.anchor.clone(),
                transaction.clock(),
            )),
            locks: Vec::new(),
            logs: Vec::new(),
        }
    }

    /// Terminates the RecordData.
    ///
    /// S::invalid() passed to this function is regarded as a revert request.
    fn end(self, snapshot: S::Clock, barrier: &ebr::Barrier) {
        let anchor_shared = self.anchor.swap(Shared::null(), Relaxed, barrier);
        unsafe {
            // Marks the record terminated.
            anchor_shared.deref().end();
        }
        for lock in self.locks.into_iter() {
            lock.release(anchor_shared, snapshot, barrier);
        }
        unsafe {
            // It is safe to destroy the anchor as all the locks are released.
            barrier.defer_destroy(anchor_shared);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{AtomicSequencer, RecordVersion};
    use crossbeam_utils::thread;
    use std::sync::{Arc, Barrier};

    #[test]
    fn visibility() {
        let storage: Storage<AtomicSequencer> = Storage::new(None);
        let versioned_object = RecordVersion::new();
        let transaction = storage.transaction();
        let barrier = Arc::new(Barrier::new(2));
        thread::scope(|s| {
            let versioned_object_ref = &versioned_object;
            let transaction_ref = &transaction;
            let barrier_cloned = barrier.clone();
            s.spawn(move |_| {
                // Step 1. Tries to acquire lock acquired by an active transaction record.
                barrier_cloned.wait();
                let mut journal = transaction_ref.start();
                assert!(journal.create(versioned_object_ref, None).is_err());
                drop(journal);

                // Step 2. Tries to acquire lock acquired by a submitted transaction record.
                barrier_cloned.wait();
                barrier_cloned.wait();
                let mut journal = transaction_ref.start();
                assert!(journal.create(versioned_object_ref, None).is_ok());
                journal.submit();
            });

            let mut journal = transaction.start();
            assert!(journal.create(&versioned_object, None).is_ok());
            barrier.wait();
            barrier.wait();
            journal.submit();
            barrier.wait();
        })
        .unwrap();
        assert!(transaction.commit().is_ok());
    }

    #[test]
    fn wait_queue() {
        let storage: Storage<AtomicSequencer> = Storage::new(None);
        let versioned_object = RecordVersion::new();
        let num_threads = 64;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        thread::scope(|s| {
            let transaction = storage.transaction();
            let storage_ref = &storage;
            let versioned_object_ref = &versioned_object;
            for _ in 0..num_threads {
                let barrier_cloned = barrier.clone();
                s.spawn(move |_| {
                    let guard = crossbeam_epoch::pin();
                    barrier_cloned.wait();
                    let snapshot = storage_ref.snapshot();
                    assert!(!versioned_object_ref.predate(&snapshot, &guard));
                    barrier_cloned.wait();
                    let snapshot = storage_ref.snapshot();
                    assert!(versioned_object_ref.predate(&snapshot, &guard));
                });
            }
            barrier.wait();
            let mut journal = transaction.start();
            let result = journal.create(&versioned_object, None);
            assert!(result.is_ok());
            journal.submit();
            std::thread::sleep(std::time::Duration::from_millis(30));
            assert!(transaction.commit().is_ok());
            barrier.wait();
        })
        .unwrap();
    }
}
