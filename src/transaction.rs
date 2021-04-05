// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Log, Sequencer, Snapshot, Storage, Version, VersionLocker};
use crossbeam_epoch::{Atomic, Guard, Shared};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::{Condvar, Mutex};

/// Transaction is the atomic unit of work for all types of the storage operations.
///
/// A single strand of Journal instances constitutes a single transaction.
/// An on-going transaction can be rewound to a certain point of time by reverting submitted Journal instances.
pub struct Transaction<'s, S: Sequencer> {
    /// The transaction refers to a Storage instance to persist pending changes at commit.
    _storage: &'s Storage<S>,
    /// The transaction refers to a Sequencer instance in order to assign a clock value for commit.
    sequencer: &'s S,
    /// A piece of data that is shared by Journal instances, and may outlive the transaction.
    anchor_ptr: Atomic<TransactionAnchor<S>>,
    /// A transaction-local clock generator.
    ///
    /// The clock value is updated whenever a Journal is submitted.
    clock: AtomicUsize,
}

impl<'s, S: Sequencer> Transaction<'s, S> {
    /// Creates a new Transaction.
    ///
    /// # Examples
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
            anchor_ptr: Atomic::new(TransactionAnchor::new()),
            clock: AtomicUsize::new(0),
        }
    }

    /// Starts a new journal, returning a new Journal instance.
    ///
    /// A Journal keeps storage changes until it is dropped.
    /// In order to make the changes permanent, the Journal has to be submitted.
    ///
    /// # Examples
    /// use tss::{AtomicCounter, Journal, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let journal = transaction.start();
    /// journal.submit();
    /// ```
    pub fn start<'t>(&'t self) -> Journal<'s, 't, S> {
        Journal::new(self)
    }

    /// Takes a snapshot of the storage including changes pending in the submitted Journals.
    ///
    /// # Examples
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

    /// Gets the current local clock value of the Transaction.
    ///
    /// It returns the number of submitted Journal instances.
    ///
    /// # Examples
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

    /// Rewinds the Transaction to the given point of time.
    ///
    /// All the changes made between the latest transaction clock and the given one are reverted.
    /// It requires a mutable reference, thus ensuring exclusivity.
    ///
    /// # Examples
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
            let guard = crossbeam_epoch::pin();
            while change_records.len() > clock {
                if let Some(record) = change_records.pop() {
                    record.end(S::invalid(), &guard);
                }
            }
            let new_clock = change_records.len();
            self.clock.store(new_clock, Release);
            Ok(new_clock)
        }
    }

    /// Commits the changes made by the Transaction.
    ///
    /// It returns a Rubicon instance, giving one last chance to roll back the transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    /// transaction.commit();
    /// ```
    pub fn commit(self) -> Result<Rubicon<'s, S>, Error> {
        // Assigns a new logical clock.
        let anchor_mut_ref = unsafe {
            self.anchor_ptr
                .load(Relaxed, crossbeam_epoch::unprotected())
                .deref_mut()
        };
        anchor_mut_ref.preliminary_snapshot = self.sequencer.get();
        std::sync::atomic::fence(Release);
        anchor_mut_ref.final_snapshot = self.sequencer.advance();
        Ok(Rubicon {
            transaction: Some(self),
        })
    }

    /// Rolls back the changes made by the Transaction.
    ///
    /// # Examples
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

    /// Post-processes transaction commit.
    ///
    /// Only a Rubicon instance is allowed to call this function.
    /// Once the transaction is post-processed, the transaction cannot be rolled back.
    fn post_process(self) {
        let guard = crossbeam_epoch::pin();
        let anchor_shared = self.anchor_ptr.swap(Shared::null(), Relaxed, &guard);
        let anchor_ref = unsafe { anchor_shared.deref() };
        anchor_ref.end(&guard);
        unsafe { guard.defer_destroy(anchor_shared) };
    }

    /// Returns a reference to the TransactionAnchor instance.
    fn anchor_ref(&self) -> &TransactionAnchor<S> {
        unsafe {
            self.anchor_ptr
                .load(Relaxed, crossbeam_epoch::unprotected())
                .deref()
        }
    }
}

impl<'s, S: Sequencer> Drop for Transaction<'s, S> {
    fn drop(&mut self) {
        // Rolls back the Transaction if not committed.
        if self
            .anchor_ptr
            .load(Relaxed, unsafe { crossbeam_epoch::unprotected() })
            .is_null()
        {
            // Committed.
            return;
        }
        let guard = crossbeam_epoch::pin();
        let mut anchor_shared = self.anchor_ptr.load(Relaxed, &guard);
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
                .anchor_ptr
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

/// Journal keeps the change history.
///
/// Locks and log records are accumulated in a Journal.
pub struct Journal<'s, 't, S: Sequencer> {
    transaction: &'t Transaction<'s, S>,
    record: RecordData<S>,
}

impl<'s, 't, S: Sequencer> Journal<'s, 't, S> {
    /// Creates a new Journal.
    fn new(transaction: &'t Transaction<'s, S>) -> Journal<'s, 't, S> {
        Journal {
            transaction,
            record: RecordData::new(transaction),
        }
    }

    /// Submits a Journal, thereby advancing the logical clock of the transaction.
    ///
    /// This operation can be mapped to completion of statement execution in terms of database management software.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, Log, Storage, Transaction};
    ///
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let transaction = storage.transaction();
    /// let journal = transaction.start();
    /// assert_eq!(journal.submit(), 1);
    /// ```
    pub fn submit(self) -> usize {
        let mut change_records = self
            .transaction
            .anchor_ref()
            .submitted_journals
            .lock()
            .unwrap();
        change_records.push(self.record);
        let new_clock = change_records.len();
        unsafe {
            // submit_clock is updated after the contents are moved to the anchor.
            change_records[new_clock - 1]
                .anchor_ptr
                .load(Relaxed, crossbeam_epoch::unprotected())
                .deref_mut()
                .submit_clock = new_clock
        };
        self.transaction.clock.store(new_clock, Release);
        new_clock
    }

    /// Takes a snapshot including changes in the Journal.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, RecordVersion, Storage, Transaction, Version};
    ///
    /// let versioned_object = RecordVersion::new();
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    ///
    /// let mut journal = transaction.start();
    /// assert!(journal.create(&versioned_object, None).is_ok());
    ///
    /// let snapshot = journal.snapshot();
    /// drop(snapshot);
    /// ```
    pub fn snapshot<'r>(&'r self) -> Snapshot<'s, 't, 'r, S> {
        Snapshot::new(
            self.transaction.sequencer,
            Some(self.transaction),
            Some(self),
        )
    }

    /// Creates a versioned object.
    ///
    /// The acquired lock is never released until the Journal is dropped.
    /// If the locked version is released without a valid clock value tagged,
    /// the version is considered invalid and will be garbage-collected later.
    ///
    /// # Examples
    /// ```
    /// use tss::{AtomicCounter, RecordVersion, Storage, Transaction, Version};
    ///
    /// let versioned_object = RecordVersion::new();
    /// let storage: Storage<AtomicCounter> = Storage::new(None);
    /// let mut transaction = storage.transaction();
    ///
    /// let mut journal = transaction.start();
    /// assert!(journal.create(&versioned_object, None).is_ok());
    /// journal.submit();
    ///
    /// transaction.commit();
    ///
    /// let snapshot = storage.snapshot();
    /// let guard = crossbeam_epoch::pin();
    /// assert!(versioned_object.predate(&snapshot, &guard));
    /// ```
    pub fn create<V: Version<S>>(
        &mut self,
        version: &V,
        payload: Option<V::Data>,
    ) -> Result<(), Error> {
        let guard = crossbeam_epoch::pin();
        let version_cell_shared = version.version_cell(&guard);
        if version_cell_shared.is_null() {
            // The versioned object is not ready for versioning.
            return Err(Error::Fail);
        }
        if let Some(locker) = version.create(self.record.anchor_ptr.load(Relaxed, &guard), &guard) {
            if let Some(payload) = payload {
                if let Some(log_record) = version.write(&locker, payload, &guard) {
                    self.record.locks.push(locker);
                    self.record.logs.push(log_record);
                    return Ok(());
                }
            } else {
                self.record.locks.push(locker);
                return Ok(());
            }
        }
        Err(Error::Fail)
    }
}

/// JournalAnchor is a piece of data that outlives its associated Journal.
///
/// VersionCell may point to a JournalAnchor.
pub struct JournalAnchor<S: Sequencer> {
    anchor_ptr: Atomic<TransactionAnchor<S>>,
    wait_queue: (Mutex<(bool, usize)>, Condvar),
    creation_clock: usize,
    submit_clock: usize,
    _pin: std::marker::PhantomPinned,
}

impl<S: Sequencer> JournalAnchor<S> {
    fn new(anchor_ptr: Atomic<TransactionAnchor<S>>, creation_clock: usize) -> JournalAnchor<S> {
        JournalAnchor {
            anchor_ptr,
            wait_queue: (Mutex::new((false, 0)), Condvar::new()),
            creation_clock,
            submit_clock: usize::MAX,
            _pin: std::marker::PhantomPinned,
        }
    }

    /// Checks if the lock it has acquired can be transferred to the Journal associated with the given JournalAnchor.
    ///
    /// It returns (true, true) if the given record has started after its data was submitted to the transaction.
    pub fn lockable(&self, journal_anchor: &JournalAnchor<S>, guard: &Guard) -> (bool, bool) {
        if self.anchor_ptr.load(Relaxed, guard) != journal_anchor.anchor_ptr.load(Relaxed, guard) {
            // Different transactions.
            return (false, false);
        }
        (true, self.submit_clock <= journal_anchor.creation_clock)
    }

    /// Checks whether the transaction clock or record anchor predates self.
    pub fn predate(
        &self,
        transaction: &Transaction<S>,
        transaction_clock: usize,
        journal: Option<&Journal<S>>,
        guard: &Guard,
    ) -> bool {
        if self.anchor_ptr.load(Relaxed, guard) != transaction.anchor_ptr.load(Relaxed, guard) {
            // Different transactions.
            return false;
        }
        let submit_clock = self.submit_clock;
        if submit_clock != usize::MAX && submit_clock <= transaction_clock {
            // It was submitted and predates the given transaction local clock.
            return true;
        }
        // The given anchor is itself.
        journal.map_or_else(
            || false,
            |journal| journal.record.anchor_ptr.load(Relaxed, guard).as_raw() == self as *const _,
        )
    }

    /// The transaction record has either been committed or rolled back.
    fn end(&self) {
        if let Ok(mut wait_queue) = self.wait_queue.0.lock() {
            if !wait_queue.0 {
                // Setting the flag true has an immediate effect on all the versioned owned by the RecordData.
                //  - It allows all the other transaction to have a chance to take ownership of the versioned objects.
                wait_queue.0 = true;
                self.wait_queue.1.notify_one();
            }
        }

        // Asynchronously post-processes with the mutex acquired.
        //
        // Still, the RecordData is holding all the VersionLock instances.
        // therefore, it firstly wakes all the waiting threads up before releasing the locks.
        while let Ok(wait_queue) = self.wait_queue.0.lock() {
            if wait_queue.1 == 0 {
                break;
            }
            drop(wait_queue);
        }
    }

    /// Returns the submit-time clock value.
    pub fn submit_clock(&self) -> usize {
        self.submit_clock
    }

    /// Waits for the final state of the RecordData to be determined.
    pub fn wait<R, F: FnOnce(&S::Clock) -> R>(&self, f: F, guard: &Guard) -> Option<R> {
        if let Ok(mut wait_queue) = self.wait_queue.0.lock() {
            while !wait_queue.0 {
                wait_queue.1 += 1;
                wait_queue = self.wait_queue.1.wait(wait_queue).unwrap();
                wait_queue.1 -= 1;
            }
            // Before waking up the next waiting thread, call the given function with the mutex acquired.
            //  - For instance, if the version is owned by the transaction, ownership can be transferred.
            let result = f(unsafe { &self.anchor_ptr.load(Acquire, guard).deref().snapshot() });

            // Once the thread wakes up, it is mandated to wake the next thread up.
            if wait_queue.1 > 0 {
                self.wait_queue.1.notify_one();
            }

            return Some(result);
        }
        None
    }

    /// Returns true if the transaction is visible to the reader.
    pub fn visible(&self, snapshot: &S::Clock, guard: &Guard) -> (bool, S::Clock) {
        let anchor_ref = unsafe { self.anchor_ptr.load(Acquire, guard).deref() };
        if anchor_ref.preliminary_snapshot == S::invalid()
            || anchor_ref.preliminary_snapshot >= *snapshot
        {
            return (false, S::invalid());
        }
        // The transaction will either be committed or rolled back soon.
        if anchor_ref.final_snapshot == S::invalid() {
            self.wait(|_| (), guard);
        }
        // Checks the final snapshot.
        let final_snapshot = anchor_ref.final_snapshot;
        (
            final_snapshot != S::invalid() && final_snapshot <= *snapshot,
            final_snapshot,
        )
    }
}

/// TransactionAnchor contains data that is required to outlive the Transaction instance.
struct TransactionAnchor<S: Sequencer> {
    /// The changes made by the transaction.
    submitted_journals: std::sync::Mutex<Vec<RecordData<S>>>,
    /// The clock value at the beginning of commit.
    preliminary_snapshot: S::Clock,
    /// The clock value at the end of commit.
    final_snapshot: S::Clock,
}

impl<S: Sequencer> TransactionAnchor<S> {
    fn new() -> TransactionAnchor<S> {
        TransactionAnchor {
            submitted_journals: std::sync::Mutex::new(Vec::new()),
            /// A valid clock value is assigned when the transaction starts to commit.
            preliminary_snapshot: S::invalid(),
            /// A valid clock value is assigned while the transaction is committed.
            final_snapshot: S::invalid(),
        }
    }

    /// Post-processes the logs and locks after the transaction is terminated.
    fn end(&self, guard: &Guard) {
        let mut change_records = self.submitted_journals.lock().unwrap();
        while let Some(record) = change_records.pop() {
            record.end(self.final_snapshot, guard);
        }
    }

    /// Returns the final snapshot of the Transaction.
    pub fn snapshot(&self) -> S::Clock {
        self.final_snapshot
    }
}

/// RecordData consists of locks acquired and logs generated with the Journal.
///
/// The data structure is not exposed publicly, and its internal state can indirectly be shown through JournalAnchor.
struct RecordData<S: Sequencer> {
    anchor_ptr: Atomic<JournalAnchor<S>>,
    locks: Vec<VersionLocker<S>>,
    logs: Vec<Log>,
}

impl<S: Sequencer> RecordData<S> {
    fn new(transaction: &Transaction<S>) -> RecordData<S> {
        RecordData {
            anchor_ptr: Atomic::new(JournalAnchor::new(
                transaction.anchor_ptr.clone(),
                transaction.clock(),
            )),
            locks: Vec::new(),
            logs: Vec::new(),
        }
    }

    /// Terminates the RecordData.
    ///
    /// S::invalid() passed to this function is regarded as a revert request.
    fn end(self, snapshot: S::Clock, guard: &Guard) {
        let anchor_shared = self.anchor_ptr.swap(Shared::null(), Relaxed, guard);
        unsafe {
            // Marks the record terminated.
            anchor_shared.deref().end();
        }
        for lock in self.locks.into_iter() {
            lock.release(anchor_shared, snapshot, guard);
        }
        unsafe {
            // It is safe to destroy the anchor as all the locks are released.
            guard.defer_destroy(anchor_shared);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{AtomicCounter, RecordVersion};
    use crossbeam_utils::thread;
    use std::sync::{Arc, Barrier};

    #[test]
    fn visibility() {
        let storage: Storage<AtomicCounter> = Storage::new(None);
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
        let storage: Storage<AtomicCounter> = Storage::new(None);
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
