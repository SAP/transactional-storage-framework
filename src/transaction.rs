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
/// A single strand of change records constitutes a single transaction.
/// An on-going transaction can be rewound to a certain point of time.
pub struct Transaction<'s, S: Sequencer> {
    /// The transaction refers to a Storage instance to persist pending changes at commit.
    _storage: &'s Storage<S>,
    /// The transaction refers to a Sequencer instance in order to assign a clock value for commit.
    sequencer: &'s S,
    /// A piece of data that can be shared among threads via versioned objects, and may outlive the transaction.
    cell: Atomic<TransactionCell<S>>,
    /// A transaction-local clock generator.
    ///
    /// The clock value is updated whenever a new TransactionRecord is pushed.
    clock: AtomicUsize,
}

impl<'s, S: Sequencer> Transaction<'s, S> {
    /// Creates a new Transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// ```
    pub fn new(storage: &'s Storage<S>, sequencer: &'s S) -> Transaction<'s, S> {
        Transaction {
            _storage: storage,
            sequencer,
            cell: Atomic::new(TransactionCell::new()),
            clock: AtomicUsize::new(0),
        }
    }

    /// Starts a new transaction session.
    ///
    /// # Examples
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// let transaction_record = transaction.start();
    /// drop(transaction_record);
    /// ```
    pub fn start<'t>(&'t self) -> TransactionRecord<'s, 't, S> {
        TransactionRecord::new(self)
    }

    /// Takes a snapshot of the storage including changes made by the transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// let snapshot = transaction.snapshot();
    /// ```
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::new(&self.sequencer, Some(self), None)
    }

    /// Gets the current transaction-local clock value of the Transaction.
    ///
    /// It returns the size of the log record vector.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// assert_eq!(transaction.clock(), 0);
    /// ```
    pub fn clock(&self) -> usize {
        self.clock.load(Acquire)
    }

    /// Submits a TransactionRecord and advances the logical clock of the transaction.
    ///
    /// This operation can be mapped to completion of statement execution in terms of database management software.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Log, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// let transaction_record = transaction.start();
    /// assert_eq!(transaction.submit(transaction_record), 1);
    /// ```
    pub fn submit<'t>(&'t self, transaction_record: TransactionRecord<'s, 't, S>) -> usize {
        let mut change_records = self.cell_ref().submitted_change_records.lock().unwrap();
        change_records.push(transaction_record.record);
        let new_clock = change_records.len();
        unsafe {
            change_records[new_clock - 1]
                .anchor_ptr
                .load(Relaxed, crossbeam_epoch::unprotected())
                .deref_mut()
                .submit_clock = new_clock
        };
        self.clock.store(new_clock, Release);
        new_clock
    }

    /// Rewinds the Transaction to the given point of time.
    ///
    /// All the changes made between the latest transaction clock and the given one are reverted.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Log, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// let result = transaction.rewind(1);
    /// assert!(result.is_err());
    ///
    /// let transaction_record = transaction.start();
    /// transaction.submit(transaction_record);
    ///
    /// let result = transaction.rewind(0);
    /// assert!(result.is_ok());
    /// ```
    pub fn rewind(&mut self, clock: usize) -> Result<usize, Error> {
        let mut change_records = self.cell_ref().submitted_change_records.lock().unwrap();
        if change_records.len() <= clock {
            Err(Error::Fail)
        } else {
            let guard = crossbeam_epoch::pin();
            while change_records.len() > clock {
                if let Some(change_unit) = change_records.pop() {
                    change_unit.end(S::invalid(), &guard);
                }
            }
            let new_clock = change_records.len();
            self.clock.store(new_clock, Release);
            Ok(new_clock)
        }
    }

    /// Commits the changes made by the Transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// transaction.commit();
    /// ```
    pub fn commit(self) -> Result<Rubicon<'s, S>, Error> {
        // Assigns a new logical clock.
        let cell_mut_ref = unsafe {
            self.cell
                .load(Relaxed, crossbeam_epoch::unprotected())
                .deref_mut()
        };
        cell_mut_ref.preliminary_snapshot = self.sequencer.get();
        std::sync::atomic::fence(Release);
        cell_mut_ref.final_snapshot = self.sequencer.advance();
        Ok(Rubicon {
            transaction: Some(self),
        })
    }

    /// Rolls back the changes made by the Transaction.
    ///
    /// If there is nothing to rollback, it returns false.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
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
    /// Once the Transaction is post-processed, the Transaction cannot be rolled back.
    fn post_process(self) {
        let guard = crossbeam_epoch::pin();
        let cell_shared = self.cell.swap(Shared::null(), Relaxed, &guard);
        let cell_ref = unsafe { cell_shared.deref() };
        cell_ref.end(&guard);
        unsafe { guard.defer_destroy(cell_shared) };
    }

    /// Returns a reference to the TransactionCell instance.
    fn cell_ref(&self) -> &TransactionCell<S> {
        unsafe {
            self.cell
                .load(Relaxed, crossbeam_epoch::unprotected())
                .deref()
        }
    }
}

impl<'s, S: Sequencer> Drop for Transaction<'s, S> {
    fn drop(&mut self) {
        // Rolls back the Transaction if not committed.
        let guard = crossbeam_epoch::pin();
        let mut cell_shared = self.cell.load(Relaxed, &guard);
        if !cell_shared.is_null() {
            // The transaction cell has neither passed to other components nor consumed.
            let cell_ref = unsafe { cell_shared.deref_mut() };
            cell_ref.preliminary_snapshot = S::invalid();
            cell_ref.end(&guard);
            unsafe { guard.defer_destroy(cell_shared) };
            // Rewinds the transaction.
            let _result = self.rewind(0);
        }
    }
}

/// Rubicon gives one last chance of rolling back the Transaction.
///
/// The Transaction is bound to be committed if no actions are taken before dropping the Rubicon
/// instance. On the other hands, the transaction stays uncommitted until the Rubicon instance is
/// dropped.
///
/// The fact that the Transaction is stopped just before being fully committed enables developers
/// to implement a distributed transaction commit protocols, such as the two-phase-commit protocol,
/// or X/Open XA. One will be able to regard a state where a piece of code holding a Rubicon
/// instance as being a state where the distributed transaction is prepared.
pub struct Rubicon<'s, S: Sequencer> {
    transaction: Option<Transaction<'s, S>>,
}

impl<'s, S: Sequencer> Rubicon<'s, S> {
    /// Rolls back a Transaction.
    ///
    /// If there is nothing to rollback, it returns false.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// if let Ok(rubicon) = transaction.commit() {
    ///     rubicon.rollback();
    /// }
    ///
    /// let mut transaction = storage.transaction();
    /// transaction.commit();
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
    /// use tss::{DefaultSequencer, Sequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// if let Ok(rubicon) = transaction.commit() {
    ///     assert!(rubicon.snapshot() != DefaultSequencer::invalid());
    ///     rubicon.rollback();
    /// };
    /// ```
    pub fn snapshot(&self) -> S::Clock {
        unsafe {
            self.transaction
                .as_ref()
                .unwrap()
                .cell
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

/// RecordData consists of locks acquired and logs generated by the TransactionRecord.
///
/// The data structure is not exposed publicly, and its internal state can indirectly be shown through TransactionRecordAnchor.
struct RecordData<S: Sequencer> {
    anchor_ptr: Atomic<TransactionRecordAnchor<S>>,
    locks: Vec<VersionLocker<S>>,
    logs: Vec<Log>,
}

impl<S: Sequencer> RecordData<S> {
    fn new(transaction: &Transaction<S>) -> RecordData<S> {
        RecordData {
            anchor_ptr: Atomic::new(TransactionRecordAnchor::new(
                transaction.cell.clone(),
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
        if snapshot == S::invalid() {
            for log in self.logs.into_iter() {
                log.undo();
            }
        }
        unsafe {
            // It is safe to destroy the anchor as all the locks are released.
            guard.defer_destroy(anchor_shared);
        }
    }
}

/// TransactionRecordAnchor represents the state of a RecordData.
///
/// VersionCell may point to a TransactionRecordAnchor.
pub struct TransactionRecordAnchor<S: Sequencer> {
    /// TransactionCell indirectly references TransactionRecordAnchor via its change record vector.
    cell_ptr: Atomic<TransactionCell<S>>,
    wait_queue: (Mutex<(bool, usize)>, Condvar),
    _creation_clock: usize,
    submit_clock: usize,
    _pin: std::marker::PhantomPinned,
}

impl<S: Sequencer> TransactionRecordAnchor<S> {
    fn new(
        transaction_cell_ptr: Atomic<TransactionCell<S>>,
        creation_clock: usize,
    ) -> TransactionRecordAnchor<S> {
        TransactionRecordAnchor {
            cell_ptr: transaction_cell_ptr,
            wait_queue: (Mutex::new((false, 0)), Condvar::new()),
            _creation_clock: creation_clock,
            submit_clock: usize::MAX,
            _pin: std::marker::PhantomPinned,
        }
    }

    /// Checks whether the transaction clock or record anchor predates self.
    pub fn predate(
        &self,
        transaction_clock: usize,
        transaction_record_anchor: Option<&TransactionRecordAnchor<S>>,
    ) -> bool {
        let submit_clock = self.submit_clock;
        if submit_clock != usize::MAX && submit_clock <= transaction_clock {
            // It was submitted and predates the given transaction local clock.
            return true;
        }
        // The given anchor is itself.
        transaction_record_anchor
            .map_or_else(|| false, |anchor| anchor as *const _ == self as *const _)
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
            let result = f(unsafe { &self.cell_ptr.load(Acquire, guard).deref().snapshot() });

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
        let cell_ref = unsafe { self.cell_ptr.load(Acquire, guard).deref() };
        if cell_ref.preliminary_snapshot == S::invalid()
            || cell_ref.preliminary_snapshot >= *snapshot
        {
            return (false, S::invalid());
        }
        // The transaction will either be committed or rolled back soon.
        if cell_ref.final_snapshot == S::invalid() {
            self.wait(|_| (), guard);
        }
        // Checks the final snapshot.
        let final_snapshot = cell_ref.final_snapshot;
        (
            final_snapshot != S::invalid() && final_snapshot <= *snapshot,
            final_snapshot,
        )
    }
}

/// TransactionRecord manages the contextual data for a transactional job.
///
/// Locks and log records are accumulated in a TransactionRecord.
pub struct TransactionRecord<'s, 't, S: Sequencer> {
    transaction: &'t Transaction<'s, S>,
    record: RecordData<S>,
}

impl<'s, 't, S: Sequencer> TransactionRecord<'s, 't, S> {
    fn new(transaction: &'t Transaction<'s, S>) -> TransactionRecord<'s, 't, S> {
        TransactionRecord {
            transaction,
            record: RecordData::new(transaction),
        }
    }

    /// Takes a snapshot including changes in the TransactionRecord.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, DefaultVersionedObject, Storage, Transaction, Version};
    ///
    /// let versioned_object = DefaultVersionedObject::new();
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    ///
    /// let mut transaction_record = transaction.start();
    /// assert!(transaction_record.lock(&versioned_object).is_ok());
    ///
    /// let snapshot = transaction_record.snapshot();
    /// drop(snapshot);
    ///
    /// transaction.submit(transaction_record);
    pub fn snapshot<'r>(&'r self) -> Snapshot<'s, 't, 'r, S> {
        Snapshot::new(
            self.transaction.sequencer,
            Some(self.transaction),
            Some(self),
        )
    }

    /// Returns a reference to the anchor.
    pub fn anchor_ref(&self) -> &TransactionRecordAnchor<S> {
        unsafe {
            self.record
                .anchor_ptr
                .load(Relaxed, crossbeam_epoch::unprotected())
                .deref()
        }
    }

    /// Locks a versioned object.
    ///
    /// The acquired lock is never released until the TransactionRecord is dropped.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, DefaultVersionedObject, Storage, Transaction, Version};
    ///
    /// let versioned_object = DefaultVersionedObject::new();
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    ///
    /// let mut transaction_record = transaction.start();
    /// assert!(transaction_record.lock(&versioned_object).is_ok());
    /// transaction.submit(transaction_record);
    ///
    /// transaction.commit();
    ///
    /// let snapshot = storage.snapshot();
    /// let guard = crossbeam_epoch::pin();
    /// assert!(unsafe { versioned_object.version_cell(&guard).deref() }.predate(&snapshot));
    /// ```
    pub fn lock<V: Version<S>>(&mut self, version: &V) -> Result<(), Error> {
        let guard = crossbeam_epoch::pin();
        let version_cell_shared = version.version_cell(&guard);
        if version_cell_shared.is_null() {
            // The versioned object is not ready for versioning.
            return Err(Error::Fail);
        }
        let version_cell_ref = unsafe { version_cell_shared.deref() };
        if let Some(locker) =
            version_cell_ref.lock(self.record.anchor_ptr.load(Relaxed, &guard), &guard)
        {
            self.record.locks.push(locker);
            Ok(())
        } else {
            Err(Error::Fail)
        }
    }
}

/// TransactionCell is contains data that is required to outlive the Transaction instance.
struct TransactionCell<S: Sequencer> {
    /// The changes made by the transaction.
    submitted_change_records: std::sync::Mutex<Vec<RecordData<S>>>,
    /// The clock value at the beginning of commit.
    preliminary_snapshot: S::Clock,
    /// The clock value at the end of commit.
    final_snapshot: S::Clock,
}

impl<S: Sequencer> TransactionCell<S> {
    fn new() -> TransactionCell<S> {
        TransactionCell {
            submitted_change_records: std::sync::Mutex::new(Vec::new()),
            /// A valid clock value is assigned when the transaction starts to commit.
            preliminary_snapshot: S::invalid(),
            /// A valid clock value is assigned once the transaction has been committed.
            final_snapshot: S::invalid(),
        }
    }

    /// Post-processes the logs and locks after the transaction is terminated.
    fn end(&self, guard: &Guard) {
        let mut change_records = self.submitted_change_records.lock().unwrap();
        while let Some(record) = change_records.pop() {
            record.end(self.final_snapshot, guard);
        }
    }

    /// Returns the final snapshot of the Transaction.
    pub fn snapshot(&self) -> S::Clock {
        self.final_snapshot
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{DefaultSequencer, DefaultVersionedObject};
    use crossbeam_utils::thread;
    use std::sync::{Arc, Barrier};

    #[test]
    fn wait_queue() {
        let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
        let versioned_object = DefaultVersionedObject::new();
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
                    assert!(
                        !unsafe { versioned_object_ref.version_cell(&guard).deref() }
                            .predate(&snapshot)
                    );
                    barrier_cloned.wait();
                    let snapshot = storage_ref.snapshot();
                    assert!(unsafe { versioned_object_ref.version_cell(&guard).deref() }
                        .predate(&snapshot));
                });
            }
            barrier.wait();
            let mut transaction_record = transaction.start();
            let result = transaction_record.lock(&versioned_object);
            assert!(result.is_ok());
            transaction.submit(transaction_record);
            std::thread::sleep(std::time::Duration::from_millis(30));
            assert!(transaction.commit().is_ok());
            barrier.wait();
        })
        .unwrap();
    }
}
