// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Log, Sequencer, Snapshot, Storage, Version, VersionLocker};
use crossbeam_epoch::{Atomic, Shared};
use scc::HashMap;
use std::collections::hash_map::RandomState;
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
    /// transaction_cell contains data that can be shared and may outlive the transaction.
    ///
    /// transaction_cell is hidden therefore any functions requiring it can only be called by the transaction.
    transaction_cell: Atomic<TransactionCell<S>>,
    /// transaction_clock is a clock generator that is local to the transaction.
    ///
    /// The clock value is updated whenever a new log is pushed.
    transaction_clock: AtomicUsize,
    /// The changes made by the transaction are kept in transaction_changes.
    transaction_changes: std::sync::Mutex<Vec<TransactionChange<S>>>,
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
            transaction_cell: Atomic::new(TransactionCell::new()),
            transaction_clock: AtomicUsize::new(0),
            transaction_changes: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Starts a new transaction session.
    ///
    /// # Examples
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// let transaction_session = transaction.start();
    /// drop(transaction_session);
    /// ```
    pub fn start<'t>(&'t self) -> TransactionSession<'s, 't, S> {
        TransactionSession::new(self)
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
        Snapshot::new(
            &self.sequencer,
            Some(unsafe {
                self.transaction_cell
                    .load(Relaxed, crossbeam_epoch::unprotected())
                    .deref()
            }),
        )
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
        self.transaction_clock.load(Acquire)
    }

    /// Advances the logical clock of the transaction by one by feeding a completed TransactionSession.
    ///
    /// This operation can be mapped to completion of statement execution in terms of database management software.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Log, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// let transaction_session = transaction.start();
    /// assert_eq!(transaction.advance(transaction_session), 1);
    /// ```
    pub fn advance<'t>(&'t self, transaction_session: TransactionSession<'s, 't, S>) -> usize {
        let mut transaction_changes = self.transaction_changes.lock().unwrap();
        transaction_changes.push(transaction_session.context);
        let new_clock = transaction_changes.len();
        self.transaction_clock.store(new_clock, Release);
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
    /// let transaction_session = transaction.start();
    /// transaction.advance(transaction_session);
    /// let result = transaction.rewind(0);
    /// assert!(result.is_ok());
    /// ```
    pub fn rewind(&mut self, transaction_clock: usize) -> Result<usize, Error> {
        let mut transaction_changes = self.transaction_changes.lock().unwrap();
        if transaction_changes.len() <= transaction_clock {
            Err(Error::Fail)
        } else {
            while transaction_changes.len() > transaction_clock {
                if let Some(transaction_change) = transaction_changes.pop() {
                    transaction_change.revert();
                }
            }
            let new_clock = transaction_changes.len();
            self.transaction_clock.store(new_clock, Release);
            Ok(new_clock)
        }
    }

    /// Locks a versioned object.
    ///
    /// The acquired lock is never released until the transaction is committed or rolled back.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, DefaultVersionedObject, Storage, Transaction, Version};
    ///
    /// let versioned_object = DefaultVersionedObject::new();
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// assert!(transaction.lock(&versioned_object).is_ok());
    /// transaction.commit();
    ///
    /// let snapshot = storage.snapshot(None);
    /// let guard = crossbeam_epoch::pin();
    /// assert!(unsafe { versioned_object.version_cell(&guard).deref() }.predate(&snapshot));
    /// ```
    pub fn lock<V: Version<S>>(&self, version: &V) -> Result<(), Error> {
        let guard = crossbeam_epoch::pin();
        let mut transaction_cell_shared = self.transaction_cell.load(Acquire, &guard);
        if transaction_cell_shared.is_null() {
            return Err(Error::Fail);
        }
        let transaction_cell_ref = unsafe { transaction_cell_shared.deref_mut() };
        let version_cell_shared = version.version_cell(&guard);
        if version_cell_shared.is_null() {
            // The versioned object is not ready for versioning.
            return Err(Error::Fail);
        }
        let version_cell_ref = unsafe { version_cell_shared.deref() };
        let result = match transaction_cell_ref
            .locks
            .insert(version_cell_ref.id(), None)
        {
            Ok(result) => {
                if let Some(locker) = version_cell_ref.lock(transaction_cell_ref, &guard) {
                    result.get().1.replace(locker);
                    Ok(())
                } else {
                    // It does not allow the lock container to have None.
                    result.erase();
                    Err(Error::Fail)
                }
            }
            Err(error) => {
                debug_assert!(error.0.get().1.is_some());
                Err(Error::Fail)
            }
        };
        result
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
        let guard = crossbeam_epoch::pin();
        let mut transaction_cell_shared = self.transaction_cell.load(Acquire, &guard);
        if transaction_cell_shared.is_null() {
            return Err(Error::Fail);
        }
        let transaction_cell_ref = unsafe { transaction_cell_shared.deref_mut() };
        transaction_cell_ref.preliminary_snapshot = self.sequencer.get();
        std::sync::atomic::fence(Release);
        transaction_cell_ref.final_snapshot = self.sequencer.advance();
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
        let transaction_cell_shared = self.transaction_cell.swap(Shared::null(), Relaxed, &guard);
        let transaction_cell_ref = unsafe { transaction_cell_shared.deref() };
        transaction_cell_ref.end();
        unsafe { guard.defer_destroy(transaction_cell_shared) };
    }
}

impl<'s, S: Sequencer> Drop for Transaction<'s, S> {
    fn drop(&mut self) {
        // Rolls back the Transaction if not committed.
        let guard = crossbeam_epoch::pin();
        let mut transaction_cell_shared = self.transaction_cell.load(Relaxed, &guard);
        if !transaction_cell_shared.is_null() {
            // The transaction cell has neither passed to other components nor consumed.
            let transaction_cell_ref = unsafe { transaction_cell_shared.deref_mut() };
            transaction_cell_ref.preliminary_snapshot = S::invalid();
            transaction_cell_ref.end();
            unsafe { guard.defer_destroy(transaction_cell_shared) };
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
                .transaction_cell
                .load(Relaxed, crossbeam_epoch::unprotected())
                .deref()
                .snapshot()
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

/// TransactionChange consists of locks acquired and logs generated by the TransactionSession.
struct TransactionChange<S: Sequencer> {
    _locks: Vec<VersionLocker<S>>,
    logs: Vec<Log>,
}

impl<S: Sequencer> TransactionChange<S> {
    /// Reverts changes.
    fn revert(self) {
        for log in self.logs.into_iter() {
            log.undo();
        }
    }
}

/// TransactionSession manages the contextual data for a transactional job.
///
/// Locks and log records are accumulated in a TransactionSession.
pub struct TransactionSession<'s, 't, S: Sequencer> {
    _transaction: &'t Transaction<'s, S>,
    context: TransactionChange<S>,
}

impl<'s, 't, S: Sequencer> TransactionSession<'s, 't, S> {
    fn new(transaction: &'t Transaction<'s, S>) -> TransactionSession<'s, 't, S> {
        TransactionSession {
            _transaction: transaction,
            context: TransactionChange {
                _locks: Vec::new(),
                logs: Vec::new(),
            },
        }
    }
}

/// TransactionCell is a piece of data that represents the future snapshot when the transaction is committed.
///
/// If the Transaction is rolled back, the TransactionCell is dropped without being updated.
pub struct TransactionCell<S: Sequencer> {
    wait_queue: (Mutex<(bool, usize)>, Condvar),
    locks: HashMap<usize, Option<VersionLocker<S>>, RandomState>,
    preliminary_snapshot: S::Clock,
    final_snapshot: S::Clock,
}

impl<S: Sequencer> TransactionCell<S> {
    fn new() -> TransactionCell<S> {
        TransactionCell {
            /// wait_queue is used to wait for the transaction to be completed.
            ///
            /// The end and wait functions implement a FIFO wait queuing mechanism.
            /// It acts as a lock for a versioned object.
            wait_queue: (Mutex::new((false, 0)), Condvar::new()),
            /// Locks that the transaction has acquired.
            locks: Default::default(),
            /// A valid clock value is assigned when the transaction starts to commit.
            preliminary_snapshot: S::invalid(),
            /// A valid clock value is assigned once the transaction has been committed.
            final_snapshot: S::invalid(),
        }
    }

    // The transaction has either been committed or rolled back.
    fn end(&self) {
        if let Ok(mut wait_queue) = self.wait_queue.0.lock() {
            if !wait_queue.0 {
                // Setting the flag true has an immediate effect on all the versioned owned by the transaction.
                //  - It allows all the other transaction to have a chance to take ownership of the versioned objects.
                wait_queue.0 = true;
                self.wait_queue.1.notify_one();
            }
        }

        // Asynchronously post-processing cleanup with the mutex acquired.
        //  - Still, the transaction is holding all the VersionLock instances.
        //  - Therefore, firstly, wakes all the waiting threads up.
        while let Ok(wait_queue) = self.wait_queue.0.lock() {
            if wait_queue.1 == 0 {
                break;
            }
            drop(wait_queue);
        }

        // Explicitly releases all the locks.
        let guard = crossbeam_epoch::pin();
        for lock in self.locks.iter() {
            lock.1.as_ref().map(|lock| lock.release(self, &guard));
        }
    }

    /// Waits for the transaction to be completed.
    pub fn wait<R, F: FnOnce(&Self) -> R>(&self, f: F) -> Option<R> {
        if let Ok(mut wait_queue) = self.wait_queue.0.lock() {
            while !wait_queue.0 {
                wait_queue.1 += 1;
                wait_queue = self.wait_queue.1.wait(wait_queue).unwrap();
                wait_queue.1 -= 1;
            }
            // Before waking up the next waiting thread, call the given function with the mutex acquired.
            //  - For instance, if the version is owned by the transaction, ownership can be transferred.
            let result = f(self);

            // Once the thread wakes up, it is mandated to wake the next thread up.
            if wait_queue.1 > 0 {
                self.wait_queue.1.notify_one();
            }

            return Some(result);
        }
        None
    }

    /// Returns true if the transaction is visible to the reader.
    pub fn visible(&self, snapshot: &S::Clock) -> (bool, S::Clock) {
        if self.preliminary_snapshot == S::invalid() || self.preliminary_snapshot >= *snapshot {
            return (false, S::invalid());
        }
        // The transaction will either be committed or rolled back soon.
        if self.final_snapshot == S::invalid() {
            self.wait(|_| ());
        }
        // Checks the final snapshot.
        let final_snapshot = self.final_snapshot;
        (
            final_snapshot != S::invalid() && final_snapshot <= *snapshot,
            final_snapshot,
        )
    }

    pub fn snapshot(&self) -> S::Clock {
        self.final_snapshot
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::DefaultSequencer;
    use crossbeam_utils::thread;
    use std::sync::{Arc, Barrier};

    #[test]
    fn wait_queue() {
        let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
        let num_threads = 64;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let guard = crossbeam_epoch::pin();
        thread::scope(|s| {
            let transaction = storage.transaction();
            let transaction_cell_ref =
                unsafe { transaction.transaction_cell.load(Acquire, &guard).deref() };
            for _ in 0..num_threads {
                let barrier_cloned = barrier.clone();
                s.spawn(move |_| {
                    barrier_cloned.wait();
                    transaction_cell_ref
                        .wait(|c| assert!(c.snapshot() != DefaultSequencer::invalid()));
                    barrier_cloned.wait();
                    transaction_cell_ref
                        .wait(|c| assert!(c.snapshot() != DefaultSequencer::invalid()));
                });
            }
            barrier.wait();
            std::thread::sleep(std::time::Duration::from_millis(30));
            assert!(transaction.commit().is_ok());
            barrier.wait();
        })
        .unwrap();
    }
}
