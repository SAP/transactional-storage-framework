// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Annals;
use super::{Error, Journal, Sequencer, Snapshot, Storage};

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::Mutex;

use scc::ebr;

/// [Transaction] is the atomic unit of work for all types of storage operations.
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
        Journal::new(self, self.anchor.clone())
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
        Snapshot::new(self.sequencer, Some(self), None)
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
    /// # Errors
    ///
    /// If an invalid clock value is given, an error is returned.
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
        if let Ok(mut change_records) = self.anchor.submitted_journals.lock() {
            if change_records.len() > clock {
                while change_records.len() > clock {
                    drop(change_records.pop());
                }
                let new_clock = change_records.len();
                self.clock.store(new_clock, Release);
                return Ok(new_clock);
            }
        }
        Err(Error::Fail)
    }

    /// Commits the changes made by the [Transaction].
    ///
    /// It returns a [Rubicon], giving one last chance to roll back the transaction.
    ///
    /// # Errors
    ///
    /// If the transaction cannot be committed, an error is returned.
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
        let anchor_mut_ref = unsafe {
            #[allow(clippy::cast_ref_to_mut)]
            &mut *(&*self.anchor as *const Anchor<S> as *mut Anchor<S>)
        };
        anchor_mut_ref.preliminary_snapshot = self.sequencer.get(Relaxed);
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

    /// Takes [Annals], and records them.
    pub(super) fn record(&self, record: Annals<S>) -> usize {
        let mut change_records = self.anchor.submitted_journals.lock().unwrap();
        change_records.push(record);
        let new_clock = change_records.len();
        // submit_clock is updated after the contents are moved to the anchor.
        change_records[new_clock - 1].assign_clock(new_clock);
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
        drop(self);
    }
}

/// [Rubicon] gives one last chance of rolling back the transaction.
///
/// The transaction is bound to be committed if no actions are taken before dropping the
/// [Rubicon] instance. On the other hands, the transaction stays uncommitted until the
/// [Rubicon] instance is dropped.
pub struct Rubicon<'s, S: Sequencer> {
    transaction: Option<Transaction<'s, S>>,
}

impl<'s, S: Sequencer> Rubicon<'s, S> {
    /// Rolls back the transaction.
    ///
    /// # Examples
    ///
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

    /// Gets the assigned commit snapshot of the transaction.
    ///
    /// # Examples
    ///
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
        self.transaction
            .as_ref()
            .map_or_else(S::Clock::default, |t| t.anchor.commit_snapshot())
    }
}

impl<'s, S: Sequencer> Drop for Rubicon<'s, S> {
    /// Post-processes the transaction that is not explicitly rolled back.
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            let anchor_mut_ref = unsafe {
                #[allow(clippy::cast_ref_to_mut)]
                &mut *(&*transaction.anchor as *const Anchor<S> as *mut Anchor<S>)
            };
            anchor_mut_ref.commit_snapshot = transaction.sequencer.advance(Release);
            transaction.post_process();
        }
    }
}

/// [Anchor] contains data that is required to outlive the [Transaction] instance.
pub(super) struct Anchor<S: Sequencer> {
    /// The changes made by the transaction.
    submitted_journals: Mutex<Vec<Annals<S>>>,

    /// The clock value when a commit is issued.
    preliminary_snapshot: S::Clock,

    /// The clock value when the commit is completed.
    commit_snapshot: S::Clock,
}

impl<S: Sequencer> Anchor<S> {
    fn new() -> Anchor<S> {
        Anchor {
            submitted_journals: Mutex::new(Vec::new()),
            preliminary_snapshot: S::Clock::default(),
            commit_snapshot: S::Clock::default(),
        }
    }

    /// Returns the clock value when the transaction starts to commit.
    pub(super) fn preliminary_snapshot(&self) -> S::Clock {
        self.preliminary_snapshot
    }

    /// Returns the final commit clock value of the transaction.
    pub(super) fn commit_snapshot(&self) -> S::Clock {
        self.commit_snapshot
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{AtomicCounter, RecordVersion, Version};
    use std::sync::{Arc, Barrier, Once};
    use std::thread;

    #[test]
    fn visibility() {
        static mut STORAGE: Option<Storage<AtomicCounter>> = None;
        static INIT: Once = Once::new();

        INIT.call_once(|| unsafe {
            STORAGE.replace(Storage::new(None));
        });

        let storage_ref = unsafe { STORAGE.as_ref().unwrap() };
        let versioned_object = Arc::new(RecordVersion::new());
        let transaction = Arc::new(storage_ref.transaction());
        let barrier = Arc::new(Barrier::new(2));

        let versioned_object_cloned = versioned_object.clone();
        let transaction_cloned = transaction.clone();
        let barrier_cloned = barrier.clone();
        let thread_handle = thread::spawn(move || {
            barrier_cloned.wait();

            // Step 1. Tries to acquire lock acquired by an active transaction journal.
            let mut journal = transaction_cloned.start();
            assert!(journal.create(&*versioned_object_cloned, None).is_err());
            drop(journal);

            // Step 2. Tries to acquire lock acquired by a submitted transaction journal.
            barrier_cloned.wait();
            barrier_cloned.wait();

            let mut journal = transaction_cloned.start();
            assert!(journal.create(&*versioned_object_cloned, None).is_ok());
            assert_eq!(journal.submit(), 2);
        });

        let mut journal = transaction.start();
        assert!(journal.create(&*versioned_object, None).is_ok());

        barrier.wait();
        barrier.wait();
        assert_eq!(journal.submit(), 1);
        barrier.wait();

        assert!(thread_handle.join().is_ok());

        if let Ok(transaction) = Arc::try_unwrap(transaction) {
            assert!(transaction.commit().is_ok());
        } else {
            unreachable!();
        }
    }

    #[test]
    fn wait_queue() {
        let storage: Arc<Storage<AtomicCounter>> = Arc::new(Storage::new(None));
        let versioned_object = Arc::new(RecordVersion::new());
        let num_threads = 64;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let mut thread_handles = Vec::new();
        for _ in 0..num_threads {
            let storage_cloned = storage.clone();
            let versioned_object_cloned = versioned_object.clone();
            let barrier_cloned = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_cloned.wait();
                let snapshot = storage_cloned.snapshot();
                assert!(!versioned_object_cloned.predate(&snapshot, &ebr::Barrier::new()));
                barrier_cloned.wait();
                let snapshot = storage_cloned.snapshot();
                assert!(versioned_object_cloned.predate(&snapshot, &ebr::Barrier::new()));
            }));
        }
        barrier.wait();
        let transaction = storage.transaction();
        let mut journal = transaction.start();
        let result = journal.create(&*versioned_object, None);
        assert!(result.is_ok());
        assert_eq!(journal.submit(), 1);
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(transaction.commit().is_ok());
        barrier.wait();
    }
}
