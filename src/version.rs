// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Sequencer, Snapshot, Transaction, TransactionCell};
use crossbeam_epoch::{Atomic, Guard, Shared};
use crossbeam_utils::atomic::AtomicCell;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// The Version trait enforces versioned objects to embed a VersionCell.
///
/// All the versioned objects in a Storage must implement the trait.
pub trait Version<S: Sequencer> {
    /// Returns a reference to the VersionCell that the versioned object owns.
    fn version_cell<'g>(&'g self, guard: &'g Guard) -> Shared<'g, VersionCell<S>>;

    /// Returns true if the versioned object is valid in the given snapshot.
    fn valid(&self, guard: &Guard, snapshot: &Snapshot<S>) -> bool {
        let version_cell_shared = self.version_cell(guard);
        if version_cell_shared.is_null() {
            // The lack of VersionCell indicates that the versioned object has been fully consolidated.
            return true;
        }
        let version_cell_ref = unsafe { version_cell_shared.deref() };
        version_cell_ref.valid(snapshot)
    }
}

/// VersionCell is a piece of data that is embedded in a versioned object.
///
/// It consists of a pair of logical clocks, and the object is deemed valid in all the storage snapshots
/// in that range if S::Clock satisfies the Ord trait, otherwise those two values represent the creation
/// and deletion time without implying a range.
///
/// A VersionCell can be locked by a transaction when the transaction creates or deletes the versioned object.
/// The transaction status change is synchronously proprated to readers of the versioned object.
///
/// A VersionCell is !Unpin as references to a VersionCell must stay valid throughout its lifetime.
pub struct VersionCell<S: Sequencer> {
    /// owner_ptr points to the owner of the VersionCell.
    ///
    /// Readers have to check the transaction state when owner_ptr points to a valid transaction.
    owner_ptr: Atomic<TransactionCell<S>>,
    deletor_ptr: Atomic<TransactionCell<S>>,
    creation_time: AtomicCell<S::Clock>,
    deletion_time: AtomicCell<S::Clock>,
    _pin: std::marker::PhantomPinned,
}

impl<S: Sequencer> Default for VersionCell<S> {
    fn default() -> VersionCell<S> {
        VersionCell {
            creator_ptr: Atomic::null(),
            deletor_ptr: Atomic::null(),
            creation_time: AtomicCell::new(S::invalid()),
            deletion_time: AtomicCell::new(S::invalid()),
            _pin: std::marker::PhantomPinned,
        }
    }
}

impl<S: Sequencer> VersionCell<S> {
    /// Creates a new VersionCell that is globally invisible.
    pub fn new() -> VersionCell<S> {
        Default::default()
    }

    /// Assigns the transaction as the creator.
    ///
    /// If the transaction is committed without reverting it, a new creation time is set.
    pub fn create(&self, transaction: &Transaction<S>) -> Option<VersionLocker<S>> {
        VersionLocker::create(self, transaction)
    }

    /// Assigns the transaction as the deletor.
    ///
    /// If the transaction is committed without reverting it, a new deletion time is set.
    pub fn delete(&self, transaction: &Transaction<S>) -> Option<VersionLocker<S>> {
        VersionLocker::delete(self, transaction)
    }

    /// Checks if the versioned object is valid in the given snapshot.
    pub fn valid(&self, snapshot: &Snapshot<S>) -> bool {
        // Checks the owner.
        if !self
            .owner_ptr
            .load(Relaxed, unsafe { crossbeam_epoch::unprotected() })
            .is_null()
        {
            let guard = crossbeam_epoch::pin();
            let owner_shared = self.owner_ptr.load(Acquire, &guard);
            if !owner_shared.is_null() {
                let transaction_cell_ref = unsafe { owner_shared.deref() };
                let (visible, clock) = transaction_cell_ref.visible(snapshot.clock());
                if visible {
                    let creation_time = self.creation_time.load();
                    if creation_time == S::invalid() || creation_time == clock {
                        return true;
                    }
                    // What it has seen is the deletor.
                    return false;
                }
            }
        }
        // Checks the creation time.
        let creation_time = self.creation_time.load();
        if creation_time == S::invalid() || creation_time > *snapshot.clock() {
            return false;
        }
        // Checks the deletion time.
        let deletion_time = self.deletion_time.load();
        if deletion_time == S::invalid() || deletion_time > *snapshot.clock() {
            return true;
        }
        false
    }
}

impl<S: Sequencer> Drop for VersionCell<S> {
    /// self.owner_ptr == Shared::null() partially proves the assertion that
    /// VersionCell outlives the transaction.
    fn drop(&mut self) {
        unsafe {
            debug_assert!(self
                .owner_ptr
                .load(Relaxed, crossbeam_epoch::unprotected())
                .is_null());
            loop {
                if self
                    .owner_ptr
                    .load(Relaxed, crossbeam_epoch::unprotected())
                    .is_null()
                {
                    break;
                }
            }
        }
    }
}

/// VersionLocker owns a VersionCell instance.
///
/// It asserts that VersionCell outlives VersionLocker.
/// In order to revert or commit the changes on a VersionCell,
/// the VersionLocker must be included in the undo log storage.
pub struct VersionLocker<S: Sequencer> {
    /// The VersionCell is guaranteed to outlive by VersionCell::drop.
    version_cell_ptr: Atomic<VersionCell<S>>,
    /// The TransactionCell owns the VersionCell.
    transaction_cell_ptr: Atomic<TransactionCell<S>>,
    /// The flag denotes the type of operation.
    creator: bool,
    /// The flag denotes that the VersionCell must not be updated on drop.
    rolled_back: bool,
}

impl<S: Sequencer> VersionLocker<S> {
    /// Locks the VersionCell.
    fn create(
        version_cell_ref: &VersionCell<S>,
        transaction: &Transaction<S>,
    ) -> Option<VersionLocker<S>> {
        if version_cell_ref.creation_time.load() != S::invalid() {
            // The VersionCell has been created by another transaction.
            return None;
        }
        let guard = crossbeam_epoch::pin();
        let transaction_cell_shared = transaction.transaction_cell(&guard);
        while let Err(result) = version_cell_ref.creator_ptr.compare_and_set(
            Shared::null(),
            transaction_cell_shared,
            Relaxed,
            &guard,
        ) {
            let current_owner_shared = result.current;
            let current_owner_ref = unsafe { current_owner_shared.deref() };
            if current_owner_ref
                .wait(|cell| {
                    if cell.snapshot() == S::invalid() {
                        // The transaction has been rolled back.
                        version_cell_ref
                            .creator_ptr
                            .compare_and_set(
                                current_owner_shared,
                                transaction_cell_shared,
                                Relaxed,
                                &guard,
                            )
                            .is_ok()
                    } else {
                        false
                    }
                })
                .map_or_else(|| false, |result| result)
            {
                break;
            }
        }
        if version_cell_ref.creation_time.load() != S::invalid() {
            // The VersionCell has been updated by another transaction.
            Some(VersionLocker {
                version_cell_ptr: Atomic::from(version_cell_ref as *const _),
                transaction_cell_ptr: Atomic::from(transaction_cell_shared),
                rolled_back: true,
            })
        } else {
            // Successfully took ownership of the VersionCell.
        }
    }

    fn delete(
        version_cell_ref: &VersionCell<S>,
        transaction: &Transaction<S>,
    ) -> Option<VersionLocker<S>> {
        None
    }
}

impl<'v, S: Sequencer> Drop for VersionLocker<'v, S> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        let transaction_cell_ref = unsafe {
            self.version_cell_ref
                .owner_ptr
                .load(Relaxed, &guard)
                .deref()
        };
        let snapshot = transaction_cell_ref.snapshot();
        self.clock_ref.store(snapshot);
        self.version_cell_ref
            .owner_ptr
            .store(Shared::null(), Release);
    }
}
