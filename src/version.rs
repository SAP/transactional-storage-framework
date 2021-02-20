// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Sequencer, Snapshot, TransactionCell};
use crossbeam_epoch::{Atomic, Guard, Shared};
use crossbeam_utils::atomic::AtomicCell;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// The Version trait enforces versioned objects to embed a VersionCell.
///
/// All the versioned objects in a Storage must implement the trait.
pub trait Version<S: Sequencer> {
    /// Returns a reference to the VersionCell that the versioned object owns.
    fn version_cell<'g>(&'g self, guard: &'g Guard) -> Shared<'g, VersionCell<S>>;

    /// Returns true if the version predates the snapshot.
    fn predate(&self, guard: &Guard, snapshot: &Snapshot<S>) -> bool {
        let version_cell_shared = self.version_cell(guard);
        if version_cell_shared.is_null() {
            // The lack of VersionCell indicates that the versioned object has been fully consolidated.
            return true;
        }
        let version_cell_ref = unsafe { version_cell_shared.deref() };
        version_cell_ref.predate(snapshot)
    }
}

/// VersionCell is a piece of data that is embedded in a versioned object.
///
/// VersionCell represents the time point when the version is created or deleted. It represents a single point of time,
/// therefore a versioned object that needs a pair of time points, usually, creation and deletion time points,
/// requires to embed two instances of VersionCell.
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
    /// time_point represents a point of time when the version is created or deleted.
    ///
    /// The time point value cannot be reset, or updated once set by a transaction.
    time_point: AtomicCell<S::Clock>,
    /// VersionCell cannot be moved.
    _pin: std::marker::PhantomPinned,
}

impl<S: Sequencer> Default for VersionCell<S> {
    fn default() -> VersionCell<S> {
        VersionCell {
            owner_ptr: Atomic::null(),
            time_point: AtomicCell::new(S::invalid()),
            _pin: std::marker::PhantomPinned,
        }
    }
}

impl<S: Sequencer> VersionCell<S> {
    /// Creates a new VersionCell that is globally invisible.
    pub fn new() -> VersionCell<S> {
        Default::default()
    }

    /// Assigns the transaction as the owner.
    ///
    /// The transaction semantics adheres to the two-phase locking protocol.
    /// If the transaction is committed, a new time point is set.
    pub fn lock(
        &self,
        transaction_cell: &TransactionCell<S>,
        guard: &Guard,
    ) -> Option<VersionLocker<S>> {
        VersionLocker::lock(self, transaction_cell, guard)
    }

    /// Checks if the VersionCell predates the snapshot.
    pub fn predate(&self, snapshot: &Snapshot<S>) -> bool {
        // Checks the time point.
        let time_point = self.time_point.load();
        if time_point != S::invalid() {
            return time_point <= *snapshot.clock();
        }

        // Checks the owner.
        if !self
            .owner_ptr
            .load(Relaxed, unsafe { crossbeam_epoch::unprotected() })
            .is_null()
        {
            let guard = crossbeam_epoch::pin();
            let owner_shared = self.owner_ptr.load(Acquire, &guard);
            if owner_shared.as_raw() != self.locked_state() && !owner_shared.is_null() {
                let transaction_cell_ref = unsafe { owner_shared.deref() };
                if snapshot.own(transaction_cell_ref) {
                    return true;
                }
                let visible = transaction_cell_ref.visible(snapshot.clock()).0;
                if self.owner_ptr.load(Acquire, &guard) == owner_shared {
                    // The owner has yet to post-process changes after committed.
                    return visible;
                }
            }
        }

        // Checks the time point again.
        let time_point = self.time_point.load();
        time_point != S::invalid() && time_point <= *snapshot.clock()
    }

    /// The memory address is used as its identifier.
    pub fn id(&self) -> usize {
        self.locked_state() as usize
    }

    /// VersionCell having owner_ptr == locked_state() is currently being locked.
    fn locked_state(&self) -> *const TransactionCell<S> {
        self as *const _ as *const TransactionCell<S>
    }
}

impl<S: Sequencer> Drop for VersionCell<S> {
    /// VersionCell cannot be dropped when it is locked.
    ///
    /// self.owner_ptr == Shared::null() partially proves the assertion that VersionCell outlives the TransactionCell.
    /// Dropping a VersionCell is usually triggered by the garbage collector of the storage system,
    /// and the garbage collector must ensure to consolidate versioned objects after the transactions are post-processed.
    fn drop(&mut self) {
        unsafe {
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
/// It is not an RAII-style type, and it requires the owner to explicitly call the unlock function.
pub struct VersionLocker<S: Sequencer> {
    /// The VersionCell is guaranteed to outlive by VersionCell::drop.
    version_cell_ptr: Atomic<VersionCell<S>>,
}

impl<S: Sequencer> VersionLocker<S> {
    /// Locks the VersionCell.
    fn lock(
        version_cell_ref: &VersionCell<S>,
        transaction_cell: &TransactionCell<S>,
        guard: &Guard,
    ) -> Option<VersionLocker<S>> {
        if version_cell_ref.time_point.load() != S::invalid() {
            // The VersionCell has been updated by another transaction.
            return None;
        }

        let locked_state = Shared::from(version_cell_ref.locked_state());
        while let Err(result) = version_cell_ref.owner_ptr.compare_and_set(
            Shared::null(),
            locked_state,
            Relaxed,
            &guard,
        ) {
            let current_owner_shared = result.current;
            if current_owner_shared.as_raw() == version_cell_ref.locked_state() {
                // Another transaction is locking the VersionCell.
                continue;
            }
            let current_owner_ref = unsafe { current_owner_shared.deref() };
            if current_owner_ref
                .wait(|cell| {
                    if cell.snapshot() == S::invalid() {
                        // The transaction has been rolled back.
                        //  - Tries to overtake ownership.
                        //  - CAS returning false means that another transaction overtook ownership.
                        version_cell_ref
                            .owner_ptr
                            .compare_and_set(current_owner_shared, locked_state, Relaxed, &guard)
                            .is_ok()
                    } else {
                        // The transaction has been committed, and the time point is bound to be set.
                        false
                    }
                })
                .map_or_else(|| false, |result| result)
            {
                // This transaction has sucessfully locked the VersionCell.
                break;
            }

            if version_cell_ref.time_point.load() != S::invalid() {
                // The VersionCell has updated its time point.
                return None;
            }
        }

        if version_cell_ref.time_point.load() != S::invalid() {
            // The VersionCell has updated its time point.
            let owner_shared = version_cell_ref
                .owner_ptr
                .swap(Shared::null(), Relaxed, &guard);
            debug_assert_eq!(owner_shared, locked_state);
            return None;
        }
        let owner_shared = version_cell_ref.owner_ptr.swap(
            Shared::from(transaction_cell as *const _),
            Relaxed,
            &guard,
        );
        debug_assert_eq!(owner_shared, locked_state);

        Some(VersionLocker {
            version_cell_ptr: Atomic::from(version_cell_ref as *const _),
        })
    }

    /// Unlocks the VersionCell.
    pub fn unlock(&self, transaction_cell_ref: &TransactionCell<S>) {
        let guard = crossbeam_epoch::pin();
        let version_cell_ref = unsafe { self.version_cell_ptr.load(Relaxed, &guard).deref() };
        let snapshot = transaction_cell_ref.snapshot();
        version_cell_ref.time_point.store(snapshot);
        let result = version_cell_ref.owner_ptr.compare_and_set(
            Shared::from(transaction_cell_ref as *const _),
            Shared::null(),
            Release,
            &guard,
        );
        debug_assert!(snapshot == S::invalid() || result.is_ok());
    }
}
