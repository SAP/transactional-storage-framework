use super::{Sequencer, Snapshot, Transaction, TransactionCell};
use crossbeam_epoch::{Atomic, Shared};
use crossbeam_utils::atomic::AtomicCell;
use std::sync::atomic::Ordering::{Relaxed, Release};

/// The Version trait enforces versioned objects to embed a VersionCell.
///
/// All the versioned objects in tss::Storage must implement the trait.
pub trait Version<S: Sequencer> {
    /// Returns a reference to the VersionCell that the versioned object owns.
    fn version_cell<'v>(&'v self) -> &'v VersionCell<S>;
}

/// VersionCell is a piece of data that is embedded in a versioned object.
///
/// It consists of a pair of logical clocks, and the object is deemed valid in all the storage snapshots
/// in that range if S::Clock satisfies the Ord trait, otherwise those two values represent the creation
/// and deletion time without implying a range.
///
/// A VersionCell can be locked by a transaction when the transaction creates or deletes the versioned
/// object. The transaction status change is synchronously proprated to readers of the versioned object.
pub struct VersionCell<S: Sequencer> {
    /// owner_ptr points to the owner of the VersionCell.
    ///
    /// Readers have to check the transaction status when owner_ptr points to a valid transaction.
    owner_ptr: Atomic<TransactionCell<S>>,
    creation_time: AtomicCell<S::Clock>,
    deletion_time: AtomicCell<S::Clock>,
}

impl<S: Sequencer> VersionCell<S> {
    /// Creates a new VersionCell.
    pub fn new() -> VersionCell<S> {
        VersionCell {
            owner_ptr: Atomic::null(),
            creation_time: AtomicCell::new(S::invalid()),
            deletion_time: AtomicCell::new(S::invalid()),
        }
    }

    /// Assigns the transaction as the creator.
    ///
    /// If the transaction is committed without reverting it, a new creation time is set.
    pub fn create(&self, transaction: &Transaction<S>) -> Option<VersionLocker<S>> {
        VersionLocker::lock(self, &self.creation_time, transaction)
    }

    /// Assigns the transaction as the deletor.
    ///
    /// If the transaction is committed without reverting it, a new deletion time is set.
    pub fn delete(&self, transaction: &Transaction<S>) -> Option<VersionLocker<S>> {
        if self.creation_time.load() == S::invalid() {
            None
        } else {
            VersionLocker::lock(self, &self.deletion_time, transaction)
        }
    }
}

impl<S: Sequencer> Drop for VersionCell<S> {
    /// self.owner_ptr == Shared::null() partially proves the assertion that
    /// VersionCell outlives VersionCell.
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
pub struct VersionLocker<'v, S: Sequencer> {
    version_cell_ref: &'v VersionCell<S>,
    clock_ref: &'v AtomicCell<S::Clock>,
}

impl<'v, S: Sequencer> VersionLocker<'v, S> {
    /// Locks the version cell.
    fn lock(
        version_cell_ref: &'v VersionCell<S>,
        clock_ref: &'v AtomicCell<S::Clock>,
        transaction: &Transaction<S>,
    ) -> Option<VersionLocker<'v, S>> {
        if clock_ref.load() != S::invalid() {
            return None;
        }
        let guard = crossbeam_epoch::pin();
        let transaction_cell_shared = transaction.transaction_cell(&guard);
        if version_cell_ref
            .owner_ptr
            .compare_and_set(Shared::null(), transaction_cell_shared, Relaxed, &guard)
            .is_err()
        {
            return None;
        }
        if clock_ref.load() != S::invalid() {
            version_cell_ref.owner_ptr.store(Shared::null(), Relaxed);
            return None;
        }
        return Some(VersionLocker {
            version_cell_ref,
            clock_ref,
        });
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
