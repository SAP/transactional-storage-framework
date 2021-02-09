use super::{Sequencer, Snapshot, Transaction, TransactionCell};
use crossbeam_epoch::Atomic;
use crossbeam_utils::atomic::AtomicCell;

/// The Version trait enforces versioned objects to embed a VersionCell.
///
/// All the versioned objects in tss::Storage must implement the trait.
pub trait Version<S: Sequencer> {
    /// Returns a reference to the VersionCell that the versioned object owns.
    fn version_cell<'v>(&'v self) -> &'v VersionCell<S>;
}

/// VersionCell is a piece of data that is embedded in a versioned object.
///
/// A VersionCell instance defines the range of logical clock values in which the object is valid.
/// It consists of a pair of logical clocks, and the object is deemed valid in all the storage snapshots
/// in that range.
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

    pub fn create(&self, transaction: &Transaction<S>) -> Option<VersionLocker<S>> {
        None
    }

    pub fn delete(&self, transaction: &Transaction<S>) -> Option<VersionLocker<S>> {
        None
    }
}

/// VersionLocker owns a VersionCell instance.
pub struct VersionLocker<'v, S: Sequencer> {
    version_cell_ref: &'v VersionCell<S>,
    clock_ref: &'v AtomicCell<S::Clock>,
    transaction_cell_ref: &'v TransactionCell<S>,
}

impl<'v, S: Sequencer> Drop for VersionLocker<'v, S> {
    fn drop(&mut self) {}
}
