use super::{Sequencer, Snapshot, Transaction};

/// The 'Version' trait defines visibility control functions.
pub trait Version<S: Sequencer> {
    /// Creates the version using the transaction.
    fn create(&self, transaction: &Transaction<S>) -> bool;

    /// Deletes the version using the transaction.
    fn delete(&self, transaction: &Transaction<S>) -> bool;

    /// Checks if the version is visible to the transactional operation.
    fn visible(&self, snapshot: &Snapshot<S>) -> bool;

    /// Makes the version visible to all the on-going and future transactional operations.
    fn consolidate(&self) -> bool;
}
