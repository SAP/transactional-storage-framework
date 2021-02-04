use super::{Sequencer, Snapshot};

/// The 'Version' trait defines visibility control functions.
pub trait Version<S: Sequencer> {
    /// Checks if the version is visible to the transactional operation.
    fn visible(&self, snapshot: &Snapshot<S>) -> bool;

    /// Makes the version visible to all the on-going and future transactional operations.
    fn consolidate(&self) -> bool;
}
