use super::{Sequencer, Storage, Transaction};

/// A snapshot of a tss::Storage.
///
/// tss::Snapshot is a snapshot of data that a tss::Storage contains at the point of time the
/// snapshot is taken. The data stored in a storage instance is only accessible through a storage
/// snapshot.
pub struct Snapshot<'s, S: Sequencer> {
    storage: &'s Storage<S>,
    transaction_info: Option<(&'s Transaction<'s, S>, usize)>,
    snapshot: S::Clock,
}

impl<'s, S: Sequencer> Snapshot<'s, S> {
    /// Takes a new snapshot of the storage.
    pub fn new(
        storage: &'s Storage<S>,
        sequencer: &S,
        transaction: Option<&'s Transaction<S>>,
    ) -> Snapshot<'s, S> {
        Snapshot {
            storage,
            transaction_info: transaction.map(|transaction| (transaction, transaction.sequence())),
            snapshot: sequencer.get(),
        }
    }

    /// Returns the clock value of the snapshot.
    pub fn get(&self) -> &S::Clock {
        &self.snapshot
    }
}
