use super::{Sequencer, Storage, Transaction};

/// A snapshot of a tss::Storage.
///
/// tss::Snapshot is a snapshot of data that a tss::Storage contains at the point of time the
/// snapshot is taken. The data stored in a storage instance is only accessible through a storage
/// snapshot.
pub struct Snapshot<'s, S: Sequencer> {
    sequencer: &'s S,
    tracker: Option<S::Tracker>,
    transaction_clock: Option<(&'s Transaction<'s, S>, usize)>,
    snapshot: S::Clock,
}

impl<'s, S: Sequencer> Snapshot<'s, S> {
    /// Takes a new snapshot of the storage.
    pub fn new(sequencer: &'s S, transaction: Option<&'s Transaction<S>>) -> Snapshot<'s, S> {
        Snapshot {
            sequencer,
            tracker: sequencer.issue(),
            transaction_clock: transaction.map(|transaction| (transaction, transaction.clock())),
            snapshot: sequencer.get(),
        }
    }

    /// Returns the clock value of the snapshot.
    pub fn get(&self) -> &S::Clock {
        &self.snapshot
    }
}

impl<'s, S: Sequencer> Drop for Snapshot<'s, S> {
    fn drop(&mut self) {
        if let Some(tracker) = self.tracker.take() {
            self.sequencer.confiscate(tracker);
        }
    }
}
