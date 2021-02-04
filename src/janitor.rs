use super::Sequencer;

/// Janitor defines garbage collector interfaces.
///
/// It manages all the active readers to calculate the minimum valid sequence.
pub trait Janitor<S: Sequencer> {
    /// Tracker is a data structure that is tagged on a snapshot.
    ///
    /// It enables a Janitor to track all the active readers in the system.
    type Tracker;

    /// Creates a instance out of a sequencer.
    fn new(sequencer: &S) -> Janitor<S>;

    /// It returns the latest database snapshot represented by a sequencer clock
    /// among those that are visible to all the active readers.
    fn latest_public_snapshot(&self) -> S::Clock;
}
