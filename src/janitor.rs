use super::Sequencer;

/// Janitor defines garbage collector interfaces.
///
/// It manages all the active readers to calculate the minimum valid sequence.
pub trait Janitor<S: Sequencer> {
    /// It returns the latest database snapshot represented as a sequencer clock
    /// among those that are visible to all the active readers.
    fn latest_public_snapshot(&self) -> S::Clock;
}
