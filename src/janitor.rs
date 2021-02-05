use super::Sequencer;

/// The DeriveClock trait defines the capability of deriving a clock value out of
/// an instance of a type that implements the trait.
pub trait DeriveClock<S: Sequencer> {
    /// Returns a clock value derived from self.
    fn clock(&self) -> S::Clock;
}

/// Janitor defines garbage collector interfaces.
///
/// It manages all the active readers to calculate the minimum valid sequence.
pub trait Janitor<'s, S: Sequencer> {
    /// Tracker is a data structure that is tagged on a snapshot.
    ///
    /// It enables a Janitor to track all the active readers in the system.
    type Tracker: DeriveClock<S>;

    /// Creates a instance out of a sequencer.
    fn new(sequencer: &'s S) -> Self;

    /// It returns the latest database snapshot represented by a sequencer clock
    /// among those that are visible to all the active readers.
    fn latest_public_snapshot(&self) -> S::Clock;

    /// Creates a tracker.
    fn track(&self) -> Self::Tracker;
}

pub struct DefaultTracker<S: Sequencer> {
    clock: S::Clock,
}

impl<S: Sequencer> DeriveClock<S> for DefaultTracker<S> {
    fn clock(&self) -> S::Clock {
        self.clock.clone()
    }
}
pub struct DefaultJanitor<'s, S: Sequencer> {
    sequencer: &'s S,
}

impl<'s, S: Sequencer> Janitor<'s, S> for DefaultJanitor<'s, S> {
    type Tracker = DefaultTracker<S>;
    fn new(sequencer: &S) -> DefaultJanitor<S> {
        DefaultJanitor { sequencer }
    }
    fn latest_public_snapshot(&self) -> S::Clock {
        self.sequencer.get()
    }
    fn track(&self) -> Self::Tracker {
        DefaultTracker {
            clock: self.sequencer.get(),
        }
    }
}
