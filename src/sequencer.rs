use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// A sequencer acts as a logical clock for the storage system.
///
/// The logical clock is the most important feature of a transactional storage system as it defines
/// the flow of time.
///
/// Developers are able to choose the mechanism by implementing the Sequencer trait, for instance,
/// the system timestamp generator can directly be used, or a hardware counter can also be
/// incorporated.
///
/// Developers must provide two special values: min, and invalid.
pub trait Sequencer {
    /// The type should satisfy Clone, Send, Sync and PartialOrd.
    ///
    /// Send and Sync are required as a single instance of Clock can be shared among threads.
    /// Copy is required as a logical clock value can be copied to various places.
    /// PartialOrd allows developers to implement a Lamport vector clock generator.
    type Clock: Clone + Copy + PartialOrd + Send + Sync;

    /// Creates a new instance of Sequence.
    fn new() -> Self;

    /// Returns a clock value that represents a snapshot that is always visible.
    ///
    /// It may not return the same value whenever it is invoked as long as it satisfies the condition.
    fn min() -> Self::Clock;

    /// Returns a clock value that no valid snapshots can be associated with.
    fn invalid() -> Self::Clock;

    /// Gets the current logical clock value.
    fn get(&self) -> Self::Clock;

    /// Sets the current logical clock value.
    ///
    /// It tries to replace the current logical clock value with the given one. It returns the
    /// result of the substitution attempt along with the latest value of the clock.
    fn set(&self, new_sequence: Self::Clock) -> Result<Self::Clock, Self::Clock>;

    /// Advances the logical clock.
    ///
    /// It returns the advanced value.
    fn advance(&self) -> Self::Clock;
}

/// The default sequencer is a atomic counter.
///
/// An atomic counter is very inefficient when the system is equipped with a large number of
/// processors.
pub struct DefaultSequencer {
    clock: AtomicUsize,
}

impl Sequencer for DefaultSequencer {
    type Clock = usize;
    fn new() -> DefaultSequencer {
        DefaultSequencer {
            clock: AtomicUsize::new(0),
        }
    }
    fn min() -> usize {
        0
    }
    fn invalid() -> usize {
        usize::MAX
    }
    fn get(&self) -> Self::Clock {
        self.clock.load(Relaxed)
    }
    fn set(&self, new_value: usize) -> Result<Self::Clock, Self::Clock> {
        self.clock.store(new_value, Relaxed);
        Ok(new_value)
    }
    fn advance(&self) -> Self::Clock {
        self.clock.fetch_add(1, Relaxed) + 1
    }
}
