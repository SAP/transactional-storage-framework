// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// The DeriveClock trait defines the capability of deriving a clock value out of
/// an instance of a type implementing the trait.
pub trait DeriveClock<C> {
    /// Returns the derived clock value.
    fn derive(&self) -> C;
}

/// Sequencer acts as a logical clock for the storage system.
///
/// The logical clock is the most important feature of a transactional storage system as it defines
/// the flow of time.
///
/// Developers are able to choose the mechanism by implementing the Sequencer trait, for instance,
/// the system timestamp generator can directly be used, or a hardware counter can also be
/// incorporated.
pub trait Sequencer {
    /// Clock is a partially ordered type that the Sequencer relies on.
    ///
    /// It should satisfy Clone, Send, Sync and PartialOrd.
    ///
    /// Send and Sync are required as a single instance of Clock can be shared among threads.
    /// Copy is required as a logical clock value can be copied to various places.
    /// PartialOrd allows developers to implement a Lamport vector clock generator.
    ///
    /// A Sequencer implementation must be able to calculate a special value that is used to represent
    /// a snapshot that is invisible to all the present and future readers.
    type Clock: Clone + Copy + PartialOrd + Send + Sync;

    /// Tracker allows the sequencer to track all the issued clock values.
    type Tracker: DeriveClock<Self::Clock>;

    /// Creates a new instance of Sequence.
    fn new() -> Self;

    /// Returns a clock value that no valid snapshots can be associated with at the moment and the future.
    fn invalid() -> Self::Clock;

    /// Returns a clock value that represents a snapshot that is visible to all the current and future readers.
    ///
    /// The returned value may change based on the state of the sequencer.
    fn min(&self) -> Self::Clock;

    /// Gets the current logical clock value.
    fn get(&self) -> Self::Clock;

    /// Issues a logical clock value that is tracked by the sequencer.
    fn issue(&self) -> Option<Self::Tracker>;

    /// Confiscates the tracker.
    fn confiscate(&self, tracker: Self::Tracker);

    /// Aggregates all the snapshots in the system holding a Ticket.
    fn fold<F: Fn(&Self::Clock)>(&self, f: F);

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

pub struct DefaultTracker {
    clock: usize,
}

impl DeriveClock<usize> for DefaultTracker {
    fn derive(&self) -> usize {
        self.clock
    }
}

/// The default sequencer is an atomic counter with a mutex-protected BTreeMap.
///
/// An atomic counter is very inefficient when the system is equipped with a large number of processors.
/// Furthermore, the mutex-protected BTreeMap does not scale as the number of threads increases.
pub struct DefaultSequencer {
    clock: AtomicUsize,
    min_heap: std::sync::Mutex<std::collections::BTreeMap<usize, usize>>,
}

impl Sequencer for DefaultSequencer {
    type Clock = usize;
    type Tracker = DefaultTracker;

    fn new() -> DefaultSequencer {
        DefaultSequencer {
            clock: AtomicUsize::new(0),
            min_heap: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }
    fn invalid() -> usize {
        usize::MAX
    }
    fn min(&self) -> usize {
        if let Ok(min_heap) = self.min_heap.lock() {
            for entry in min_heap.iter() {
                return *entry.0;
            }
        }
        return 0;
    }
    fn get(&self) -> Self::Clock {
        self.clock.load(Relaxed)
    }
    fn issue(&self) -> Option<Self::Tracker> {
        if let Ok(mut min_heap) = self.min_heap.lock() {
            let current_clock = self.get();
            if let Some(counter) = min_heap.get_mut(&current_clock) {
                *counter += 1;
            } else {
                min_heap.insert(current_clock, 1);
            }
            return Some(Self::Tracker {
                clock: current_clock,
            });
        }
        None
    }
    fn confiscate(&self, tracker: Self::Tracker) {
        if let Ok(mut min_heap) = self.min_heap.lock() {
            if let Some(counter) = min_heap.get_mut(&tracker.clock) {
                if *counter > 0 {
                    *counter -= 1;
                    return;
                }
            }
            min_heap.remove(&tracker.clock);
        }
    }
    fn fold<F: Fn(&Self::Clock)>(&self, f: F) {
        if let Ok(min_heap) = self.min_heap.lock() {
            for entry in min_heap.iter() {
                f(entry.0);
            }
        }
    }
    fn set(&self, new_value: usize) -> Result<Self::Clock, Self::Clock> {
        self.clock.store(new_value, Relaxed);
        Ok(new_value)
    }
    fn advance(&self) -> Self::Clock {
        self.clock.fetch_add(1, Relaxed) + 1
    }
}
