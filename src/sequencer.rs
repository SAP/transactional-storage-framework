// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The module defines the [`Sequencer`] trait.
//!
//! The [`Sequencer`] trait and the [`Clock`](Sequencer::Clock) are the basis of all the database
//! operations as they define the flow of time.

use scc::Queue;
use std::fmt::Debug;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed};

/// [`Sequencer`] acts as a logical clock for the storage system.
///
/// A logical clock is the most important feature of a transactional storage system as it defines
/// the flow of time.
///
/// Developers are able to implement their own sequencing mechanism other than a simple atomic
/// counter by implementing the [`Sequencer`] trait, for instance, the system timestamp generator
/// can directly be used, or an efficient hardware-aided counter can also be incorporated.
pub trait Sequencer: 'static + Debug + Default + Send + Sync {
    /// [`Clock`](Sequencer::Clock) is a partially ordered type representing a single point of time
    /// in a system.
    ///
    /// It should satisfy [`Clone`], [`Copy`], [`PartialEq`], [`PartialOrd`], [`Send`], and
    /// [`Sync`].
    ///
    /// [`Clone`], [`Copy`], [`Send`] and [`Sync`] are required as the value can be copied and sent
    /// across threads and awaits frequently. [`PartialEq`] and [`PartialOrd`] allow developers to
    /// implement a floating-point, or a `Lamport vector clock` generator.
    ///
    /// The [`Default`] value is treated an `invisible` time point in the system.
    type Clock: Clone + Copy + Debug + Default + PartialEq + PartialOrd + Send + Sync;

    /// [`Tracker`](Sequencer::Tracker) allows the sequencer to track every actively used
    /// [`Clock`](Sequencer::Clock) instance associated with a [`Snapshot`](super::Snapshot).
    ///
    /// A [`Tracker`](Sequencer::Tracker) can be cloned.
    type Tracker: Clone + DeriveClock<Self::Clock>;

    /// Returns a [`Clock`](Sequencer::Clock) that represents a database snapshot being visible to
    /// all the current and future readers.
    ///
    /// This must not return the default [`Clock`](Sequencer::Clock) value.
    fn min(&self, order: Ordering) -> Self::Clock;

    /// Gets the current [`Clock`](Sequencer::Clock).
    ///
    /// This must not return the default [`Clock`](Sequencer::Clock) value.
    fn get(&self, order: Ordering) -> Self::Clock;

    /// Tracks the current [`Clock`](Sequencer::Clock) value by wrapping it in a
    /// [`Tracker`](Sequencer::Tracker).
    ///
    /// This must not wrap the default [`Clock`](Sequencer::Clock) value.
    fn track(&self, order: Ordering) -> Self::Tracker;

    /// Updates the current logical [`Clock`](Sequencer::Clock) value.
    ///
    /// It tries to replace the current [`Clock`](Sequencer::Clock) value with the given one. It
    /// returns the result of the update along with the latest value of the clock.
    ///
    /// # Errors
    ///
    /// It returns an error along with the latest [`Clock`](Sequencer::Clock) value of the
    /// [`Sequencer`] when the given value is unsuitable for the [`Sequencer`], for example, the
    /// supplied [`Clock`](Sequencer::Clock) is too old.
    fn update(
        &self,
        new_sequence: Self::Clock,
        order: Ordering,
    ) -> Result<Self::Clock, Self::Clock>;

    /// Advances its own [`Clock`](Sequencer::Clock).
    ///
    /// It returns the updated [`Clock`](Sequencer::Clock).
    fn advance(&self, order: Ordering) -> Self::Clock;
}

/// The [`DeriveClock`] trait defines the capability of deriving a [`Clock`](Sequencer::Clock).
pub trait DeriveClock<C> {
    /// Returns the [`Clock`](Sequencer::Clock).
    fn clock(&self) -> C;
}

/// [`AtomicCounter`] implements [`Sequencer`] on top of a single atomic counter.
///
/// An atomic counter is known to be inefficient when the system is equipped with a large number of
/// processors.
#[derive(Debug)]
pub struct AtomicCounter {
    clock: AtomicU64,
    list: Queue<Entry>,
}

impl Sequencer for AtomicCounter {
    type Clock = u64;
    type Tracker = U64Tracker;

    #[inline]
    fn min(&self, _order: Ordering) -> u64 {
        let min = self.get(Acquire);
        while let Ok(Some(_)) = self.list.pop_if(|e| e.ref_cnt.load(Relaxed) == 0) {}
        self.list.peek(|e| e.map_or(min, |t| t.timestamp.min(min)))
    }

    #[inline]
    fn get(&self, order: Ordering) -> Self::Clock {
        self.clock.load(order)
    }

    #[inline]
    fn track(&self, order: Ordering) -> Self::Tracker {
        loop {
            let candidate = self.get(order);
            let mut reuse = None;
            match self.list.push_if(
                Entry {
                    timestamp: candidate,
                    ref_cnt: AtomicU64::new(1),
                },
                |e| {
                    if let Some(e) = e {
                        if e.timestamp >= candidate {
                            if e.ref_cnt
                                .fetch_update(Relaxed, Relaxed, |r| {
                                    if r == 0 {
                                        None
                                    } else {
                                        Some(r + 1)
                                    }
                                })
                                .is_ok()
                            {
                                // Reuse the entry.
                                reuse.replace(std::ptr::addr_of!((**e)));
                                return false;
                            }
                            // Cannot push a new entry if the existing if larger.
                            return e.timestamp == candidate;
                        }
                    }
                    true
                },
            ) {
                Ok(new_entry) => {
                    debug_assert!(reuse.is_none());
                    return U64Tracker {
                        ptr: std::ptr::addr_of!(**new_entry),
                    };
                }
                Err(_) => {
                    if let Some(ptr) = reuse {
                        return U64Tracker { ptr };
                    }
                }
            }
        }
    }

    #[inline]
    fn update(&self, new_value: Self::Clock, order: Ordering) -> Result<Self::Clock, Self::Clock> {
        let current = self.clock.load(Relaxed);
        loop {
            if current >= new_value {
                return Err(current);
            }
            if self
                .clock
                .compare_exchange(current, new_value, order, Relaxed)
                .is_ok()
            {
                return Ok(new_value);
            }
        }
    }

    #[inline]
    fn advance(&self, order: Ordering) -> Self::Clock {
        self.clock.fetch_add(1, order) + 1
    }
}

impl Default for AtomicCounter {
    #[inline]
    fn default() -> Self {
        AtomicCounter {
            // Starts from `1` in order to avoid using `0`.
            clock: AtomicU64::new(1),
            list: Queue::default(),
        }
    }
}

/// [`U64Tracker`] points to a tracking entry associated with its own [`Clock`](Sequencer::Clock).
pub struct U64Tracker {
    ptr: *const Entry,
}

impl Clone for U64Tracker {
    #[inline]
    fn clone(&self) -> Self {
        let entry = unsafe { self.ptr.as_ref().unwrap() };
        let prev = entry.ref_cnt.fetch_add(1, Relaxed);
        debug_assert_ne!(prev, 0);
        Self { ptr: self.ptr }
    }
}

impl DeriveClock<u64> for U64Tracker {
    #[inline]
    fn clock(&self) -> u64 {
        let entry = unsafe { self.ptr.as_ref().unwrap() };
        entry.timestamp
    }
}

impl Drop for U64Tracker {
    #[inline]
    fn drop(&mut self) {
        let entry = unsafe { self.ptr.as_ref().unwrap() };
        let prev = entry.ref_cnt.fetch_sub(1, Relaxed);
        debug_assert_ne!(prev, 0);
    }
}

unsafe impl Send for U64Tracker {}
unsafe impl Sync for U64Tracker {}

#[derive(Debug)]
struct Entry {
    timestamp: u64,
    ref_cnt: AtomicU64,
}

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::atomic::Ordering::Release;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn atomic_counter() {
        let atomic_counter: Arc<AtomicCounter> = Arc::new(AtomicCounter {
            clock: AtomicU64::new(1),
            list: Queue::default(),
        });

        let num_threads = 16;
        let mut thread_handles = Vec::with_capacity(num_threads);
        let barrier = Arc::new(Barrier::new(num_threads));
        for _ in 0..num_threads {
            let atomic_counter_cloned = atomic_counter.clone();
            let barrier_cloned = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_cloned.wait();
                for _ in 0..4096 {
                    let advanced = atomic_counter_cloned.advance(Release);
                    let current = atomic_counter_cloned.get(Acquire);
                    assert!(advanced <= current);

                    let tracker = atomic_counter_cloned.track(Acquire);
                    assert!(current <= tracker.clock());

                    let min = atomic_counter_cloned.min(Relaxed);
                    assert!(min <= tracker.clock());

                    drop(tracker);

                    let advanced = atomic_counter_cloned.advance(Release);
                    let current = atomic_counter_cloned.get(Acquire);
                    assert!(advanced <= current);
                }
                barrier_cloned.wait();
            }));
        }

        thread_handles
            .into_iter()
            .for_each(|t| assert!(t.join().is_ok()));
        assert_eq!(atomic_counter.min(Acquire), atomic_counter.get(Acquire));
    }
}
