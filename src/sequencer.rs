// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The module defines the [`Sequencer`] trait.
//!
//! The [`Sequencer`] trait and the [`Instant`](Sequencer::Instant) are the basis of all the
//! database operations as they define the flow of time.

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
pub trait Sequencer: 'static + Debug + Default + Send + Sync + Unpin {
    /// [`Instant`](Sequencer::Instant) is a partially ordered type representing an instant in a
    /// database system.
    ///
    /// It should satisfy [`Clone`], [`Copy`], [`PartialEq`], [`PartialOrd`], [`Send`], and
    /// [`Sync`].
    ///
    /// [`Clone`], [`Copy`], [`Send`] and [`Sync`] are required as the value can be copied and sent
    /// across threads and awaits frequently. [`PartialEq`] and [`PartialOrd`] allow developers to
    /// implement a floating-point, or a `Vector Clock` generator.
    ///
    /// The [`Default`] value is regarded as `⊥`, and the value is not allowed to be used by a
    /// transaction as its commit time point value.
    type Instant: Clone + Copy + Debug + Default + PartialEq + PartialOrd + Send + Sync + Unpin;

    /// [`Tracker`](Sequencer::Tracker) allows the sequencer to track every actively used
    /// [`Instant`](Sequencer::Instant) instance associated with a [`Snapshot`](super::Snapshot).
    ///
    /// A [`Tracker`](Sequencer::Tracker) can be cloned.
    type Tracker: Clone + ToInstant<Self>;

    /// Returns an [`Instant`](Sequencer::Instant) that represents a database snapshot being
    /// visible to all the current and future readers.
    fn min(&self, order: Ordering) -> Self::Instant;

    /// Gets the current [`Instant`](Sequencer::Instant).
    fn now(&self, order: Ordering) -> Self::Instant;

    /// Tracks the current [`Instant`](Sequencer::Instant) value by wrapping it in a
    /// [`Tracker`](Sequencer::Tracker).
    fn track(&self, order: Ordering) -> Self::Tracker;

    /// Updates the current logical [`Instant`](Sequencer::Instant) value.
    ///
    /// It tries to replace the current [`Instant`](Sequencer::Instant) value with the given one,
    /// and returns the most recent [`Instant`](Sequencer::Instant).
    ///
    /// # Errors
    ///
    /// It returns an error along with the latest [`Instant`](Sequencer::Instant) value if the
    /// supplied value was unusable for the [`Sequencer`], e.g., too old.
    fn update(
        &self,
        new_sequence: Self::Instant,
        order: Ordering,
    ) -> Result<Self::Instant, Self::Instant>;

    /// Advances its own [`Instant`](Sequencer::Instant).
    ///
    /// It returns the updated [`Instant`](Sequencer::Instant).
    fn advance(&self, order: Ordering) -> Self::Instant;
}

/// The [`ToInstant`] trait defines the capability of deriving an [`Instant`](Sequencer::Instant).
pub trait ToInstant<S: Sequencer> {
    /// Returns the corresponding [`Instant`](Sequencer::Instant) value.
    fn to_instant(&self) -> S::Instant;
}

/// [`AtomicCounter`] implements [`Sequencer`] on top of a single atomic counter.
///
/// An atomic counter is known to be inefficient when the system is equipped with a large number of
/// processors.
#[derive(Debug)]
pub struct AtomicCounter {
    /// The current logical clock value.
    clock: AtomicU64,

    /// The list of tracked entries.
    list: Queue<Entry>,
}

/// [`U64Tracker`] points to a tracking entry associated with its own
/// [`Instant`](Sequencer::Instant).
#[derive(Debug)]
pub struct U64Tracker {
    /// A pointer to the [`Entry`].
    ptr: *const Entry,
}

#[derive(Debug)]
struct Entry {
    /// The instant.
    instant: u64,

    /// The reference counter.
    ref_cnt: AtomicU64,
}

impl Sequencer for AtomicCounter {
    type Instant = u64;
    type Tracker = U64Tracker;

    #[inline]
    fn min(&self, _order: Ordering) -> u64 {
        let min = self.now(Acquire);
        while let Ok(Some(_)) = self.list.pop_if(|e| e.ref_cnt.load(Relaxed) == 0) {}
        self.list.peek(|e| e.map_or(min, |t| t.instant.min(min)))
    }

    #[inline]
    fn now(&self, order: Ordering) -> Self::Instant {
        self.clock.load(order)
    }

    #[inline]
    fn track(&self, order: Ordering) -> Self::Tracker {
        loop {
            let candidate = self.now(order);
            let mut reuse = None;
            match self.list.push_if(
                Entry {
                    instant: candidate,
                    ref_cnt: AtomicU64::new(1),
                },
                |e| {
                    if let Some(e) = e {
                        if e.instant >= candidate {
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
                            return e.instant == candidate;
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
    fn update(
        &self,
        new_value: Self::Instant,
        order: Ordering,
    ) -> Result<Self::Instant, Self::Instant> {
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
    fn advance(&self, order: Ordering) -> Self::Instant {
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

impl U64Tracker {
    fn entry(&self) -> &Entry {
        // Safety: `self` is holding a strong reference to the entry, therefore the entry is
        // guaranteed to be valid.
        unsafe { self.ptr.as_ref().unwrap() }
    }
}

impl Clone for U64Tracker {
    #[inline]
    fn clone(&self) -> Self {
        let prev = self.entry().ref_cnt.fetch_add(1, Relaxed);
        debug_assert_ne!(prev, 0);
        Self { ptr: self.ptr }
    }
}

impl ToInstant<AtomicCounter> for U64Tracker {
    #[inline]
    fn to_instant(&self) -> u64 {
        self.entry().instant
    }
}

impl Drop for U64Tracker {
    #[inline]
    fn drop(&mut self) {
        let prev = self.entry().ref_cnt.fetch_sub(1, Relaxed);
        debug_assert_ne!(prev, 0);
    }
}

// Safety: the instance being pointed by `U64Tracker` is on the heap.
unsafe impl Send for U64Tracker {}

// Safety: the instance being pointed by `U64Tracker` can be accessed by other threads.
unsafe impl Sync for U64Tracker {}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::Ordering::Release;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn atomic_counter() {
        let atomic_counter: Arc<AtomicCounter> = Arc::new(AtomicCounter {
            clock: AtomicU64::new(1),
            list: Queue::default(),
        });
        let num_tasks = 16;
        let mut task_handles = Vec::with_capacity(num_tasks);
        let barrier = Arc::new(Barrier::new(num_tasks));
        for _ in 0..num_tasks {
            let atomic_counter_clone = atomic_counter.clone();
            let barrier_clone = barrier.clone();
            task_handles.push(tokio::spawn(async move {
                barrier_clone.wait().await;
                for _ in 0..4096 {
                    let advanced = atomic_counter_clone.advance(Release);
                    let current = atomic_counter_clone.now(Acquire);
                    assert!(advanced <= current);

                    let tracker = atomic_counter_clone.track(Acquire);
                    assert!(current <= tracker.to_instant());

                    let min = atomic_counter_clone.min(Relaxed);
                    assert!(min <= tracker.to_instant());

                    drop(tracker);

                    let advanced = atomic_counter_clone.advance(Release);
                    let current = atomic_counter_clone.now(Acquire);
                    assert!(advanced <= current);
                }
            }));
        }
        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }
        assert_eq!(atomic_counter.min(Acquire), atomic_counter.now(Acquire));
    }
}
