// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! [`AtomicCounter`] implementation.

use super::{Sequencer, ToInstant};
use crate::utils;
use scc::Queue;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed};

/// [`AtomicCounter`] implements [`Sequencer`] on top of a single atomic counter.
///
/// An atomic counter is known to be inefficient when the system is equipped with a large number of
/// processors.
#[derive(Debug)]
pub struct AtomicCounter {
    /// The current logical clock value.
    clock: AtomicU64,

    /// The list of tracked entries spread over thread-local queues.
    ///
    /// A single [`EntryContainer`] can be shared among multiple threads because of hash conflicts
    /// or too many threads having been spawned.
    sharded_entry_list: Vec<EntryContainer>,
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

/// [`EntryContainer`] is aligned to a typical size of cache lines.
#[repr(align(64))]
#[derive(Debug, Default)]
struct EntryContainer(Queue<Entry>);

impl Sequencer for AtomicCounter {
    type Instant = u64;
    type Tracker = U64Tracker;

    #[inline]
    fn min(&self, _order: Ordering) -> u64 {
        let mut min = self.now(Acquire);
        for entry_list in &self.sharded_entry_list {
            while let Ok(Some(_)) = entry_list.0.pop_if(|e| e.ref_cnt.load(Relaxed) == 0) {}
            min = entry_list.0.peek(|e| e.map_or(min, |t| t.instant.min(min)));
        }
        min
    }

    #[inline]
    fn now(&self, order: Ordering) -> Self::Instant {
        self.clock.load(order)
    }

    #[inline]
    fn track(&self, order: Ordering) -> Self::Tracker {
        let shard_id = utils::shard_id() % self.sharded_entry_list.len();
        loop {
            let candidate = self.now(order);
            let mut reuse = None;
            match self.sharded_entry_list[shard_id].0.push_if(
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
        let num_shards = utils::advise_num_shards();
        let mut sharded_entry_list = Vec::with_capacity(num_shards);
        sharded_entry_list.resize_with(num_shards, EntryContainer::default);
        AtomicCounter {
            // Starts from `1` in order to avoid using `0`.
            clock: AtomicU64::new(1),
            sharded_entry_list,
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

impl ToInstant<AtomicCounter> for U64Tracker {
    #[inline]
    fn to_instant(&self) -> u64 {
        self.entry().instant
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::Ordering::Release;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn atomic_counter() {
        let atomic_counter: Arc<AtomicCounter> = Arc::new(AtomicCounter::default());
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
