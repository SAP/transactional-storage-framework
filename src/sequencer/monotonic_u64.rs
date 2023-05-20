// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! [`MonotonicU64`] [`Sequencer`] implementation.

use super::{Sequencer, ToInstant};
use crate::utils;
use scc::Queue;
use std::mem::transmute;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed};

/// [`MonotonicU64`] implements [`Sequencer`] on top of a single `u64` atomic counter.
///
/// An atomic counter is known to be inefficient when the system is equipped with a large number of
/// processors.
#[derive(Debug)]
pub struct MonotonicU64 {
    /// The current logical clock value.
    clock: AtomicU64,

    /// The list of tracked entries spread over thread-local queues.
    ///
    /// A single [`EntryContainer`] can be shared among multiple threads because of hash conflicts
    /// or too many threads having been spawned.
    sharded_entry_list: Vec<EntryContainer>,
}

/// [`U64Tracker`] has a reference to a tracking entry.
#[derive(Debug)]
pub struct U64Tracker<'s> {
    /// A reference to the [`Entry`].
    entry: &'s Entry,
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

impl Sequencer for MonotonicU64 {
    type Instant = u64;
    type Tracker<'s> = U64Tracker<'s>;

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
    fn track(&self, order: Ordering) -> Self::Tracker<'_> {
        let shard_id = utils::shard_id() % self.sharded_entry_list.len();
        loop {
            let candidate = self.now(order);
            let mut reuse: Option<&Entry> = None;
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
                                // Safety: the entry is ref-counted.
                                let prolonged_entry_ref = unsafe { transmute::<&Entry, _>(&**e) };
                                reuse.replace(prolonged_entry_ref);
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
                    // Safety: the entry is ref-counted.
                    let prolonged_entry_ref = unsafe { transmute::<&Entry, _>(&**new_entry) };
                    return U64Tracker {
                        entry: prolonged_entry_ref,
                    };
                }
                Err(_) => {
                    if let Some(entry) = reuse {
                        return U64Tracker { entry };
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

impl Default for MonotonicU64 {
    #[inline]
    fn default() -> Self {
        let num_shards = utils::advise_num_shards();
        let mut sharded_entry_list = Vec::with_capacity(num_shards);
        sharded_entry_list.resize_with(num_shards, EntryContainer::default);
        MonotonicU64 {
            // Starts from `1` in order to avoid using `0`.
            clock: AtomicU64::new(1),
            sharded_entry_list,
        }
    }
}

impl<'s> U64Tracker<'s> {
    fn entry(&self) -> &Entry {
        self.entry
    }
}

impl<'s> Clone for U64Tracker<'s> {
    #[inline]
    fn clone(&self) -> Self {
        let prev = self.entry().ref_cnt.fetch_add(1, Relaxed);
        debug_assert_ne!(prev, 0);
        Self { entry: self.entry }
    }
}

impl<'s> Drop for U64Tracker<'s> {
    #[inline]
    fn drop(&mut self) {
        let prev = self.entry().ref_cnt.fetch_sub(1, Relaxed);
        debug_assert_ne!(prev, 0);
    }
}

impl<'s> ToInstant<MonotonicU64> for U64Tracker<'s> {
    #[inline]
    fn to_instant(&self) -> u64 {
        self.entry().instant
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Release;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn atomic_counter() {
        let atomic_counter: Arc<MonotonicU64> = Arc::new(MonotonicU64::default());
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
