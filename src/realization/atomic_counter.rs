// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::sequencer::DeriveClock;
use crate::Sequencer;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed};

use scc::Queue;

/// [`AtomicCounter`] implements [`Sequencer`] by managing a single atomic counter.
///
/// An atomic counter is known to be inefficient when the system is equipped with a large
/// number of processors.
pub struct AtomicCounter {
    clock: AtomicUsize,
    list: Queue<Entry>,
}

impl Sequencer for AtomicCounter {
    type Clock = usize;
    type Tracker = UsizeTracker;

    fn min(&self, _order: Ordering) -> usize {
        let min = self.get(Acquire);
        while let Ok(Some(_)) = self.list.pop_if(|e| e.ref_cnt.load(Relaxed) == 0) {}
        self.list.peek(|e| e.timestamp).map_or(min, |t| t.min(min))
    }

    fn get(&self, order: Ordering) -> Self::Clock {
        self.clock.load(order)
    }

    fn issue(&self, order: Ordering) -> Self::Tracker {
        loop {
            let candidate = self.get(order);
            let mut reuse = None;
            match self.list.push_if(
                Entry {
                    timestamp: candidate,
                    ref_cnt: AtomicUsize::new(1),
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
                                reuse.replace(e as *const Entry);
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
                    return UsizeTracker {
                        ptr: std::ptr::addr_of!(**new_entry),
                    };
                }
                Err(_) => {
                    if let Some(ptr) = reuse {
                        return UsizeTracker { ptr };
                    }
                }
            }
        }
    }

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

    fn advance(&self, order: Ordering) -> Self::Clock {
        self.clock.fetch_add(1, order) + 1
    }
}

impl Default for AtomicCounter {
    fn default() -> Self {
        AtomicCounter {
            // Starts from `1` in order to avoid using `0`.
            clock: AtomicUsize::new(1),
            list: Queue::default(),
        }
    }
}

/// [`UsizeTracker`] keeps its associated [`AtomicCounter`] from becoming oblivious of its clock.
pub struct UsizeTracker {
    ptr: *const Entry,
}

impl Clone for UsizeTracker {
    fn clone(&self) -> Self {
        let entry = unsafe { self.ptr.as_ref().unwrap() };
        let prev = entry.ref_cnt.fetch_add(1, Relaxed);
        debug_assert_ne!(prev, 0);
        Self { ptr: self.ptr }
    }
}

impl DeriveClock<usize> for UsizeTracker {
    fn clock(&self) -> usize {
        let entry = unsafe { self.ptr.as_ref().unwrap() };
        entry.timestamp
    }
}

impl Drop for UsizeTracker {
    fn drop(&mut self) {
        let entry = unsafe { self.ptr.as_ref().unwrap() };
        let prev = entry.ref_cnt.fetch_sub(1, Relaxed);
        debug_assert_ne!(prev, 0);
    }
}

unsafe impl Send for UsizeTracker {}
unsafe impl Sync for UsizeTracker {}

struct Entry {
    timestamp: usize,
    ref_cnt: AtomicUsize,
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
            clock: AtomicUsize::new(1),
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

                    let tracker = atomic_counter_cloned.issue(Acquire);
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
