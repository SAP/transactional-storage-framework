// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::sequencer::DeriveClock;
use crate::Sequencer;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Mutex, Once};

use scc::{ebr, LinkedList};

/// [`AtomicCounter`] implements [`Sequencer`] by managing a single atomic counter.
///
/// An atomic counter is known to be inefficient when the system is equipped with a large
/// number of processors.
pub struct AtomicCounter {
    clock: AtomicUsize,
}

impl Sequencer for AtomicCounter {
    type Clock = usize;
    type Tracker = UsizeTracker;

    fn min(&self, _order: Ordering) -> usize {
        let barrier = ebr::Barrier::new();
        let mut current = first_ptr(&barrier);
        let mut min = self.get(Acquire);
        while let Some(tracker) = current.as_ref() {
            if let Some(local_min) = tracker.min() {
                if local_min < min {
                    min = local_min;
                }
            }
            current = tracker.next_ptr(Acquire, &barrier);
        }
        min
    }

    fn get(&self, order: Ordering) -> Self::Clock {
        self.clock.load(order)
    }

    fn issue(&self, order: Ordering) -> Self::Tracker {
        TRACKER.with(|t| {
            let mut min_heap = t.0.min_heap.lock().unwrap();
            let current_clock = self.get(order);
            match min_heap.get_mut(&current_clock) {
                Some(counter) => {
                    *counter += 1;
                }
                None => {
                    min_heap.insert(current_clock, 1);
                }
            }
            Self::Tracker {
                clock: current_clock,
                local_tracker: t.0.ptr(&ebr::Barrier::new()).as_raw(),
            }
        })
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
        }
    }
}

/// [`UsizeTracker`] keeps its associated [`AtomicCounter`] from becoming oblivious of its clock.
pub struct UsizeTracker {
    clock: usize,
    local_tracker: *const LocalTracker,
}

impl Clone for UsizeTracker {
    fn clone(&self) -> Self {
        if let Ok(mut min_heap) = unsafe { (*self.local_tracker).min_heap.lock() } {
            match min_heap.get_mut(&self.clock) {
                Some(counter) => {
                    *counter += 1;
                }
                None => {
                    min_heap.insert(self.clock, 1);
                }
            };
        }
        Self {
            clock: self.clock,
            local_tracker: self.local_tracker,
        }
    }
}

impl DeriveClock<usize> for UsizeTracker {
    fn clock(&self) -> usize {
        self.clock
    }
}

impl Drop for UsizeTracker {
    fn drop(&mut self) {
        if let Ok(mut min_heap) = unsafe { (*self.local_tracker).min_heap.lock() } {
            let remove = if let Some(counter) = min_heap.get_mut(&self.clock) {
                if *counter == 1 {
                    true
                } else {
                    *counter -= 1;
                    false
                }
            } else {
                false
            };
            if remove {
                min_heap.remove(&self.clock);
            }
        }
    }
}

struct LocalTracker {
    next: ebr::AtomicArc<LocalTracker>,
    min_heap: Mutex<BTreeMap<usize, usize>>,
    orphaned: AtomicBool,
}

impl LocalTracker {
    fn alloc_new() -> ebr::Arc<LocalTracker> {
        let barrier = ebr::Barrier::new();
        let new = ebr::Arc::new(LocalTracker {
            next: ebr::AtomicArc::null(),
            min_heap: Mutex::default(),
            orphaned: AtomicBool::new(false),
        });
        let mut current = anchor().load(Relaxed, &barrier);
        loop {
            new.next.swap((current.get_arc(), ebr::Tag::None), Relaxed);
            match anchor().compare_exchange(
                current,
                (Some(new.clone()), ebr::Tag::None),
                Release,
                Relaxed,
                &barrier,
            ) {
                Ok(_) => {
                    return new;
                }
                Err((_, actual)) => {
                    current = actual;
                }
            }
        }
    }

    fn min(&self) -> Option<usize> {
        if let Ok(min_heap) = self.min_heap.lock() {
            #[allow(clippy::never_loop)]
            for entry in min_heap.iter() {
                return Some(*entry.0);
            }
        }
        if self.orphaned.load(Relaxed) {
            self.delete_self(Release);
        }
        None
    }
}

impl scc::LinkedList for LocalTracker {
    fn link_ref(&self) -> &ebr::AtomicArc<Self> {
        &self.next
    }
}

/// This is the head of the tracker linked list.
static mut ANCHOR: Option<ebr::AtomicArc<LocalTracker>> = None;
static ANCHOR_REF: Once = Once::new();

/// Returns the global anchor.
fn anchor() -> &'static ebr::AtomicArc<LocalTracker> {
    unsafe {
        ANCHOR_REF.call_once(|| {
            ANCHOR.replace(ebr::AtomicArc::null());
        });
        ANCHOR.as_ref().unwrap()
    }
}

/// Returns the first [`LocalTracker`].
fn first_ptr(barrier: &ebr::Barrier) -> ebr::Ptr<LocalTracker> {
    let mut current_ptr = anchor().load(Acquire, barrier);
    while let Some(tracker_ref) = current_ptr.as_ref() {
        if tracker_ref.is_deleted(Acquire) {
            let next_ptr = tracker_ref.next_ptr(Relaxed, barrier);
            match anchor().compare_exchange(
                current_ptr,
                (next_ptr.get_arc(), ebr::Tag::None),
                Acquire,
                Acquire,
                barrier,
            ) {
                Ok(_) => {
                    current_ptr = next_ptr;
                }
                Err((_, actual)) => {
                    return actual;
                }
            }
        } else {
            break;
        }
    }
    current_ptr
}

/// [`LocalTrackerHolder`] implements [Drop] to set a flag on the thread-local tracker when the
/// thread is terminated.
struct LocalTrackerHolder(ebr::Arc<LocalTracker>);

impl Drop for LocalTrackerHolder {
    fn drop(&mut self) {
        self.0.orphaned.store(true, Release);
    }
}

thread_local! {
    static TRACKER: LocalTrackerHolder = LocalTrackerHolder(LocalTracker::alloc_new());
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn atomic_counter() {
        static ATOMIC_COUNTER: AtomicCounter = AtomicCounter {
            clock: AtomicUsize::new(1),
        };

        let num_threads = 16;
        let mut thread_handles = Vec::with_capacity(num_threads);
        let barrier = Arc::new(Barrier::new(num_threads));
        for _ in 0..num_threads {
            let barrier_cloned = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_cloned.wait();
                for _ in 0..4096 {
                    let advanced = ATOMIC_COUNTER.advance(Release);
                    let current = ATOMIC_COUNTER.get(Acquire);
                    assert!(advanced <= current);

                    let tracker = ATOMIC_COUNTER.issue(Acquire);
                    assert!(current <= tracker.clock());

                    let min = ATOMIC_COUNTER.min(Relaxed);
                    assert!(min <= tracker.clock());

                    drop(tracker);

                    let advanced = ATOMIC_COUNTER.advance(Release);
                    let current = ATOMIC_COUNTER.get(Acquire);
                    assert!(advanced <= current);
                }
            }));
        }

        thread_handles
            .into_iter()
            .for_each(|t| assert!(t.join().is_ok()));
        assert_eq!(ATOMIC_COUNTER.min(Acquire), ATOMIC_COUNTER.get(Acquire));
    }
}
