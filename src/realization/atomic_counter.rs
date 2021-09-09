// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::sequencer::DeriveClock;
use crate::Sequencer;

use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};
use std::sync::{Mutex, Once};

use scc::{ebr, LinkedList};

/// [`AtomicCounter`] implements [Sequencer] by managing a single atomic counter.
///
/// An atomic counter is inefficient when the system is equipped with a large number of
/// processors. Furthermore, the mutex-protected [`BTreeMap`] does not scale as the number of
/// threads increases.
pub struct AtomicCounter {
    clock: AtomicUsize,
}

impl Sequencer for AtomicCounter {
    type Clock = usize;
    type Tracker = UsizeTracker;

    fn min(&self, _order: Ordering) -> usize {
        let barrier = ebr::Barrier::new();
        let mut current = anchor().load(Acquire, &barrier);
        let mut min = self.get(Relaxed);
        while let Some(tracker) = current.as_ref() {
            if let Ok(min_heap) = tracker.min_heap.lock() {
                #[allow(clippy::never_loop)]
                for entry in min_heap.iter() {
                    if min > *entry.0 {
                        min = *entry.0;
                    }
                    break;
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

    fn fold<F: Fn(&Self::Clock)>(&self, f: F, _order: Ordering) {
        let barrier = ebr::Barrier::new();
        let mut current = anchor().load(Acquire, &barrier);
        while let Some(tracker) = current.as_ref() {
            if let Ok(min_heap) = tracker.min_heap.lock() {
                #[allow(clippy::never_loop)]
                for entry in min_heap.iter() {
                    f(entry.0);
                }
            }
            current = tracker.next_ptr(Acquire, &barrier);
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
            min_heap.remove(&self.clock);
        }
    }
}

struct LocalTracker {
    next: ebr::AtomicArc<LocalTracker>,
    min_heap: Mutex<BTreeMap<usize, usize>>,
}

impl LocalTracker {
    fn alloc_new() -> ebr::Arc<LocalTracker> {
        let barrier = ebr::Barrier::new();
        let new = ebr::Arc::new(LocalTracker {
            next: ebr::AtomicArc::null(),
            min_heap: Mutex::default(),
        });
        let mut current = anchor().load(Relaxed, &barrier);
        loop {
            new.next
                .swap((current.try_into_arc(), ebr::Tag::None), Relaxed);
            match anchor().compare_exchange(
                current,
                (Some(new.clone()), ebr::Tag::None),
                Release,
                Relaxed,
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

/// [`LocalTrackerHolder`] implements [Drop] to set a flag on the thread-local tracker when the
/// thread is terminated.
struct LocalTrackerHolder(ebr::Arc<LocalTracker>);

impl Drop for LocalTrackerHolder {
    fn drop(&mut self) {
        self.0.delete_self(Relaxed);
    }
}

thread_local! {
    static TRACKER: LocalTrackerHolder = LocalTrackerHolder(LocalTracker::alloc_new());
}
