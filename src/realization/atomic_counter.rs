// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::{DeriveClock, Sequencer};

use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{self, Relaxed};
use std::sync::{Arc, Mutex};

/// [`AtomicCounter`] implements [Sequencer] by managing a single atomic counter.
///
/// An atomic counter is inefficient when the system is equipped with a large number of
/// processors. Furthermore, the mutex-protected [`BTreeMap`] does not scale as the number of
/// threads increases.
pub struct AtomicCounter {
    clock: AtomicUsize,
    min_heap: Arc<Mutex<BTreeMap<usize, usize>>>,
}

impl Sequencer for AtomicCounter {
    type Clock = usize;
    type Tracker = UsizeTracker;

    fn new() -> AtomicCounter {
        AtomicCounter {
            clock: AtomicUsize::new(0),
            min_heap: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn min(&self, _order: Ordering) -> usize {
        if let Ok(min_heap) = self.min_heap.lock() {
            #[allow(clippy::never_loop)]
            for entry in min_heap.iter() {
                return *entry.0;
            }
        }
        0
    }

    fn get(&self, order: Ordering) -> Self::Clock {
        self.clock.load(order)
    }

    fn issue(&self, order: Ordering) -> Self::Tracker {
        let mut min_heap = self.min_heap.lock().unwrap();
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
            min_heap: self.min_heap.clone(),
        }
    }

    fn fold<F: Fn(&Self::Clock)>(&self, f: F, _order: Ordering) {
        if let Ok(min_heap) = self.min_heap.lock() {
            for entry in min_heap.iter() {
                f(entry.0);
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

/// [`UsizeTracker`] keeps its associated [`AtomicCounter`] from becoming oblivious of its clock.
pub struct UsizeTracker {
    clock: usize,
    min_heap: Arc<Mutex<BTreeMap<usize, usize>>>,
}

impl Clone for UsizeTracker {
    fn clone(&self) -> Self {
        if let Ok(mut min_heap) = self.min_heap.lock() {
            if let Some(counter) = min_heap.get_mut(&self.clock) {
                *counter += 1;
            }
        }
        Self {
            clock: self.clock,
            min_heap: self.min_heap.clone(),
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
        if let Ok(mut min_heap) = self.min_heap.lock() {
            if let Some(counter) = min_heap.get_mut(&self.clock) {
                if *counter > 0 {
                    *counter -= 1;
                    return;
                }
            }
            min_heap.remove(&self.clock);
        }
    }
}
