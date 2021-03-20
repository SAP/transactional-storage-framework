use crate::{DeriveClock, Sequencer};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// AtomicCounter implements the Sequencer trait by managing a single atomic counter.
///
/// An atomic counter is very inefficient when the system is equipped with a large number of processors.
/// Furthermore, the mutex-protected BTreeMap does not scale as the number of threads increases.
pub struct AtomicCounter {
    clock: AtomicUsize,
    min_heap: std::sync::Mutex<std::collections::BTreeMap<usize, usize>>,
}

impl Sequencer for AtomicCounter {
    type Clock = usize;
    type Tracker = AtomicCounterTracker;

    fn new() -> AtomicCounter {
        AtomicCounter {
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
        0
    }
    fn get(&self) -> Self::Clock {
        self.clock.load(Relaxed)
    }
    fn issue(&self) -> Self::Tracker {
        let mut min_heap = self.min_heap.lock().unwrap();
        let current_clock = self.get();
        if let Some(counter) = min_heap.get_mut(&current_clock) {
            *counter += 1;
        } else {
            min_heap.insert(current_clock, 1);
        }
        Self::Tracker {
            clock: current_clock,
        }
    }
    fn forge(&self, tracker: &Self::Tracker) -> Option<Self::Tracker> {
        if let Ok(mut min_heap) = self.min_heap.lock() {
            if let Some(counter) = min_heap.get_mut(&tracker.derive()) {
                *counter += 1;
                return Some(Self::Tracker {
                    clock: tracker.clock,
                });
            }
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

pub struct AtomicCounterTracker {
    clock: usize,
}

impl DeriveClock<usize> for AtomicCounterTracker {
    fn derive(&self) -> usize {
        self.clock
    }
}
