// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use criterion::{criterion_group, criterion_main, Criterion};
use sap_tsf::sequencer::ToInstant;
use sap_tsf::{AtomicCounter, Sequencer};
use std::sync::atomic::Ordering::Relaxed;

fn track_empty(c: &mut Criterion) {
    let ac = AtomicCounter::default();
    c.bench_function("AtomicCounter: empty", |b| {
        b.iter(|| {
            let tracker = ac.track(Relaxed);
            assert_eq!(tracker.to_instant(), 1);
        });
    });
}

fn track_non_empty(c: &mut Criterion) {
    let ac = AtomicCounter::default();
    let tracker = ac.track(Relaxed);
    assert_eq!(tracker.to_instant(), 1);
    c.bench_function("AtomicCounter: non-empty", |b| {
        b.iter(|| {
            let tracker = ac.track(Relaxed);
            assert_eq!(tracker.to_instant(), 1);
        });
    });
}

criterion_group!(atomic_counter, track_empty, track_non_empty);
criterion_main!(atomic_counter);
