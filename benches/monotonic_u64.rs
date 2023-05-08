// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use criterion::{criterion_group, criterion_main, Criterion};
use sap_tsf::sequencer::ToInstant;
use sap_tsf::{MonotonicU64, Sequencer};
use std::sync::atomic::Ordering::Relaxed;

fn track_empty(c: &mut Criterion) {
    let mu = MonotonicU64::default();
    c.bench_function("MonotonicU64: empty", |b| {
        b.iter(|| {
            let tracker = mu.track(Relaxed);
            assert_eq!(tracker.to_instant(), 1);
        });
    });
}

fn track_non_empty(c: &mut Criterion) {
    let mu = MonotonicU64::default();
    let tracker = mu.track(Relaxed);
    assert_eq!(tracker.to_instant(), 1);
    c.bench_function("MonotonicU64: non-empty", |b| {
        b.iter(|| {
            let tracker = mu.track(Relaxed);
            assert_eq!(tracker.to_instant(), 1);
        });
    });
}

criterion_group!(monotonic_u64, track_empty, track_non_empty);
criterion_main!(monotonic_u64);
