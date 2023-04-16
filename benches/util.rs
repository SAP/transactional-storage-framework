// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use criterion::{criterion_group, criterion_main, Criterion};
use sap_tsf::util;
use std::thread;

fn std_thread_id(c: &mut Criterion) {
    let current_thread_id = thread::current().id();
    c.bench_function("std: thread::id", |b| {
        b.iter(|| {
            assert_eq!(current_thread_id, thread::current().id());
        });
    });
}

fn thread_id(c: &mut Criterion) {
    let current_thread_id = thread::current().id();
    c.bench_function("util: thread_id", |b| {
        b.iter(|| {
            assert_eq!(current_thread_id, util::thread_id());
        });
    });
}

criterion_group!(util, std_thread_id, thread_id);
criterion_main!(util);
