// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use criterion::{criterion_group, criterion_main, Criterion};
use sap_tsf::utils;
use std::thread;

fn std_thread_id(c: &mut Criterion) {
    let current_thread_id = thread::current().id();
    c.bench_function("std: thread::id", |b| {
        b.iter(|| {
            assert_eq!(current_thread_id, thread::current().id());
        });
    });
}

fn shard_id(c: &mut Criterion) {
    let current_shard_id = utils::shard_id();
    c.bench_function("utils: shard_id", |b| {
        b.iter(|| {
            assert_eq!(current_shard_id, utils::shard_id());
        });
    });
}

fn thread_id(c: &mut Criterion) {
    let current_thread_id = thread::current().id();
    c.bench_function("utils: thread_id", |b| {
        b.iter(|| {
            assert_eq!(current_thread_id, utils::thread_id());
        });
    });
}

criterion_group!(utils, std_thread_id, shard_id, thread_id);
criterion_main!(utils);
