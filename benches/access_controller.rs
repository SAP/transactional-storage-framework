// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use sap_tsf::{Database, ToObjectID};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct O(usize);
impl ToObjectID for O {
    fn to_object_id(&self) -> usize {
        self.0
    }
}

async fn create_check(size: usize, iters: u64) -> Duration {
    let database = Arc::new(
        Database::with_path(Path::new("bench_access_controller_create"))
            .await
            .unwrap(),
    );
    let access_controller = database.access_controller();
    let transaction = database.transaction();
    let mut journal = transaction.journal();
    let start = Instant::now();
    for _ in 0..iters {
        for o in 0..size {
            assert!(access_controller
                .create(&O(o), &mut journal, None)
                .await
                .is_ok());
        }
    }
    start.elapsed()
}

fn create(c: &mut Criterion) {
    let size: usize = 64;
    c.bench_with_input(
        BenchmarkId::new("AccessController: create", size),
        &size,
        |b, &s| {
            b.to_async(FuturesExecutor)
                .iter_custom(|iters| create_check(s, iters));
        },
    );
}

criterion_group!(access_controller, create);
criterion_main!(access_controller);
