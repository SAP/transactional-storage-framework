// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use sap_tsf::Database;
use std::fs::remove_dir_all;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn create_check(size: u64, iters: u64) -> Duration {
    let path = Path::new("bench_access_controller_create");
    let database = Arc::new(Database::with_path(path).await.unwrap());
    let access_controller = database.access_controller();
    let transaction = database.transaction();
    let mut journal = transaction.journal();
    let start = Instant::now();
    for _ in 0..iters {
        for o in 0..size {
            assert!(access_controller
                .create(o, &mut journal, None)
                .await
                .is_ok());
        }
    }
    let elapsed = start.elapsed();
    drop(journal);
    drop(transaction);
    drop(database);
    assert!(remove_dir_all(path).is_ok());
    elapsed
}

fn create(c: &mut Criterion) {
    let size: u64 = 64;
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
