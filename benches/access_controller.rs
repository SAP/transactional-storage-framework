use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use sap_tsf::{AtomicCounter, Database, ToObjectID, VolatileDevice};
use std::sync::Arc;

struct O(usize);
impl ToObjectID for O {
    fn to_object_id(&self) -> usize {
        self.0
    }
}

async fn create_check(
    size: usize,
    database: Arc<Database<AtomicCounter, VolatileDevice<AtomicCounter>>>,
) {
    let access_controller = database.access_controller();
    let transaction = database.transaction();
    let mut journal = transaction.journal();
    for o in 0..size {
        assert!(access_controller
            .create(&O(o), &mut journal, None)
            .await
            .is_ok());
    }
}

fn create(c: &mut Criterion) {
    let database = Arc::new(Database::default());
    let size: usize = 1024;
    c.bench_with_input(
        BenchmarkId::new("AccessController: create", size),
        &size,
        |b, &s| {
            // Insert a call to `to_async` to convert the bencher to async mode.
            // The timing loops are the same as with the normal bencher.
            b.to_async(FuturesExecutor)
                .iter(|| create_check(s, database.clone()));
        },
    );
}

criterion_group!(access_controller, create);
criterion_main!(access_controller);
