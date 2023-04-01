// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use crate::Database;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn single_threaded() {
        let database = Database::default();
        let storage_snapshot = database.snapshot();
        let transaction = database.transaction();
        let transaction_snapshot = transaction.snapshot();
        let journal = transaction.journal();
        drop(journal);
        drop(transaction_snapshot);
        drop(transaction);
        drop(storage_snapshot);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn multi_threaded() {
        let num_tasks = 16;
        let barrier = Arc::new(Barrier::new(num_tasks));
        let database = Arc::new(Database::default());
        let mut task_handles = Vec::with_capacity(num_tasks);
        for _ in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let database_clone = database.clone();
            task_handles.push(tokio::spawn(async move {
                barrier_clone.wait().await;
                let storage_snapshot = database_clone.snapshot();
                let transaction = database_clone.transaction();
                let transaction_snapshot = transaction.snapshot();
                let journal = transaction.journal();
                drop(journal);
                drop(transaction_snapshot);
                drop(transaction);
                drop(storage_snapshot);
            }));
        }
        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }
    }
}
