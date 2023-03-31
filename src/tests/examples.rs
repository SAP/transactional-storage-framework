// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use crate::sequencer::AtomicCounter;
    use crate::Database;

    use std::sync::Arc;

    #[tokio::test]
    async fn single_threaded() {
        let database = Database::default();
        let storage_snapshot = database.snapshot();
        let transaction = database.transaction();
        let transaction_snapshot = transaction.snapshot();
        let journal = transaction.start();
        drop(journal);
        drop(transaction_snapshot);
        drop(transaction);
        drop(storage_snapshot);
        drop(database);
        /*
        assert!(storage
            .create_directory(
                "/thomas/eats/apples",
                &transaction_snapshot,
                &mut journal,
                None
            )
            .is_ok());

        // journal_snapshot includes changes pending in the journal.
        let journal_snapshot = journal.snapshot();
        assert!(storage
            .get("/thomas/eats/apples", &journal_snapshot)
            .is_some());
        drop(journal_snapshot);
        assert_eq!(journal.submit(), 1);

        // storage_snapshot had been taken before the transaction started.
        assert!(storage
            .get("/thomas/eats/apples", &storage_snapshot)
            .is_none());
        // transaction_snapshot had been taken before the journal started.
        assert!(storage
            .get("/thomas/eats/apples", &transaction_snapshot)
            .is_none());

        let storage_snapshot = storage.snapshot();

        drop(transaction_snapshot);
        assert!(transaction.commit().is_ok());

        // storage_snapshot had been taken before the transaction was committed.
        assert!(storage
            .get("/thomas/eats/apples", &storage_snapshot)
            .is_none());

        let storage_snapshot = storage.snapshot();

        // storage_snapshot was taken after the transaction had been committed.
        assert!(storage
            .get("/thomas/eats/apples", &storage_snapshot)
            .is_some());
        */
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn multi_threaded() {
        let storage: Arc<Database<AtomicCounter>> = Arc::new(Database::default());
        drop(storage);
        /*(
        let num_threads = 8;
        let mut thread_handles = Vec::with_capacity(num_threads);
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        for _ in 0..num_threads {
            let barrier_cloned = barrier.clone();
            let storage_cloned = storage.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_cloned.wait();
                let transaction = storage_cloned.transaction();
                let transaction_snapshot = transaction.snapshot();
                let mut journal = transaction.start();
                assert!(storage_cloned
                    .create_directory(
                        "/thomas/eats/apples",
                        &transaction_snapshot,
                        &mut journal,
                        None
                    )
                    .is_err());
                assert!(journal.submit() > 0);
                barrier_cloned.wait();
            }));
        }

        let transaction = storage.transaction();
        let transaction_snapshot = transaction.snapshot();
        let mut journal = transaction.start();
        assert!(storage
            .create_directory(
                "/thomas/eats/apples",
                &transaction_snapshot,
                &mut journal,
                None
            )
            .is_ok());
        assert!(journal.submit() > 0);
        barrier.wait();
        barrier.wait();

        for handle in thread_handles {
            handle.join().unwrap();
        }
        */
    }
}
