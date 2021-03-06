// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod examples {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tss::{DefaultSequencer, Storage};

    #[test]
    fn single_threaded() {
        let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
        let storage_snapshot = storage.snapshot();

        let transaction = storage.transaction();
        let transaction_snapshot = transaction.snapshot();
        let mut journal = transaction.start();
        assert!(storage
            .create_directory("/thomas/eats/apples", &transaction_snapshot, &mut journal)
            .is_ok());

        // journal_snapshot includes changes pending in the journal.
        let journal_snapshot = journal.snapshot();
        assert!(storage
            .get("/thomas/eats/apples", &journal_snapshot)
            .is_some());
        drop(journal_snapshot);
        journal.submit();

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
    }

    #[test]
    fn multi_threaded() {
        let storage: Arc<Storage<DefaultSequencer>> = Arc::new(Storage::new(String::from("db")));
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
                    .create_directory("/thomas/eats/apples", &transaction_snapshot, &mut journal)
                    .is_err());
                journal.submit();
                barrier_cloned.wait();
            }));
        }

        let transaction = storage.transaction();
        let transaction_snapshot = transaction.snapshot();
        let mut journal = transaction.start();
        assert!(storage
            .create_directory("/thomas/eats/apples", &transaction_snapshot, &mut journal)
            .is_ok());
        journal.submit();
        barrier.wait();
        barrier.wait();

        for handle in thread_handles {
            handle.join().unwrap();
        }
    }
}
