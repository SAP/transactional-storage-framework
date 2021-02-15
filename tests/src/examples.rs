// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod examples {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tss::{DefaultSequencer, Snapshot, Storage, Transaction};

    #[test]
    fn single_threaded() {
        let storage: Storage<DefaultSequencer> = Storage::new(String::from("farm"));
        let transaction = storage.transaction();
        let result = storage.create_directory(&String::from("/thomas/eats/apples"), &transaction);
        assert!(result.is_ok());
    }

    #[test]
    fn multi_threaded() {
        let storage: Arc<Storage<DefaultSequencer>> = Arc::new(Storage::new(String::from("farm")));
        let num_threads = 8;
        let mut thread_handles = Vec::with_capacity(num_threads);
        let barrier = Arc::new(Barrier::new(num_threads));
        for thread_id in 0..num_threads {
            let barrier_copied = barrier.clone();
            let storage_copied = storage.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
    }
}
