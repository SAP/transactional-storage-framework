// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::log_record::LogRecord;
use super::FileIOData;
use crate::transaction::Playback;
use crate::{Database, Error, FileIO, JournalID, Sequencer, TransactionID};
use std::num::NonZeroU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::task::Waker;

/// Collection of data for database recovery.
#[derive(Debug)]
pub(super) struct RecoveryData<S: Sequencer<Instant = u64>> {
    /// Database to recover.
    database: Option<Database<S, FileIO<S>>>,

    /// Recover until the logical time point.
    #[allow(dead_code)]
    until: Option<u64>,

    /// [`Waker`] associated with the database owner.
    waker: Option<Waker>,

    /// Fully recovered database is sent through `result`.
    result: Option<Result<Database<S, FileIO<S>>, Error>>,
}

/// Keeps identifiers of the most recently used journal.
struct MostRecentJournal {
    transaction_id: TransactionID,
    journal_id: JournalID,
}

// No log entries are wider than 32 bytes.
const BUFFER_SIZE: usize = 32;

impl<S: Sequencer<Instant = u64>> RecoveryData<S> {
    /// Creates a new [`RecoveryData`].
    pub(super) fn new(database: Database<S, FileIO<S>>, until: Option<u64>) -> Self {
        Self {
            database: Some(database),
            until,
            waker: None,
            result: None,
        }
    }

    /// Returns the result of recovery.
    pub(super) fn get_result(&mut self) -> Option<Result<Database<S, FileIO<S>>, Error>> {
        self.result.take()
    }

    /// Sets a [`Waker`].
    pub(super) fn set_waker(&mut self, waker: Waker) {
        self.waker.replace(waker);
    }

    /// Cancels the recovery task.
    pub(super) fn cancel(&mut self) {
        self.result.replace(Err(Error::Timeout));
    }
}

pub(super) fn recover_database<S: Sequencer<Instant = u64>>(file_io_data: &FileIOData<S>) {
    let mut guard = file_io_data.recovery_data.lock().unwrap();
    let database = guard.as_mut().unwrap().database.take().unwrap();
    let playback_container: scc::HashMap<TransactionID, Playback<S, FileIO<S>>> =
        scc::HashMap::default();
    drop(guard);

    let file_len = file_io_data.log.len(Acquire);

    // The variable is only updated when the journal creates or deletes a database objects.
    let mut last_journal_anchor: Option<MostRecentJournal> = None;

    let mut read_offset = 0;
    let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
    while file_io_data.log.read(&mut buffer, read_offset).is_ok() {
        if file_io_data.recovery_cancelled.load(Relaxed) {
            // Canceled.
            return;
        }
        if let Some(bytes_read) = apply_to_database(
            &buffer,
            &database,
            &playback_container,
            &mut last_journal_anchor,
        ) {
            read_offset += bytes_read;
        } else {
            read_offset = file_len;
            break;
        }
    }
    if read_offset < file_len && file_len - read_offset < BUFFER_SIZE as u64 {
        // More to read in the log file.
        #[allow(clippy::cast_possible_truncation)]
        let buffer_piece = &mut buffer[0..(file_len - read_offset) as usize];
        if file_io_data.log.read(buffer_piece, read_offset).is_ok() {
            if file_io_data.recovery_cancelled.load(Relaxed) {
                // Canceled.
                return;
            }
            if let Some(bytes_read) = apply_to_database(
                buffer_piece,
                &database,
                &playback_container,
                &mut last_journal_anchor,
            ) {
                read_offset += bytes_read;
            } else {
                read_offset = file_len;
            }
        }
    }

    if !playback_container.is_empty() {
        // TODO: cleanup open transactions.
        playback_container.clear();
    }
    drop(playback_container);

    let mut guard = file_io_data.recovery_data.lock().unwrap();
    if guard.as_ref().unwrap().result.is_some() {
        // Canceled.
        return;
    }
    let recovery_data = guard.as_mut().unwrap();
    if read_offset == file_len {
        recovery_data.result.replace(Ok(database));
    } else {
        recovery_data
            .result
            .replace(Err(Error::IO(std::io::ErrorKind::InvalidData)));
    }
    if let Some(waker) = recovery_data.waker.take() {
        waker.wake();
    }
}

/// Applies log records in the buffer to the database.
///
/// Returns `None` if it read the end of the log file.
#[allow(clippy::too_many_lines)]
fn apply_to_database<'d, S: Sequencer<Instant = u64>>(
    mut buffer: &[u8],
    database: &'d Database<S, FileIO<S>>,
    playback_container: &scc::HashMap<TransactionID, Playback<'d, S, FileIO<S>>>,
    last_journal_anchor: &mut Option<MostRecentJournal>,
) -> Option<u64> {
    // TODO: parallelize.
    let buffer_len = buffer.len();
    while !buffer.is_empty() {
        if let Some((log_record, remaining)) = LogRecord::<S>::from_raw_data(buffer) {
            buffer = remaining;
            match log_record {
                LogRecord::EndOfLog => {
                    return None;
                }
                LogRecord::BufferSubmitted(submit_instant) => {
                    let last_journal_anchor = last_journal_anchor.take().unwrap();
                    let mut playback_entry = playback_container
                        .get(&last_journal_anchor.transaction_id)
                        .unwrap();
                    playback_entry
                        .get_mut()
                        .submit_journal_anchor(last_journal_anchor.journal_id, submit_instant);
                }
                LogRecord::BufferDiscarded => {
                    let last_journal_anchor = last_journal_anchor.take().unwrap();
                    let mut playback_entry = playback_container
                        .get(&last_journal_anchor.transaction_id)
                        .unwrap();
                    playback_entry
                        .get_mut()
                        .discard_journal_anchor(last_journal_anchor.journal_id);
                }
                LogRecord::JournalCreatedObjectSingle(transaction_id, journal_id, object_id) => {
                    let mut playback_entry = playback_container
                        .entry(transaction_id)
                        .or_insert_with(|| Playback::new(database));
                    let journal_anchor = playback_entry
                        .get_mut()
                        .get_or_create_journal_anchor(journal_id);
                    database
                        .access_controller()
                        .playback_create_sync(object_id, &journal_anchor);
                    last_journal_anchor.replace(MostRecentJournal {
                        transaction_id,
                        journal_id,
                    });
                }
                LogRecord::JournalCreatedObjectRange(
                    transaction_id,
                    journal_id,
                    start_object_id,
                    interval,
                    num_objects,
                ) => {
                    let mut playback_entry = playback_container
                        .entry(transaction_id)
                        .or_insert_with(|| Playback::new(database));
                    let journal_anchor = playback_entry
                        .get_mut()
                        .get_or_create_journal_anchor(journal_id);
                    (0..num_objects).for_each(|i| {
                        let object_id = start_object_id + u64::from(i) * u64::from(interval);
                        database
                            .access_controller()
                            .playback_create_sync(object_id, &journal_anchor);
                    });
                    last_journal_anchor.replace(MostRecentJournal {
                        transaction_id,
                        journal_id,
                    });
                }
                LogRecord::JournalDeletedObjectSingle(transaction_id, journal_id, object_id) => {
                    let mut playback_entry = playback_container
                        .entry(transaction_id)
                        .or_insert_with(|| Playback::new(database));
                    let journal_anchor = playback_entry
                        .get_mut()
                        .get_or_create_journal_anchor(journal_id);
                    database
                        .access_controller()
                        .playback_delete_sync(object_id, &journal_anchor);
                    last_journal_anchor.replace(MostRecentJournal {
                        transaction_id,
                        journal_id,
                    });
                }
                LogRecord::JournalDeletedObjectRange(
                    transaction_id,
                    journal_id,
                    start_object_id,
                    interval,
                    num_objects,
                ) => {
                    let mut playback_entry = playback_container
                        .entry(transaction_id)
                        .or_insert_with(|| Playback::new(database));
                    let journal_anchor = playback_entry
                        .get_mut()
                        .get_or_create_journal_anchor(journal_id);
                    (0..num_objects).for_each(|i| {
                        let object_id = start_object_id + u64::from(i) * u64::from(interval);
                        database
                            .access_controller()
                            .playback_delete_sync(object_id, &journal_anchor);
                    });
                    last_journal_anchor.replace(MostRecentJournal {
                        transaction_id,
                        journal_id,
                    });
                }
                LogRecord::JournalSubmitted(transaction_id, journal_id, submit_instant) => {
                    let mut playback_entry = playback_container.get(&transaction_id).unwrap();
                    playback_entry
                        .get_mut()
                        .submit_journal_anchor(journal_id, submit_instant);
                }
                LogRecord::JournalDiscarded(transaction_id, journal_id) => {
                    let mut playback_entry = playback_container.get(&transaction_id).unwrap();
                    playback_entry.get_mut().discard_journal_anchor(journal_id);
                }
                LogRecord::TransactionPrepared(transaction_id, prepare_instant) => {
                    let mut playback_entry = playback_container
                        .entry(transaction_id)
                        .or_insert_with(|| Playback::new(database));
                    playback_entry.get_mut().prepare(prepare_instant);
                }
                LogRecord::TransactionCommitted(transaction_id, commit_instant) => {
                    let playback_entry = playback_container
                        .entry(transaction_id)
                        .or_insert_with(|| Playback::new(database));
                    playback_entry.remove().commit(commit_instant);
                }
                LogRecord::TransactionRolledBack(transaction_id, rollback_to) => {
                    if let Some(mut playback_entry) = playback_container.get(&transaction_id) {
                        if rollback_to == 0 {
                            playback_entry.remove().rollback();
                        } else {
                            playback_entry
                                .get_mut()
                                .rewind(NonZeroU32::new(rollback_to));
                        }
                    }
                }
            }
        } else {
            // The length of the buffer piece was insufficient.
            debug_assert_ne!(buffer.len(), BUFFER_SIZE);
            debug_assert_eq!(buffer.len() % 4, 0);
            return Some((buffer_len - buffer.len()) as u64);
        }
    }
    Some(buffer_len as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use std::path::Path;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;
    use tokio::fs::remove_dir_all;

    #[tokio::test]
    async fn basic() {
        const DIR: &str = "recovery_basic_test";
        let path = Path::new(DIR);
        let database = Arc::new(Database::with_path(path).await.unwrap());

        let transaction = database.transaction();
        let mut journal = transaction.journal();
        for o in 0..16 {
            journal.create(&[o], None).await.unwrap();
        }
        assert_eq!(Some(journal.submit()), NonZeroU32::new(1));
        let mut journal = transaction.journal();
        for o in 16..32 {
            journal.delete(&[o], None).await.unwrap();
        }
        assert_eq!(Some(journal.submit()), NonZeroU32::new(2));
        assert!(transaction.commit().await.is_ok());

        let transaction = database.transaction();
        let mut journal = transaction.journal();
        journal
            .delete((32..64).collect::<Vec<u64>>().as_slice(), None)
            .await
            .unwrap();
        assert_eq!(Some(journal.submit()), NonZeroU32::new(1));
        assert!(transaction.commit().await.is_ok());

        let transaction = database.transaction();
        let mut journal = transaction.journal();
        journal
            .create((64..96).collect::<Vec<u64>>().as_slice(), None)
            .await
            .unwrap();
        drop(journal);
        transaction.rollback();

        let instant = database.sequencer().now(Relaxed);
        drop(database);

        let database_recovered = Arc::new(Database::with_path(path).await.unwrap());
        let recovered_instant = database_recovered.sequencer().now(Relaxed);
        assert_eq!(instant, recovered_instant);

        let snapshot = database_recovered.snapshot();
        for o in 0..16 {
            assert_eq!(
                database_recovered
                    .access_controller()
                    .read(o, &snapshot, None)
                    .await,
                Ok(true)
            );
        }
        for o in 16..64 {
            assert_eq!(
                database_recovered
                    .access_controller()
                    .read(o, &snapshot, None)
                    .await,
                Ok(false)
            );
        }
        for o in 64..96 {
            assert_eq!(
                database_recovered
                    .access_controller()
                    .read(o, &snapshot, None)
                    .await,
                Ok(false)
            );
        }
        for o in 96..128 {
            assert_eq!(
                database_recovered
                    .access_controller()
                    .read(o, &snapshot, None)
                    .await,
                Ok(true)
            );
        }

        let transaction = database_recovered.transaction();
        let mut journal = transaction.journal();
        journal
            .create((96..129).collect::<Vec<u64>>().as_slice(), None)
            .await
            .unwrap();
        assert_eq!(Some(journal.submit()), NonZeroU32::new(1));
        assert!(transaction.commit().await.is_ok());

        let instant = database_recovered.sequencer().now(Relaxed);

        drop(snapshot);
        drop(database_recovered);

        let database_recovered_again = Arc::new(Database::with_path(path).await.unwrap());
        let recovered_instant = database_recovered_again.sequencer().now(Relaxed);
        assert_eq!(instant, recovered_instant);

        assert!(remove_dir_all(path).await.is_ok());
    }
}
