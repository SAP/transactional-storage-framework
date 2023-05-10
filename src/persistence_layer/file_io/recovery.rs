// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::log_record::LogRecord;
use super::FileIOData;
use crate::transaction::Playback;
use crate::{Database, Error, FileIO, Sequencer, TransactionID};
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

    let file_len = file_io_data.log0.len(Acquire);
    let mut read_offset = 0;
    let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
    while file_io_data.log0.read(&mut buffer, read_offset).is_ok() {
        if file_io_data.recovery_cancelled.load(Relaxed) {
            // Canceled.
            return;
        }
        if let Some(bytes_read) = apply_to_database(&buffer, &database, &playback_container) {
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
        if file_io_data.log0.read(buffer_piece, read_offset).is_ok() {
            if file_io_data.recovery_cancelled.load(Relaxed) {
                // Canceled.
                return;
            }
            if let Some(bytes_read) = apply_to_database(&buffer, &database, &playback_container) {
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
fn apply_to_database<'d, S: Sequencer<Instant = u64>>(
    mut buffer: &[u8],
    database: &'d Database<S, FileIO<S>>,
    playback_container: &scc::HashMap<TransactionID, Playback<'d, S, FileIO<S>>>,
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
                LogRecord::BufferSubmitted(_) => todo!(),
                LogRecord::BufferDiscarded => todo!(),
                LogRecord::JournalCreatedObjectSingle(_, _, _) => todo!(),
                LogRecord::JournalCreatedObjectRange(_, _, _, _) => todo!(),
                LogRecord::JournalDeletedObjectSingle(_, _, _) => todo!(),
                LogRecord::JournalDeletedObjectRange(_, _, _, _) => todo!(),
                LogRecord::JournalSubmitted(_, _, _) => todo!(),
                LogRecord::JournalDiscarded(_, _) => todo!(),
                LogRecord::TransactionPrepared(_, _) => todo!(),
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
            debug_assert_eq!(buffer.len() / 8, 0);
            return Some((buffer_len - buffer.len()) as u64);
        }
    }
    Some(buffer_len as u64)
}
