// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{log_record::LogRecord, FileIOData};
use crate::{Database, Error, FileIO, Sequencer};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::task::Waker;

/// Collection of data for database recovery.
#[derive(Debug)]
pub(super) struct RecoveryData<S: Sequencer<Instant = u64>> {
    /// Database to recover.
    database: Database<S, FileIO<S>>,

    /// Recover until the logical time point.
    #[allow(dead_code)]
    until: Option<u64>,

    /// [`Waker`] associated with the database owner.
    waker: Option<Waker>,

    /// Completion flag.
    result: Option<Result<(), Error>>,
}

// No log entries are wider than 32 bytes.
const BUFFER_SIZE: usize = 32;

impl<S: Sequencer<Instant = u64>> RecoveryData<S> {
    /// Creates a new [`RecoveryData`].
    pub(super) fn new(database: Database<S, FileIO<S>>, until: Option<u64>) -> Self {
        Self {
            database,
            until,
            waker: None,
            result: None,
        }
    }

    /// Returns the result of recovery.
    pub(super) fn get_result(&mut self) -> Option<Result<(), Error>> {
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

    /// Takes [`Database`] by consuming `self`.
    pub(super) fn take(self) -> Database<S, FileIO<S>> {
        self.database
    }
}

pub(super) fn recover_database<S: Sequencer<Instant = u64>>(file_io_data: &FileIOData<S>) {
    let file_len = file_io_data.log0.len(Acquire);
    let mut read_offset = 0;
    let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
    while file_io_data.log0.read(&mut buffer, read_offset).is_ok() {
        let mut guard = file_io_data.recovery_data.lock().unwrap();
        if guard.as_ref().unwrap().result.is_some() {
            // Canceled.
            return;
        }
        let database = &guard.as_mut().unwrap().database;
        if let Some(bytes_read) = apply_to_database(&buffer, database) {
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
            let mut guard = file_io_data.recovery_data.lock().unwrap();
            if guard.as_ref().unwrap().result.is_some() {
                // Canceled.
                return;
            }
            let database = &guard.as_mut().unwrap().database;
            if let Some(bytes_read) = apply_to_database(&buffer, database) {
                read_offset += bytes_read;
            } else {
                read_offset = file_len;
            }
        }
    }

    let mut guard = file_io_data.recovery_data.lock().unwrap();
    if guard.as_ref().unwrap().result.is_some() {
        // Canceled.
        return;
    }
    let recovery_data = guard.as_mut().unwrap();
    if read_offset == file_len {
        recovery_data.result.replace(Ok(()));
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
fn apply_to_database<S: Sequencer<Instant = u64>>(
    mut buffer: &[u8],
    database: &Database<S, FileIO<S>>,
) -> Option<u64> {
    let buffer_len = buffer.len();
    while !buffer.is_empty() {
        if let Some((log_record, remaining)) = LogRecord::<S>::from_raw_data(buffer) {
            buffer = remaining;
            match log_record {
                LogRecord::EndOfLog => {
                    return None;
                }
                LogRecord::BufferCommitted => todo!(),
                LogRecord::BufferRolledBack => todo!(),
                LogRecord::Created(_, _, _) => todo!(),
                LogRecord::CreatedTwo(_, _, _, _) => todo!(),
                LogRecord::Deleted(_, _, _) => todo!(),
                LogRecord::DeletedTwo(_, _, _, _) => todo!(),
                LogRecord::Prepared(_, instant) | LogRecord::Committed(_, instant) => {
                    // TODO: instantiate recovery transactions.
                    let _: Result<u64, u64> = database.sequencer().update(instant, Release);
                }
                LogRecord::RolledBack(_, _rollback_to) => todo!(),
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
