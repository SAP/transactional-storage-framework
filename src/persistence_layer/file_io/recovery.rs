// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{log_record::LogRecord, FileIOData};
use crate::{Database, Error, FileIO, Sequencer};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::task::Waker;

/// Collection of data for database recovery.
#[derive(Debug)]
pub(super) struct RecoveryData<S: Sequencer> {
    /// Database to recover.
    database: Database<S, FileIO<S>>,

    /// Recover until the logical time point.
    #[allow(dead_code)]
    until: Option<S::Instant>,

    /// [`Waker`] associated with the database owner.
    waker: Option<Waker>,

    /// Completion flag.
    result: Option<Result<(), Error>>,
}

// No log entries are wider than 32 bytes.
const BUFFER_SIZE: usize = 32;

impl<S: Sequencer> RecoveryData<S> {
    /// Creates a new [`RecoveryData`].
    pub(super) fn new(database: Database<S, FileIO<S>>, until: Option<S::Instant>) -> Self {
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

    /// Takes [`Database`] by consuming `self`.
    pub(super) fn take(self) -> Database<S, FileIO<S>> {
        self.database
    }
}

pub(super) fn recover_database<S: Sequencer>(file_io_data: &FileIOData<S>) {
    let file_len = file_io_data.log0.len(Acquire);
    let mut read_offset = 0;
    let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
    while file_io_data.log0.read(&mut buffer, read_offset).is_ok() {
        let mut guard = file_io_data.recovery_data.lock().unwrap();
        let database = &guard.as_mut().unwrap().database;
        read_offset += apply_to_database(&buffer, database);
    }
    if read_offset < file_len && file_len - read_offset < BUFFER_SIZE as u64 {
        // More to read in the log file.
        #[allow(clippy::cast_possible_truncation)]
        let buffer_piece = &mut buffer[0..(file_len - read_offset) as usize];
        if file_io_data.log0.read(buffer_piece, read_offset).is_ok() {
            let mut guard = file_io_data.recovery_data.lock().unwrap();
            let database = &guard.as_mut().unwrap().database;
            read_offset += apply_to_database(&buffer, database);
        }
    }

    let mut guard = file_io_data.recovery_data.lock().unwrap();
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
fn apply_to_database<S: Sequencer>(mut buffer: &[u8], database: &Database<S, FileIO<S>>) -> u64 {
    let buffer_len = buffer.len();
    while !buffer.is_empty() {
        if let Some((log_record, remaining)) = LogRecord::<S>::from_raw_data(buffer) {
            buffer = remaining;
            match log_record {
                LogRecord::Prepared(_, instant) | LogRecord::Committed(_, instant) => {
                    // TODO: instantiate recovery transactions.
                    let result = database.sequencer().update(instant, Release);
                    debug_assert!(result.is_ok());
                }
                LogRecord::RolledBack(_, _rollback_to) => todo!(),
            }
        } else {
            // The length of the buffer piece was insufficient.
            debug_assert_ne!(buffer.len(), BUFFER_SIZE);
            debug_assert_eq!(buffer.len() / 8, 0);
            return (buffer_len - buffer.len()) as u64;
        }
    }
    buffer_len as u64
}
