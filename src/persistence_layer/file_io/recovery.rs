// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{log_record::LogRecord, FileIOData};
use crate::{Database, Error, FileIO, Sequencer};
use std::sync::atomic::Ordering::Release;
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
    let mut read_offset = 0;
    let mut buffer: [u8; 32] = [0; 32];
    while file_io_data.log0.read(&mut buffer, read_offset).is_ok() {
        read_offset += 32;
        let mut buffer_piece: &[u8] = &buffer;
        while !buffer_piece.is_empty() {
            if let Some((log_record, remaining)) = LogRecord::<S>::from_raw_data(buffer_piece) {
                buffer_piece = remaining;
                match log_record {
                    LogRecord::Prepared(_, instant) | LogRecord::Committed(_, instant) => {
                        if let Ok(mut recovery_data) = file_io_data.recovery_data.lock() {
                            let result = recovery_data
                                .as_mut()
                                .unwrap()
                                .database
                                .sequencer()
                                .update(instant, Release);
                            debug_assert!(result.is_ok());
                        }
                    }
                    LogRecord::RolledBack(_, _rollback_to) => todo!(),
                }
            }
        }
    }

    if let Ok(mut recovery_data) = file_io_data.recovery_data.lock() {
        recovery_data.as_mut().unwrap().result.replace(Ok(()));
        if let Some(waker) = recovery_data.as_mut().unwrap().waker.take() {
            waker.wake();
        }
    }
}
