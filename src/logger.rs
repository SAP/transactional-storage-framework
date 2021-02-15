// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Sequencer, Transaction};
use std::time::Duration;

/// Log stores the data that is to be persisted.
pub struct Log {
    log_data: Vec<u8>,
    undo_data: Vec<u8>,
    undo_hook: Option<Box<dyn FnOnce((&Vec<u8>, &Vec<u8>))>>,
}

/// The Logger trait defines logging interfaces.
pub trait Logger<S: Sequencer> {
    /// Persists the given data.
    ///
    /// It returns the start and end log sequence number pair of the persisted data.
    fn persist(&self, log: &Log, transaction: &Transaction<S>) -> Result<(usize, usize), Error>;

    /// Flushes pending log records into the persistent storage.
    ///
    /// It returns the max flushed log sequence number.
    fn flush(&self, position: usize, timeout: Duration) -> Result<usize, Error>;
}
