// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Sequencer, Transaction};
use std::time::Duration;

/// Log stores the data that is to be persisted.
pub struct Log {
    /// The data stored in the vector is persisted.
    log: Vec<u8>,
    /// undo_hook is invoked when the transaction is rewound or rolled back.
    ///
    /// The undo data can be stored separately.
    undo_hook: Option<(Vec<u8>, Box<dyn FnOnce(&Vec<u8>)>)>,
}

impl Log {
    /// Creates a new Log.
    pub fn new() -> Log {
        Log {
            log: Vec::new(),
            undo_hook: None,
        }
    }

    /// Invokes the undo hook.
    pub fn undo(mut self) {
        if let Some((undo_data, undo_hook)) = self.undo_hook.take() {
            undo_hook(&undo_data);
        }
    }
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
