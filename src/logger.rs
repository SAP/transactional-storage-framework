// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::Error;

/// LogState denotes the location of the log data.
pub enum LogState {
    /// The log data is in memory.
    Memory(Vec<u8>),
    /// The log data is passed to a Logger.
    ///
    /// The usize pair is the start and end positions in the log buffer.
    Pending(usize, usize),
    /// The log data is fully persisted.
    ///
    /// The max known persisted log position.
    Persisted(usize),
}

/// Log stores the data that is to be persisted.
pub struct Log {
    /// The data stored in the vector is persisted.
    ///
    /// log being None indicates that the Log instance is invalid.
    log: Option<LogState>,
    /// undo_hook is invoked when the transaction is rewound or rolled back.
    ///
    /// The undo data can be stored separately.
    undo_hook: Option<(Vec<u8>, Box<dyn FnOnce(&Vec<u8>)>)>,
}

impl Log {
    /// Creates a new Log.
    pub fn new() -> Log {
        Log {
            log: Some(LogState::Memory(Vec::new())),
            undo_hook: None,
        }
    }

    /// Passes the log data to the logger.
    pub fn submit<L: Logger>(&mut self, logger: &L) -> Result<(), Error> {
        if let Some(LogState::Memory(data)) = self.log.take() {
            match logger.submit(data) {
                Ok((start_position, end_position)) => {
                    self.log
                        .replace(LogState::Pending(start_position, end_position));
                    return Ok(());
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
        Err(Error::Fail)
    }

    /// Makes sure that the pending log is persisted.
    pub fn persist<L: Logger>(&mut self, logger: &L) -> Result<(), Error> {
        if let Some(LogState::Pending(_, end_position)) = self.log.take() {
            match logger.persist(end_position) {
                Ok(max_persisted_position) => {
                    self.log
                        .replace(LogState::Persisted(max_persisted_position));
                    return Ok(());
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
        Err(Error::Fail)
    }

    /// Invokes the undo hook.
    pub fn undo(mut self) {
        if let Some((undo_data, undo_hook)) = self.undo_hook.take() {
            undo_hook(&undo_data);
        }
    }
}

/// The Logger trait defines logging interfaces.
pub trait Logger {
    /// Submits the given data to the log buffer.
    ///
    /// It returns the start and end log sequence number pair of the submitted data.
    fn submit(&self, log_data: Vec<u8>) -> Result<(usize, usize), Error>;

    /// Persists the log buffer up to the given log position.
    ///
    /// It returns the max flushed log sequence number.
    fn persist(&self, position: usize) -> Result<usize, Error>;
}
