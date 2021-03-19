// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{ContainerHandle, Error, Sequencer, Transaction};

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
}

impl Default for Log {
    fn default() -> Log {
        Log {
            log: Some(LogState::Memory(Vec::new())),
        }
    }
}

impl Log {
    /// Creates a new Log.
    pub fn new() -> Log {
        Default::default()
    }

    /// Passes the log data to the logger.
    ///
    /// The given transaction can be used by the logger for transaction recovery.
    pub fn submit<S: Sequencer, L: Logger<S>>(
        &mut self,
        logger: &L,
        transaction: &Transaction<S>,
    ) -> Result<(), Error> {
        if let Some(LogState::Memory(data)) = self.log.take() {
            match logger.submit(data, transaction) {
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
    pub fn persist<S: Sequencer, L: Logger<S>>(&mut self, logger: &L) -> Result<(), Error> {
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
}

/// The Logger trait defines logging interfaces.
pub trait Logger<S: Sequencer> {
    /// Submits the given data to the log buffer.
    ///
    /// It returns the start and end log sequence number pair of the submitted data.
    /// The given transaction is associated to the log data, thereby enabling it to recover transactions.
    fn submit(
        &self,
        log_data: Vec<u8>,
        transaction: &Transaction<S>,
    ) -> Result<(usize, usize), Error>;

    /// Persists the log buffer up to the given log position.
    ///
    /// It returns the max flushed log sequence number.
    fn persist(&self, position: usize) -> Result<usize, Error>;

    /// Recovers the storage.
    ///
    /// If a sequencer clock value is given, it only recovers the storage up until the given time point.
    fn recover(&self, until: Option<S::Clock>) -> Option<ContainerHandle<S>>;

    /// Loads a specific container.
    ///
    /// A container can be unloaded from memory without a logger, but it requires a logger to load data.
    fn load(&self, path: &str) -> Option<ContainerHandle<S>>;
}

/// FileLogger is a file-based logger that pushes data into files sequentially.
///
/// Checkpoint operations entail log file truncation.
pub struct FileLogger<S: Sequencer> {
    _path: String,
    _invalid_clock: S::Clock,
}

impl<S: Sequencer> FileLogger<S> {
    pub fn new(anchor: &str) -> FileLogger<S> {
        FileLogger {
            _path: String::from(anchor),
            _invalid_clock: S::invalid(),
        }
    }
}

impl<S: Sequencer> Logger<S> for FileLogger<S> {
    fn submit(
        &self,
        _log_data: Vec<u8>,
        _transaction: &Transaction<S>,
    ) -> Result<(usize, usize), Error> {
        Err(Error::Fail)
    }
    fn persist(&self, _position: usize) -> Result<usize, Error> {
        Err(Error::Fail)
    }
    fn recover(&self, _until: Option<S::Clock>) -> Option<ContainerHandle<S>> {
        None
    }
    fn load(&self, _path: &str) -> Option<ContainerHandle<S>> {
        None
    }
}
