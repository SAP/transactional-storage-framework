use super::{Error, Sequencer, Transaction};
use std::time::Duration;

/// The Logger trait defines logging interfaces.
pub trait Logger<S: Sequencer> {
    /// Persists the given redo data.
    ///
    /// It returns the log sequence number.
    fn redo(&self, data: &[u8], transaction: &Transaction<S>) -> Result<usize, Error>;

    /// Persists the given undo data.
    ///
    /// It returns the log sequence number.
    fn undo(&self, data: &[u8], transaction: &Transaction<S>) -> Result<usize, Error>;

    /// Flushes pending log records into the persistent storage.
    ///
    /// It returns the max flushed log sequence number.
    fn flush(&self, position: usize, timeout: Duration) -> Result<usize, Error>;
}
