use crate::{ContainerHandle, Error, Logger, Sequencer, Transaction};

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
