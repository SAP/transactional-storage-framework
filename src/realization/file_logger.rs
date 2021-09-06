// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::{Container, Error, Logger, Sequencer, Transaction};

use scc::ebr;

/// [`FileLogger`] is a file-based logger that pushes data into files sequentially.
///
/// Checkpoint operations entail log file truncation.
pub struct FileLogger<S: Sequencer> {
    _path: String,
    _invalid_clock: S::Clock,
}

impl<S: Sequencer> FileLogger<S> {
    /// Creates  new [`FileLogger`].
    #[must_use]
    pub fn new(anchor: &str) -> FileLogger<S> {
        FileLogger {
            _path: String::from(anchor),
            _invalid_clock: S::Clock::default(),
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
    fn recover(&self, _until: Option<S::Clock>) -> Option<ebr::Arc<Container<S>>> {
        None
    }
    fn load(&self, _path: &str) -> Option<ebr::Arc<Container<S>>> {
        None
    }
}
