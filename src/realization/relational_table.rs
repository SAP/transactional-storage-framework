// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::DataPlane;
use crate::{Error, Sequencer, Snapshot, Transaction};

/// Table is a two dimensional array of u8.
pub struct RelationalTable<S: Sequencer> {
    _version_vector: Option<Vec<S::Clock>>,
}

impl<S: Sequencer> RelationalTable<S> {
    /// Creates a new [`RelationalTable`].
    #[must_use]
    pub fn new() -> RelationalTable<S> {
        RelationalTable {
            _version_vector: None,
        }
    }
}

impl<S: Sequencer> DataPlane<S> for RelationalTable<S> {
    fn read(
        &self,
        _record_index: usize,
        _column_index: usize,
        _snapshot: &Snapshot<S>,
    ) -> Option<&[u8]> {
        None
    }
    fn update(
        &self,
        _record_index: usize,
        _column_index: usize,
        _data: (&[u8], usize),
        _transaction: &Transaction<S>,
        _snapshot: &Snapshot<S>,
    ) -> Result<(usize, usize), Error> {
        Err(Error::Fail)
    }
    fn put(
        &self,
        _data: (&[u8], usize),
        _transaction: &Transaction<S>,
        _snapshot: &Snapshot<S>,
    ) -> Result<usize, Error> {
        Err(Error::Fail)
    }
    fn remove(
        &self,
        _record_index: usize,
        _column_index: usize,
        _transaction: &Transaction<S>,
        _snapshot: &Snapshot<S>,
    ) -> Result<(usize, usize), Error> {
        Err(Error::Fail)
    }
    fn size(&self) -> (usize, usize) {
        (0, 0)
    }
    fn vacuum(&self, _min_snapshot_clock: S::Clock) -> Result<(), Error> {
        Ok(())
    }
}

impl<S: Sequencer> Default for RelationalTable<S> {
    fn default() -> Self {
        Self::new()
    }
}
