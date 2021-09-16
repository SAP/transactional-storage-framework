// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Sequencer, Snapshot, Transaction};

use std::time::Duration;

/// [`DataPlane`] defines the data container interfaces.
///
/// A container is a two-dimensional plane of data.
pub trait DataPlane<S: Sequencer> {
    /// Gets the data located at the given position.
    fn read(
        &self,
        record_index: usize,
        column_index: usize,
        snapshot: &Snapshot<S>,
    ) -> Option<&[u8]>;

    /// Updates the data stored at the given position.
    ///
    /// It returns the new position of the updated data.
    ///
    /// # Errors
    ///
    /// Returns an error if update fails.
    fn update(
        &self,
        record_index: usize,
        column_index: usize,
        data: (&[u8], usize),
        transaction: &Transaction<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<(usize, usize), Error>;

    /// Puts the data into the container.
    ///
    /// # Errors
    ///
    /// Returns an error if putting the data fails.
    fn put(
        &self,
        data: (&[u8], usize),
        transaction: &Transaction<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<usize, Error>;

    /// Removes the data stored at the given position.
    ///
    /// # Errors
    ///
    /// Returns an error if removing the record fails.
    fn remove(
        &self,
        record_index: usize,
        column_index: usize,
        transaction: &Transaction<S>,
        snapshot: &Snapshot<S>,
    ) -> Result<(usize, usize), Error>;

    /// Returns the size of the container.
    fn size(&self) -> (usize, usize);

    /// Reclaims unreachable versioned records, and defragments data slots.
    ///
    /// # Errors
    ///
    /// It returns an error is vacuuming cannot be completed.
    fn vacuum(&self, min_snapshot_clock: S::Clock, timeout: Option<Duration>) -> Result<(), Error>;
}
