// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Sequencer};
use std::fmt::Debug;

/// The [`PersistenceLayer`] trait defines the interface between [`Database`](super::Database) and
/// the persistence layer of the database.
pub trait PersistenceLayer<S: Sequencer>: 'static + Debug + Send + Sync {
    /// Recovers the database.
    ///
    /// If a sequencer clock value is given, it only recovers the storage up until the given time point.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the database could not be recovered.
    fn recover(&self, until: Option<S::Clock>) -> Result<(), Error>;
}

/// Volatile memory device.
#[derive(Debug, Default)]
pub struct VolatileDevice<S: Sequencer> {
    _phantom: std::marker::PhantomData<S>,
}

impl<S: Sequencer> PersistenceLayer<S> for VolatileDevice<S> {
    #[inline]
    fn recover(&self, _until: Option<<S as Sequencer>::Clock>) -> Result<(), Error> {
        Ok(())
    }
}
