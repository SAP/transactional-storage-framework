// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Journal, PersistenceLayer, Sequencer, Snapshot};
use scc::HashMap;
use std::time::Instant;

/// [`AccessController`] grants or rejects access to a database object identified as a [`usize`]
/// value.
#[derive(Debug, Default)]
pub struct AccessController {
    _table: HashMap<usize, Entry>,
}

/// [`ToAccessID`] derives a fixed [`usize`] value for the instance.
pub trait ToAccessID {
    /// The returned value should be unique in the process during the lifetime of the instance.
    fn to_access_id(&self) -> usize;
}

impl AccessController {
    /// Tries to gain read access to the database object.
    #[allow(clippy::unused_self, clippy::unused_async)]
    #[inline]
    pub async fn try_read<S: Sequencer>(&self, _snapshot: Snapshot<'_, '_, '_, S>) -> bool {
        unimplemented!()
    }

    /// Takes ownership of the record.
    ///
    /// The record becomes only accessible to future readers if the transaction that took ownership
    /// of the record has been successfully committed.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the transaction could not take ownership.
    #[allow(clippy::unused_self, clippy::unused_async)]
    #[inline]
    pub async fn take_ownership<S: Sequencer, P: PersistenceLayer<S>>(
        &self,
        _id: usize,
        _journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        unimplemented!()
    }

    /// Acquires the exclusive lock on the record.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the lock could not be acquired.
    #[allow(clippy::unused_self, clippy::unused_async)]
    #[inline]
    pub async fn lock_exclusive<S: Sequencer, P: PersistenceLayer<S>>(
        &self,
        _id: usize,
        _journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        unimplemented!()
    }

    /// Acquires a shared lock on the record.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the lock could not be acquired.
    #[allow(clippy::unused_self, clippy::unused_async)]
    #[inline]
    pub async fn lock_shared<S: Sequencer, P: PersistenceLayer<S>>(
        &self,
        _id: usize,
        _journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct Entry {}
