// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Journal, Sequencer};
use scc::HashMap;
use std::time::Instant;

/// [`LockTable`] manages the state of individual records.
#[derive(Debug, Default)]
pub struct LockTable {
    _table: HashMap<u64, Entry>,
}

impl LockTable {
    /// Takes ownership of the record.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the transaction could not take ownership.
    #[allow(clippy::unused_self, clippy::unused_async)]
    pub async fn take_ownership<S: Sequencer>(
        &self,
        _record_id: u64,
        _journal: &mut Journal<'_, '_, S>,
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
    pub async fn lock_exclusive<S: Sequencer>(
        &self,
        _record_id: u64,
        _journal: &mut Journal<'_, '_, S>,
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
    pub async fn lock_shared<S: Sequencer>(
        &self,
        _record_id: u64,
        _journal: &mut Journal<'_, '_, S>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct Entry {}
