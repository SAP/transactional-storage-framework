// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Journal, PersistenceLayer, Sequencer, Snapshot};
use scc::HashMap;
use std::time::Instant;

/// [`AccessController`] grants or rejects access to a database object identified as a [`usize`]
/// value.
#[derive(Debug, Default)]
pub struct AccessController<S: Sequencer> {
    table: HashMap<usize, Entry<S>>,
}

/// [`ToObjectID`] derives a fixed [`usize`] value for the instance.
pub trait ToObjectID {
    /// The returned value should be unique in the process during the lifetime of the instance.
    fn to_access_id(&self) -> usize;
}

impl<S: Sequencer> AccessController<S> {
    /// Tries to gain read access to the database object.
    #[inline]
    pub async fn try_read<O: ToObjectID>(
        &self,
        object: &O,
        snapshot: Snapshot<'_, '_, '_, S>,
    ) -> bool {
        if let Some(visibility) = self
            .table
            .read_async(&object.to_access_id(), |_, entry| match entry {
                Entry::Immutable(instant) => {
                    // The object has become immutable, and old readers cannot see it.
                    snapshot >= *instant
                }
            })
            .await
        {
            visibility
        } else {
            // No access control is set.
            true
        }
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
    pub async fn take_ownership<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        _object: &O,
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
    pub async fn lock_exclusive<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        _object: &O,
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
    pub async fn lock_shared<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        _object: &O,
        _journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        unimplemented!()
    }
}

#[derive(Debug)]
enum Entry<S: Sequencer> {
    /// The record is now immutable, and only read access can be granted.
    #[allow(dead_code)]
    Immutable(S::Instant),
}
