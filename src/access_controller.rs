// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor as JournalAnchor;
use super::{Error, Journal, PersistenceLayer, Sequencer, Snapshot};
use scc::{ebr, HashMap};
use std::collections::BTreeSet;
use std::time::Instant;

/// [`AccessController`] grants or rejects access to a database object identified as a [`usize`]
/// value.
#[derive(Debug, Default)]
pub struct AccessController<S: Sequencer> {
    table: HashMap<usize, Entry<S>>,
}

/// [`ToObjectID`] derives a fixed [`usize`] value for the instance.
pub trait ToObjectID {
    /// It must always return the same value for the same `self`, and the value has to be unique in
    /// the process during the lifetime of `self`.
    fn to_access_id(&self) -> usize;
}

impl<S: Sequencer> AccessController<S> {
    /// Tries to gain read access to the database object.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the specified deadline was reached.
    #[inline]
    pub async fn read<O: ToObjectID>(
        &self,
        object: &O,
        snapshot: &Snapshot<'_, '_, '_, S>,
        deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        if let Some((mut visibility, await_visibility)) = self
            .table
            .read_async(&object.to_access_id(), |_, entry| match entry {
                Entry::Immutable(instant) => {
                    // The object has become immutable, and old readers cannot see it.
                    (*snapshot >= *instant, None)
                }
                Entry::Owned(owner) => {
                    // The record is being created.
                    match owner.grant_read_access(snapshot, deadline) {
                        Ok(visibility) => (visibility, None),
                        Err(await_visibility) => (false, Some(await_visibility)),
                    }
                }
                Entry::Locked(_owner) => {
                    // The record being locked does not affect visibility.
                    (true, None)
                }
                Entry::Shared(_owners) => {
                    // The record being shared does not affect visibility.
                    (true, None)
                }
            })
            .await
        {
            if let Some(await_visibility) = await_visibility {
                visibility = await_visibility.await?;
            }
            Ok(visibility)
        } else {
            // No access control is set.
            Ok(true)
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

    /// The record is exclusively owned by the transaction.
    #[allow(dead_code)]
    Owned(ebr::Arc<JournalAnchor<S>>),

    /// The record is exclusively locked by the transaction.
    #[allow(dead_code)]
    Locked(ebr::Arc<JournalAnchor<S>>),

    /// The record is shared among multiple transactions.
    #[allow(dead_code)]
    Shared(SharedOwners<S>),
}

#[derive(Debug)]
enum SharedOwners<S: Sequencer> {
    /// There is only a single owner.
    #[allow(dead_code)]
    Single(ebr::Arc<JournalAnchor<S>>),

    /// There are multiple owners.
    #[allow(dead_code)]
    Multiple(ebr::Arc<BTreeSet<ebr::Arc<JournalAnchor<S>>>>),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::AtomicCounter;

    static_assertions::assert_eq_size!(Entry<AtomicCounter>, [u8; 16]);
}
