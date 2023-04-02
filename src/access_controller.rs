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
    //
    // This method returns `true` if no access control is defined for the database object,
    // therefore any access control mapping must be removed only if the corresponding database
    // object is always visible to all the readers, or it has become unreachable to readers.
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
                Entry::Reserved(owner) => {
                    // The database object is being created.
                    match owner.grant_read_access(snapshot, deadline) {
                        Ok(visibility) => (visibility, None),
                        Err(await_visibility) => (false, Some(await_visibility)),
                    }
                }
                Entry::Created(instant) => {
                    // The database object was created at `instant`.
                    (*snapshot >= *instant, None)
                }
                Entry::Locked(_owner) => {
                    // TODO: usually, the database object being locked does not affect visibility,
                    // but there are corner cases that have to be addressed precisely.
                    (true, None)
                }
                Entry::Deleted(instant) => {
                    // The database object was deleted at `instant`.
                    (*snapshot < *instant, None)
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

    /// Reserves access control data for a database object to create it.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if memory allocation failed or the identifier of the database
    /// object is not unique in the process.
    #[allow(clippy::unused_self, clippy::unused_async)]
    #[inline]
    pub async fn reserve<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        _object: &O,
        _journal: &mut Journal<'_, '_, S, P>,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    /// Acquires a shared lock on the database object.
    ///
    /// Returns `true` if the lock is newly acquired in the transaction.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the lock could not be acquired.
    #[allow(clippy::unused_self, clippy::unused_async)]
    #[inline]
    pub async fn share<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        _object: &O,
        _journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        unimplemented!()
    }

    /// Acquires the exclusive lock on the database object.
    ///
    /// Returns `true` if the lock is newly acquired in the transaction.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the lock could not be acquired.
    #[allow(clippy::unused_self, clippy::unused_async)]
    #[inline]
    pub async fn lock<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        _object: &O,
        _journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        unimplemented!()
    }

    /// Takes ownership of the database object for deletion.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the transaction could not take ownership.
    #[allow(clippy::unused_self, clippy::unused_async)]
    #[inline]
    pub async fn mark<O: ToObjectID, P: PersistenceLayer<S>>(
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
    /// The database object is prepared to be created.
    #[allow(dead_code)]
    Reserved(ebr::Arc<JournalAnchor<S>>),

    /// The database object was created at the instant.
    #[allow(dead_code)]
    Created(S::Instant),

    /// The database object is locked.
    #[allow(dead_code)]
    Locked(LockMode<S>),

    /// The database object was deleted at the instant.
    #[allow(dead_code)]
    Deleted(S::Instant),
}

#[derive(Debug)]
enum LockMode<S: Sequencer> {
    /// The database object is locked shared by a single transaction.
    #[allow(dead_code)]
    SingleShared(ebr::Arc<JournalAnchor<S>>),

    /// The database object which may not be visible to some readers is shared by one or more
    /// transactions.
    #[allow(dead_code)]
    SharedWithInstant(Box<(S::Instant, OwnerSet<S>)>),

    /// The database object is locked by the transaction.
    #[allow(dead_code)]
    Exclusive(ebr::Arc<JournalAnchor<S>>),

    /// The database object which may not be visible to some readers is locked by the transaction.
    #[allow(dead_code)]
    ExclusiveWithInstant(Box<(S::Instant, ebr::Arc<JournalAnchor<S>>)>),

    /// The database object is being deleted by the transaction.
    #[allow(dead_code)]
    Marked(ebr::Arc<JournalAnchor<S>>),

    /// The database object which may not be visible to some readers is being deleted by the
    /// transaction.
    #[allow(dead_code)]
    MarkedWithInstant(Box<(S::Instant, ebr::Arc<JournalAnchor<S>>)>),
}

#[derive(Debug)]
struct OwnerSet<S: Sequencer> {
    #[allow(dead_code)]
    set: BTreeSet<ebr::Arc<JournalAnchor<S>>>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::AtomicCounter;

    static_assertions::assert_eq_size!(Entry<AtomicCounter>, [u8; 16]);
}
