// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor as JournalAnchor;
use super::{Error, Journal, PersistenceLayer, Sequencer, Snapshot};
use scc::hash_map::Entry as MapEntry;
use scc::{ebr, HashMap};
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
    fn to_object_id(&self) -> usize;
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
            .read_async(&object.to_object_id(), |_, entry| match entry {
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
    /// An [`Error`] is returned if memory allocation failed, the database object was already
    /// created and globally visible, or another transaction has not completed creating the
    /// database object until the deadline is reached.
    #[inline]
    pub async fn reserve<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        object: &O,
        journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<(), Error> {
        let mut entry = match self.table.entry_async(object.to_object_id()).await {
            MapEntry::Occupied(entry) => entry,
            MapEntry::Vacant(entry) => {
                entry.insert_entry(Entry::Reserved(journal.anchor().clone()));
                return Ok(());
            }
        };
        if let Entry::Reserved(_owner) = entry.get_mut() {
            // TODO: wait for the owner to be rolled or committed.
        }
        Err(Error::SerializationFailure)
    }

    /// Acquires a shared lock on the database object.
    ///
    /// Returns `true` if the lock is newly acquired in the transaction.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the lock could not be acquired.
    #[inline]
    pub async fn share<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        object: &O,
        journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        let mut entry = match self.table.entry_async(object.to_object_id()).await {
            MapEntry::Occupied(entry) => entry,
            MapEntry::Vacant(entry) => {
                entry.insert_entry(Entry::Locked(LockMode::SingleShared(
                    journal.anchor().clone(),
                )));
                return Ok(true);
            }
        };
        match entry.get_mut() {
            Entry::Reserved(_) => {
                // TODO: wait for the owner to be rolled or committed.
                Err(Error::Timeout)
            }
            Entry::Created(instant) => {
                *entry.get_mut() = Entry::Locked(LockMode::SharedWithInstant(Box::new((
                    *instant,
                    OwnerSet::with_owner(journal.anchor().clone()),
                ))));
                Ok(true)
            }
            Entry::Locked(_) => {
                // TODO: try to add the transaction to the owner set.
                Err(Error::Conflict)
            }
            Entry::Deleted(_) => {
                // Already deleted.
                Err(Error::SerializationFailure)
            }
        }
    }

    /// Acquires the exclusive lock on the database object.
    ///
    /// Returns `true` if the lock is newly acquired in the transaction.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the lock could not be acquired.
    #[inline]
    pub async fn lock<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        object: &O,
        journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        let mut entry = match self.table.entry_async(object.to_object_id()).await {
            MapEntry::Occupied(entry) => entry,
            MapEntry::Vacant(entry) => {
                entry.insert_entry(Entry::Locked(LockMode::Exclusive(journal.anchor().clone())));
                return Ok(true);
            }
        };
        match entry.get_mut() {
            Entry::Reserved(_) => {
                // TODO: wait for the owner to be rolled or committed.
                Err(Error::Timeout)
            }
            Entry::Created(instant) => {
                *entry.get_mut() = Entry::Locked(LockMode::ExclusiveWithInstant(Box::new((
                    *instant,
                    journal.anchor().clone(),
                ))));
                Ok(true)
            }
            Entry::Locked(_) => {
                // TODO: try to acquire the lock after cleaning up the entry.
                Err(Error::Conflict)
            }
            Entry::Deleted(_) => {
                // Already deleted.
                Err(Error::SerializationFailure)
            }
        }
    }

    /// Takes ownership of the database object for deletion.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the transaction could not take ownership.
    #[inline]
    pub async fn mark<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        object: &O,
        journal: &mut Journal<'_, '_, S, P>,
        _deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        let mut entry = match self.table.entry_async(object.to_object_id()).await {
            MapEntry::Occupied(entry) => entry,
            MapEntry::Vacant(entry) => {
                entry.insert_entry(Entry::Locked(LockMode::Marked(journal.anchor().clone())));
                return Ok(true);
            }
        };
        match entry.get_mut() {
            Entry::Reserved(_) => {
                // TODO: wait for the owner to be rolled or committed.
                Err(Error::Timeout)
            }
            Entry::Created(instant) => {
                *entry.get_mut() = Entry::Locked(LockMode::MarkedWithInstant(Box::new((
                    *instant,
                    journal.anchor().clone(),
                ))));
                Ok(true)
            }
            Entry::Locked(_) => {
                // TODO: try to mark it after cleaning up the entry.
                Err(Error::Conflict)
            }
            Entry::Deleted(_) => {
                // Already deleted.
                Err(Error::SerializationFailure)
            }
        }
    }
}

#[derive(Debug)]
enum Entry<S: Sequencer> {
    /// The database object is prepared to be created.
    Reserved(ebr::Arc<JournalAnchor<S>>),

    /// The database object was created at the instant.
    #[allow(dead_code)]
    Created(S::Instant),

    /// The database object is locked.
    Locked(LockMode<S>),

    /// The database object was deleted at the instant.
    #[allow(dead_code)]
    Deleted(S::Instant),
}

#[derive(Debug)]
enum LockMode<S: Sequencer> {
    /// The database object is locked shared by a single transaction.
    SingleShared(ebr::Arc<JournalAnchor<S>>),

    /// The database object which may not be visible to some readers is shared by one or more
    /// transactions.
    SharedWithInstant(Box<(S::Instant, OwnerSet<S>)>),

    /// The database object is locked by the transaction.
    Exclusive(ebr::Arc<JournalAnchor<S>>),

    /// The database object which may not be visible to some readers is locked by the transaction.
    ExclusiveWithInstant(Box<(S::Instant, ebr::Arc<JournalAnchor<S>>)>),

    /// The database object is being deleted by the transaction.
    Marked(ebr::Arc<JournalAnchor<S>>),

    /// The database object which may not be visible to some readers is being deleted by the
    /// transaction.
    #[allow(dead_code)]
    MarkedWithInstant(Box<(S::Instant, ebr::Arc<JournalAnchor<S>>)>),
}

#[derive(Debug)]
struct OwnerSet<S: Sequencer> {
    #[allow(dead_code)]
    set: Vec<ebr::Arc<JournalAnchor<S>>>,
}

impl<S: Sequencer> OwnerSet<S> {
    /// Creates a new [`OwnerSet`] with a single owner inserted.
    fn with_owner(owner: ebr::Arc<JournalAnchor<S>>) -> OwnerSet<S> {
        OwnerSet { set: vec![owner] }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{AtomicCounter, Database};

    static_assertions::assert_eq_size!(Entry<AtomicCounter>, [u8; 16]);

    impl ToObjectID for usize {
        fn to_object_id(&self) -> usize {
            *self
        }
    }

    #[tokio::test]
    async fn empty_reserve() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        assert!(transaction.commit().await.is_ok());

        let snapshot = database.snapshot();
        assert!(access_controller.read(&0, &snapshot, None).await.unwrap());
    }

    #[tokio::test]
    async fn empty_share() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .share(&0, &mut journal, None)
            .await
            .unwrap());
        assert_eq!(journal.submit(), 1);
        assert!(transaction.commit().await.is_ok());
    }

    #[tokio::test]
    async fn empty_lock() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .lock(&0, &mut journal, None)
            .await
            .unwrap());
        assert_eq!(journal.submit(), 1);
        assert!(transaction.commit().await.is_ok());
    }

    #[tokio::test]
    async fn empty_mark() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .mark(&0, &mut journal, None)
            .await
            .unwrap());
        assert_eq!(journal.submit(), 1);
        assert!(transaction.commit().await.is_ok());
    }
}
