// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor as JournalAnchor;
use super::journal::{AwaitResponse, Relationship};
use super::{Error, Journal, PersistenceLayer, Sequencer, Snapshot};
use scc::hash_map::Entry as MapEntry;
use scc::{ebr, HashMap};
use std::cmp;
use std::collections::{BTreeSet, VecDeque};
use std::mem::take;
use std::ops::{Deref, DerefMut};
use std::time::Instant;

/// [`AccessController`] grants or rejects access to a database object identified as a [`usize`]
/// value.
#[derive(Debug, Default)]
pub struct AccessController<S: Sequencer> {
    table: HashMap<usize, ObjectState<S>>,
}

/// [`ToObjectID`] derives a fixed [`usize`] value for the instance.
pub trait ToObjectID {
    /// It must always return the same value for the same `self`, and the value has to be unique in
    /// the process during the lifetime of `self`.
    fn to_object_id(&self) -> usize;
}

/// [`PromotedAccess`] is kept inside a [`Journal`] when the [`Journal`] successfully promoted
/// access permission for a database object.
///
/// [`PromotedAccess`] instances kept in a [`Journal`] are used when the [`Journal`] has to be
/// rolled back, so that the access permission can also be rolled back.
#[derive(Debug)]
pub(super) enum PromotedAccess<S: Sequencer> {
    /// Promoted to `exclusive` from `shared`.
    #[allow(dead_code)]
    SharedToExclusive(Owner<S>),

    /// Promoted to `deleted` from `shared`.
    #[allow(dead_code)]
    SharedToDeleted(Owner<S>),

    /// Promoted to `deleted` from `exclusive`.
    #[allow(dead_code)]
    ExclusiveToDeleted(Owner<S>),
}

/// An owner of a database object.
#[derive(Debug)]
pub(super) struct Owner<S: Sequencer> {
    /// The address of the owner [`JournalAnchor`](super::journal::Anchor) is used as its
    /// identification.
    anchor: ebr::Arc<JournalAnchor<S>>,
}

/// Possible states of a database object.
#[derive(Debug)]
pub(super) enum ObjectState<S: Sequencer> {
    /// The database object is locked.
    Locked(LockMode<S>),

    /// The database object was created at the instant.
    #[allow(dead_code)]
    Created(S::Instant),

    /// The database object was deleted at the instant.
    #[allow(dead_code)]
    Deleted(S::Instant),
}

/// Locking modes.
#[derive(Debug)]
pub(super) enum LockMode<S: Sequencer> {
    /// The database object is prepared to be created.
    Created(Owner<S>),

    /// The database object is prepared to be created, but there are waiting transactions.
    CreatedAwaitable(Box<ExclusiveAwaitable<S>>),

    /// The database object is locked shared by a single transaction.
    Shared(Owner<S>),

    /// The database object which may not be visible to some readers is shared by one or more
    /// transactions.
    SharedAwaitable(Box<SharedAwaitable<S>>),

    /// The database object is locked by the transaction.
    Exclusive(Owner<S>),

    /// The database object which may not be visible to some readers is locked by the transaction.
    ExclusiveAwaitable(Box<ExclusiveAwaitable<S>>),

    /// The database object is being deleted by the transaction.
    Deleted(Owner<S>),

    /// The database object which may not be visible to some readers is being deleted by the
    /// transaction.
    DeletedAwaitable(Box<ExclusiveAwaitable<S>>),
}

/// Types of access requests.
///
/// The instant when the request was made is stored in it, and the value is used by the deadlock
/// detector.
#[derive(Debug)]
pub(super) enum Request<S: Sequencer> {
    /// A request to create a database object.
    Create(Instant, Owner<S>),

    /// A request to acquire a shared lock on the database object.
    Share(Instant, Owner<S>),

    /// A request to acquire the exclusive lock on the database object.
    Lock(Instant, Owner<S>),

    /// A request to acquire the exclusive lock on the database object for deletion.
    Delete(Instant, Owner<S>),
}

/// Shared access control information container for a database object.
#[derive(Debug)]
pub(super) struct SharedAwaitable<S: Sequencer> {
    /// The instant when the database object was created.
    ///
    /// The value is equal to `S::Instant::default()` if the database object is globally visible.
    creation_instant: S::Instant,

    /// A set of owners.
    owner_set: BTreeSet<Owner<S>>,

    /// The wait queue of the database object.
    wait_queue: WaitQueue<S>,
}

/// Exclusive access control information container for a database object.
#[derive(Debug)]
pub(super) struct ExclusiveAwaitable<S: Sequencer> {
    /// The instant when the database object was created.
    ///
    /// The value is equal to `S::Instant::default()` if the database object is globally visible.
    creation_instant: S::Instant,

    /// The only owner.
    owner: Owner<S>,

    /// Owner information before promotion.
    #[allow(dead_code)]
    promotion_data: Option<PromotedAccess<S>>,

    /// The wait queue of the database object.
    wait_queue: WaitQueue<S>,
}

/// The access request wait queue for a database object.
#[derive(Debug, Default)]
struct WaitQueue<S: Sequencer>(VecDeque<Request<S>>);

impl<S: Sequencer> AccessController<S> {
    /// Tries to gain read access to the database object.
    //
    // This method returns `true` if no access control is defined for the database object,
    // therefore any access control mapping must be removed only if the corresponding database
    // object is always visible to all the readers, or it has become unreachable to readers.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the specified deadline was reached or memory allocation failed
    /// when pushing a [`Waker`](std::task::Waker) into the owner
    /// [`Transaction`](super::Transaction).
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, ToObjectID};
    ///
    /// struct O(usize);
    ///
    /// impl ToObjectID for O {
    ///     fn to_object_id(&self) -> usize {
    ///         self.0
    ///    }
    /// }
    ///
    /// let database = Database::default();
    /// let access_controller = database.access_controller();
    /// async {
    ///     let transaction = database.transaction();
    ///     let mut journal = transaction.journal();
    ///     assert!(access_controller.create(&O(1), &mut journal, None).await.is_ok());
    ///     journal.submit();
    ///     assert!(transaction.commit().await.is_ok());
    ///
    ///     let snapshot = database.snapshot();
    ///     assert_eq!(access_controller.read(&O(1), &snapshot, None).await, Ok(true));
    /// };
    /// ```
    #[inline]
    pub async fn read<O: ToObjectID>(
        &self,
        object: &O,
        snapshot: &Snapshot<'_, '_, '_, S>,
        deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        loop {
            let await_eot = match self
                .table
                .read_async(&object.to_object_id(), |_, entry| match entry {
                    ObjectState::Locked(lock_mode) => {
                        match lock_mode {
                            LockMode::Created(owner) => {
                                // The database object is being created.
                                owner.grant_read_access(snapshot, deadline)
                            }
                            LockMode::CreatedAwaitable(exclusive_awaitable) => {
                                // The database object is being created.
                                exclusive_awaitable
                                    .owner
                                    .grant_read_access(snapshot, deadline)
                            }
                            LockMode::Shared(_) | LockMode::Exclusive(_) => {
                                // The database object is temporarily locked, but globally visible.
                                Ok(true)
                            }
                            LockMode::SharedAwaitable(shared_awaitable) => {
                                // The database object is temporarily shared, but the creation
                                // instant has to be checked.
                                Ok(*snapshot >= shared_awaitable.creation_instant)
                            }
                            LockMode::ExclusiveAwaitable(exclusive_awaitable) => {
                                // The database object is temporarily locked, but the creation
                                // instant has to be checked.
                                Ok(*snapshot >= exclusive_awaitable.creation_instant)
                            }
                            LockMode::Deleted(owner) => {
                                // The database object is being deleted.
                                //
                                // The result should be negated since seeing the deletion means not
                                // seeing the database object.
                                owner.grant_read_access(snapshot, deadline).map(|r| !r)
                            }
                            LockMode::DeletedAwaitable(exclusive_awaitable) => {
                                if *snapshot >= exclusive_awaitable.creation_instant {
                                    // The database object is being deleted.
                                    exclusive_awaitable
                                        .owner
                                        .anchor
                                        .grant_read_access(snapshot, deadline)
                                        .map(|r| !r)
                                } else {
                                    // The database object was created after the reader had
                                    // started.
                                    Ok(false)
                                }
                            }
                        }
                    }
                    ObjectState::Created(instant) => {
                        // The database object was created at `instant`.
                        Ok(*snapshot >= *instant)
                    }
                    ObjectState::Deleted(instant) => {
                        // The database object was deleted at `instant`.
                        Ok(*snapshot < *instant)
                    }
                })
                .await
            {
                Some(Ok(visibility)) => return Ok(visibility),
                Some(Err(await_eot)) => await_eot,
                None => break,
            };
            await_eot.await?;
        }

        // No access control is set.
        Ok(true)
    }

    /// Creates access control data for a newly created database object.
    ///
    /// The access control data is atomically converted into a time point data when the
    /// transaction is committed, so that readers can check if they have the read access right to
    /// the data, and no other transactions cannot create the same database object.
    ///
    /// Transactions may compete for the same database object, e.g., when the new database object
    /// becomes reachable via an index before being visible to database readers, however at most
    /// one of competing transactions can create access control data corresponding to the database
    /// object.
    ///
    /// The access control data can be deleted from the [`AccessController`] when the information
    /// is no longer neede, e.g., all the readers are allowed to access the database object; in
    /// this case, [`AccessController`] does not prohibit other transactions to successfully create
    /// new access control data for the same database object which may lead to a read consistency
    /// issue. Therefore, transactions must have checked whether the database object is accessible
    /// beforehand.
    ///
    /// `true` is returned if new access control data was created, and `false` if the transaction
    /// has already created the data.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if memory allocation failed, the database object was already
    /// created, or another transaction could not complete creating the database object until the
    /// deadline was reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, ToObjectID};
    ///
    /// struct O(usize);
    ///
    /// impl ToObjectID for O {
    ///     fn to_object_id(&self) -> usize {
    ///         self.0
    ///    }
    /// }
    ///
    /// let database = Database::default();
    /// let access_controller = database.access_controller();
    /// async {
    ///     let transaction = database.transaction();
    ///     let mut journal = transaction.journal();
    ///     assert!(access_controller.create(&O(1), &mut journal, None).await.is_ok());
    /// };
    /// ```
    #[inline]
    pub async fn create<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        object: &O,
        journal: &mut Journal<'_, '_, S, P>,
        deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        let mut entry = match self.table.entry_async(object.to_object_id()).await {
            MapEntry::Occupied(entry) => entry,
            MapEntry::Vacant(entry) => {
                entry.insert_entry(ObjectState::Locked(LockMode::Created(Owner::from(journal))));
                return Ok(true);
            }
        };

        let result = Self::try_create(entry.get_mut(), journal.anchor(), deadline)?;
        if let Some(result) = result {
            return Ok(result);
        } else if let (
            Some(deadline),
            ObjectState::Locked(LockMode::CreatedAwaitable(exclusive_awaitable)),
        ) = (deadline, entry.get_mut())
        {
            let message_sender = journal.message_sender();
            let owner = Owner::from(journal);
            let request = Request::Create(Instant::now(), owner.clone());
            exclusive_awaitable.push_request(request);
            return AwaitResponse::new(owner, entry, message_sender, deadline).await;
        }

        // The database object has been created, deleted, or invisible.
        Err(Error::SerializationFailure)
    }

    /// Acquires a shared lock on the database object.
    ///
    /// Returns `true` if the lock was newly acquired in the transaction.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the shared access request was denied, memory allocation failed,
    /// or the specified deadline was reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, ToObjectID};
    ///
    /// struct O(usize);
    ///
    /// impl ToObjectID for O {
    ///     fn to_object_id(&self) -> usize {
    ///         self.0
    ///    }
    /// }
    ///
    /// let database = Database::default();
    /// let access_controller = database.access_controller();
    /// async {
    ///     let transaction = database.transaction();
    ///     let mut journal = transaction.journal();
    ///     assert!(access_controller.create(&O(1), &mut journal, None).await.is_ok());
    ///     journal.submit();
    ///     assert!(transaction.commit().await.is_ok());
    ///
    ///     let transaction = database.transaction();
    ///     let mut journal = transaction.journal();
    ///     assert_eq!(access_controller.share(&O(1), &mut journal, None).await, Ok(true));
    /// };
    /// ```
    #[inline]
    pub async fn share<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        object: &O,
        journal: &mut Journal<'_, '_, S, P>,
        deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        let mut entry = match self.table.entry_async(object.to_object_id()).await {
            MapEntry::Occupied(entry) => entry,
            MapEntry::Vacant(entry) => {
                entry.insert_entry(ObjectState::Locked(LockMode::Shared(Owner::from(journal))));
                return Ok(true);
            }
        };

        let result = Self::try_share(entry.get_mut(), journal.anchor(), deadline)?;
        if let Some(result) = result {
            return Ok(result);
        } else if let ObjectState::Locked(lock_mode) = entry.get_mut() {
            match lock_mode {
                LockMode::CreatedAwaitable(exclusive_awaitable)
                | LockMode::ExclusiveAwaitable(exclusive_awaitable)
                | LockMode::DeletedAwaitable(exclusive_awaitable) => {
                    if let Some(deadline) = deadline {
                        let message_sender = journal.message_sender();
                        let owner = Owner::from(journal);
                        let request = Request::Share(Instant::now(), owner.clone());
                        exclusive_awaitable.push_request(request);
                        return AwaitResponse::new(owner, entry, message_sender, deadline).await;
                    }
                }
                LockMode::SharedAwaitable(shared_awaitable) => {
                    if let Some(deadline) = deadline {
                        let message_sender = journal.message_sender();
                        let owner = Owner::from(journal);
                        let request = Request::Share(Instant::now(), owner.clone());
                        shared_awaitable.push_request(request);
                        return AwaitResponse::new(owner, entry, message_sender, deadline).await;
                    }
                }
                _ => (),
            }
        }

        // The database object has been deleted, or invisible.
        Err(Error::SerializationFailure)
    }

    /// Acquires the exclusive lock on the database object.
    ///
    /// Returns `true` if the lock was newly acquired in the transaction.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the exclusive access request was denied, memory allocation
    /// failed, or the specified deadline was reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, ToObjectID};
    ///
    /// struct O(usize);
    ///
    /// impl ToObjectID for O {
    ///     fn to_object_id(&self) -> usize {
    ///         self.0
    ///    }
    /// }
    ///
    /// let database = Database::default();
    /// let access_controller = database.access_controller();
    /// async {
    ///     let transaction = database.transaction();
    ///     let mut journal = transaction.journal();
    ///     assert!(access_controller.create(&O(1), &mut journal, None).await.is_ok());
    ///     journal.submit();
    ///     assert!(transaction.commit().await.is_ok());
    ///
    ///     let transaction = database.transaction();
    ///     let mut journal = transaction.journal();
    ///     assert_eq!(access_controller.lock(&O(1), &mut journal, None).await, Ok(true));
    /// };
    /// ```
    #[inline]
    pub async fn lock<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        object: &O,
        journal: &mut Journal<'_, '_, S, P>,
        deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        let mut entry = match self.table.entry_async(object.to_object_id()).await {
            MapEntry::Occupied(entry) => entry,
            MapEntry::Vacant(entry) => {
                entry.insert_entry(ObjectState::Locked(LockMode::Exclusive(Owner::from(
                    journal,
                ))));
                return Ok(true);
            }
        };

        let result = Self::try_lock(entry.get_mut(), journal.anchor(), deadline)?;
        if let Some(result) = result {
            return Ok(result);
        } else if let ObjectState::Locked(lock_mode) = entry.get_mut() {
            match lock_mode {
                LockMode::CreatedAwaitable(exclusive_awaitable)
                | LockMode::ExclusiveAwaitable(exclusive_awaitable)
                | LockMode::DeletedAwaitable(exclusive_awaitable) => {
                    if let Some(deadline) = deadline {
                        let message_sender = journal.message_sender();
                        let owner = Owner::from(journal);
                        let request = Request::Lock(Instant::now(), owner.clone());
                        exclusive_awaitable.push_request(request);
                        return AwaitResponse::new(owner, entry, message_sender, deadline).await;
                    }
                }
                LockMode::SharedAwaitable(shared_awaitable) => {
                    if let Some(deadline) = deadline {
                        let message_sender = journal.message_sender();
                        let owner = Owner::from(journal);
                        let request = Request::Lock(Instant::now(), owner.clone());
                        shared_awaitable.push_request(request);
                        return AwaitResponse::new(owner, entry, message_sender, deadline).await;
                    }
                }
                _ => (),
            }
        }

        // The database object has been deleted, or invisible.
        Err(Error::SerializationFailure)
    }

    /// Takes ownership of the database object for deletion.
    ///
    /// Returns `true` if the database object was newly deleted in the transaction.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the database object was deleted, memory allocation failed, or
    /// the specified deadline was reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use sap_tsf::{Database, ToObjectID};
    ///
    /// struct O(usize);
    ///
    /// impl ToObjectID for O {
    ///     fn to_object_id(&self) -> usize {
    ///         self.0
    ///    }
    /// }
    ///
    /// let database = Database::default();
    /// let access_controller = database.access_controller();
    /// async {
    ///     let transaction = database.transaction();
    ///     let mut journal = transaction.journal();
    ///     assert!(access_controller.create(&O(1), &mut journal, None).await.is_ok());
    ///     journal.submit();
    ///     assert!(transaction.commit().await.is_ok());
    ///
    ///     let transaction = database.transaction();
    ///     let mut journal = transaction.journal();
    ///     assert_eq!(access_controller.delete(&O(1), &mut journal, None).await, Ok(true));
    /// };
    /// ```
    #[inline]
    pub async fn delete<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        object: &O,
        journal: &mut Journal<'_, '_, S, P>,
        deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        let mut entry = match self.table.entry_async(object.to_object_id()).await {
            MapEntry::Occupied(entry) => entry,
            MapEntry::Vacant(entry) => {
                entry.insert_entry(ObjectState::Locked(LockMode::Deleted(Owner::from(journal))));
                return Ok(true);
            }
        };

        let result = Self::try_delete(entry.get_mut(), journal.anchor(), deadline)?;
        if let Some(result) = result {
            return Ok(result);
        } else if let ObjectState::Locked(lock_mode) = entry.get_mut() {
            match lock_mode {
                LockMode::CreatedAwaitable(exclusive_awaitable)
                | LockMode::ExclusiveAwaitable(exclusive_awaitable)
                | LockMode::DeletedAwaitable(exclusive_awaitable) => {
                    if let Some(deadline) = deadline {
                        let message_sender = journal.message_sender();
                        let owner = Owner::from(journal);
                        let request = Request::Delete(Instant::now(), owner.clone());
                        exclusive_awaitable.push_request(request);
                        return AwaitResponse::new(owner, entry, message_sender, deadline).await;
                    }
                }
                LockMode::SharedAwaitable(shared_awaitable) => {
                    if let Some(deadline) = deadline {
                        // Wait for the database resource to be available to the transaction.
                        let message_sender = journal.message_sender();
                        let owner = Owner::from(journal);
                        let request = Request::Delete(Instant::now(), owner.clone());
                        shared_awaitable.push_request(request);
                        return AwaitResponse::new(owner, entry, message_sender, deadline).await;
                    }
                }
                _ => (),
            }
        }

        // The database object has been deleted, or invisible.
        Err(Error::SerializationFailure)
    }

    /// Transfers ownership to all the eligible waiting transactions.
    ///
    /// If the database object still need to be monitored, it returns `true`. It is a blocking and
    /// synchronous method invoked by [`Overseer`](super::overseer::Overseer).
    pub(super) fn transfer_ownership(&self, object_id: usize) -> bool {
        self.table
            .update(&object_id, |_, object_state| {
                let wait_queue = if let ObjectState::Locked(lock_mode) = object_state {
                    match lock_mode {
                        LockMode::Created(_)
                        | LockMode::Shared(_)
                        | LockMode::Exclusive(_)
                        | LockMode::Deleted(_) => None,
                        LockMode::CreatedAwaitable(exclusive_awaitable)
                        | LockMode::ExclusiveAwaitable(exclusive_awaitable)
                        | LockMode::DeletedAwaitable(exclusive_awaitable) => {
                            let wait_queue = take(&mut exclusive_awaitable.wait_queue);
                            Self::process_wait_queue(object_state, wait_queue)
                        }
                        LockMode::SharedAwaitable(shared_awaitable) => {
                            let wait_queue = take(&mut shared_awaitable.wait_queue);
                            Self::process_wait_queue(object_state, wait_queue)
                        }
                    }
                } else {
                    None
                };
                Self::post_process_object_state(object_state, wait_queue)
            })
            .map_or(false, |r| r)
    }

    /// Processes the supplied wait queue.
    fn process_wait_queue(
        object_state: &mut ObjectState<S>,
        mut wait_queue: WaitQueue<S>,
    ) -> Option<WaitQueue<S>> {
        while let Some(request) = wait_queue.clone_oldest() {
            if request.owner().is_result_set() {
                // The request must have been timed out.
                wait_queue.remove_oldest();
                continue;
            }
            let result = match &request {
                Request::Create(_, new_owner) => {
                    Self::try_create(object_state, &new_owner.anchor, Some(Instant::now()))
                }
                Request::Share(_, new_owner) => {
                    Self::try_share(object_state, &new_owner.anchor, Some(Instant::now()))
                }
                Request::Lock(_, new_owner) => {
                    Self::try_lock(object_state, &new_owner.anchor, Some(Instant::now()))
                }
                Request::Delete(_, new_owner) => {
                    Self::try_delete(object_state, &new_owner.anchor, Some(Instant::now()))
                }
            };
            match result {
                Ok(Some(result)) => {
                    wait_queue.remove_oldest();
                    request.owner().set_result(Ok(result));
                }
                Ok(None) => {
                    break;
                }
                Err(error) => {
                    wait_queue.remove_oldest();
                    request.owner().set_result(Err(error));
                }
            }
        }

        if wait_queue.is_empty() {
            None
        } else {
            Some(wait_queue)
        }
    }

    /// Cleans up [`ObjectState`] and gets the supplied wait queue into the [`ObjectState`].
    ///
    /// Returns `true` if there are waiting transactions.
    fn post_process_object_state(
        object_state: &mut ObjectState<S>,
        mut wait_queue: Option<WaitQueue<S>>,
    ) -> bool {
        match object_state {
            ObjectState::Locked(lock_mode) => match lock_mode {
                LockMode::Created(owner) => {
                    if let Some(wait_queue) = wait_queue {
                        if wait_queue.is_empty() {
                            return false;
                        }
                        *lock_mode = LockMode::CreatedAwaitable(
                            ExclusiveAwaitable::with_owner_and_wait_queue(
                                owner.clone(),
                                wait_queue,
                            ),
                        );
                    } else {
                        return false;
                    }
                }
                LockMode::CreatedAwaitable(exclusive_awaitable) => {
                    if exclusive_awaitable.wait_queue.inherit(wait_queue.as_mut()) {
                        if exclusive_awaitable.creation_instant == S::Instant::default() {
                            *lock_mode = LockMode::Created(exclusive_awaitable.owner.clone());
                        }
                        return false;
                    }
                }
                LockMode::Shared(owner) => {
                    if let Some(wait_queue) = wait_queue {
                        if wait_queue.is_empty() {
                            return false;
                        }
                        *lock_mode = LockMode::SharedAwaitable(
                            SharedAwaitable::with_owner_and_wait_queue(owner.clone(), wait_queue),
                        );
                    } else {
                        return false;
                    }
                }
                LockMode::SharedAwaitable(shared_awaitable) => {
                    if shared_awaitable.wait_queue.inherit(wait_queue.as_mut()) {
                        if shared_awaitable.creation_instant == S::Instant::default()
                            && shared_awaitable.owner_set.len() == 1
                        {
                            if let Some(owner) = shared_awaitable.owner_set.pop_first() {
                                *lock_mode = LockMode::Shared(owner);
                            }
                        }
                        return false;
                    }
                }
                LockMode::Exclusive(owner) => {
                    if let Some(wait_queue) = wait_queue {
                        if wait_queue.is_empty() {
                            return false;
                        }
                        *lock_mode = LockMode::ExclusiveAwaitable(
                            ExclusiveAwaitable::with_owner_and_wait_queue(
                                owner.clone(),
                                wait_queue,
                            ),
                        );
                    } else {
                        return false;
                    }
                }
                LockMode::ExclusiveAwaitable(exclusive_awaitable) => {
                    if exclusive_awaitable.wait_queue.inherit(wait_queue.as_mut()) {
                        if exclusive_awaitable.creation_instant == S::Instant::default() {
                            *lock_mode = LockMode::Exclusive(exclusive_awaitable.owner.clone());
                        }
                        return false;
                    }
                }
                LockMode::Deleted(owner) => {
                    if let Some(wait_queue) = wait_queue {
                        if wait_queue.is_empty() {
                            return false;
                        }
                        *lock_mode = LockMode::DeletedAwaitable(
                            ExclusiveAwaitable::with_owner_and_wait_queue(
                                owner.clone(),
                                wait_queue,
                            ),
                        );
                    } else {
                        return false;
                    }
                }
                LockMode::DeletedAwaitable(exclusive_awaitable) => {
                    if exclusive_awaitable.wait_queue.inherit(wait_queue.as_mut()) {
                        if exclusive_awaitable.creation_instant == S::Instant::default() {
                            *lock_mode = LockMode::Deleted(exclusive_awaitable.owner.clone());
                        }
                        return false;
                    }
                }
            },
            ObjectState::Created(_) | ObjectState::Deleted(_) => return false,
        };
        true
    }

    /// Tries to create the database object.
    fn try_create(
        object_state: &mut ObjectState<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        while let ObjectState::Locked(lock_mode) = object_state {
            match lock_mode {
                LockMode::Created(owner) => {
                    // The state of the owner needs to be checked.
                    match owner.grant_write_access(new_owner) {
                        Relationship::Committed(_) => {
                            // The transaction was committed, meaning that the database object has
                            // been successfully created.
                            return Err(Error::SerializationFailure);
                        }
                        Relationship::RolledBack => {
                            // The transaction or the owner journal was rolled back.
                            *lock_mode = LockMode::Created(Owner {
                                anchor: new_owner.clone(),
                            });
                            return Ok(Some(true));
                        }
                        Relationship::Linearizable => {
                            // Already created in a previously submitted journal in the same
                            // transaction.
                            return Ok(Some(false));
                        }
                        Relationship::Concurrent => {
                            // TODO: intra-transaction deadlock - need to check this.
                            return Err(Error::Deadlock);
                        }
                        Relationship::Unknown => {
                            if deadline.is_some() {
                                // Prepare for awaiting access to the database object.
                                *lock_mode = LockMode::CreatedAwaitable(
                                    ExclusiveAwaitable::with_owner(owner.clone()),
                                );
                            } else {
                                // No deadline is specified.
                                break;
                            }
                        }
                    }
                }
                LockMode::CreatedAwaitable(_) => {
                    return Self::take_exclusively_owned_awaitable_to_create(
                        lock_mode, new_owner, deadline,
                    );
                }
                _ => break,
            }
        }
        Err(Error::SerializationFailure)
    }

    /// Tries to take shared ownership of the database object.
    fn try_share(
        object_state: &mut ObjectState<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        loop {
            match object_state {
                ObjectState::Locked(lock_mode) => {
                    match lock_mode {
                        LockMode::Created(_) | LockMode::Exclusive(_) | LockMode::Deleted(_) => {
                            let result = Self::take_exclusively_owned_for_share(
                                lock_mode, new_owner, deadline,
                            )?;
                            if let Some(result) = result {
                                return Ok(Some(result));
                            }
                        }
                        LockMode::CreatedAwaitable(_)
                        | LockMode::ExclusiveAwaitable(_)
                        | LockMode::DeletedAwaitable(_) => {
                            return Self::take_exclusively_owned_awaitable_for_share(
                                lock_mode, new_owner, deadline,
                            );
                        }
                        LockMode::Shared(owner) => {
                            if let Relationship::Linearizable = owner.grant_write_access(new_owner)
                            {
                                // The transaction already owns the database object.
                                return Ok(Some(false));
                            }
                            let mut shared_awaitable = SharedAwaitable::with_owner(owner.clone());
                            shared_awaitable.owner_set.insert(Owner {
                                anchor: new_owner.clone(),
                            });
                            *lock_mode = LockMode::SharedAwaitable(shared_awaitable);
                            return Ok(Some(true));
                        }
                        LockMode::SharedAwaitable(shared_awaitable) => {
                            if shared_awaitable.owner_set.iter().any(|o| {
                                matches!(
                                    o.grant_write_access(new_owner),
                                    Relationship::Linearizable
                                )
                            }) {
                                // The transaction already owns the database object.
                                return Ok(Some(false));
                            } else if shared_awaitable.wait_queue.is_empty() {
                                shared_awaitable.owner_set.insert(Owner {
                                    anchor: new_owner.clone(),
                                });
                                return Ok(Some(true));
                            } else if deadline.is_some() {
                                return Ok(None);
                            }

                            // The wait queue is not empty, but no deadline is specified.
                            break;
                        }
                    }
                }
                ObjectState::Created(instant) => {
                    // The database object is not owned or locked.
                    *object_state = ObjectState::Locked(LockMode::SharedAwaitable(
                        SharedAwaitable::with_instant_and_owner(
                            *instant,
                            Owner {
                                anchor: new_owner.clone(),
                            },
                        ),
                    ));
                    return Ok(Some(true));
                }
                ObjectState::Deleted(_) => {
                    // Already deleted.
                    break;
                }
            }
        }
        Err(Error::SerializationFailure)
    }

    /// Tries to take exclusive ownership of the database object.
    fn try_lock(
        object_state: &mut ObjectState<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        loop {
            match object_state {
                ObjectState::Locked(lock_mode) => match lock_mode {
                    LockMode::Created(_) | LockMode::Exclusive(_) | LockMode::Deleted(_) => {
                        let concluded =
                            Self::take_exclusively_owned_to_own(lock_mode, new_owner, deadline)?;
                        if let Some(result) = concluded {
                            return Ok(Some(result));
                        }
                    }
                    LockMode::CreatedAwaitable(_)
                    | LockMode::ExclusiveAwaitable(_)
                    | LockMode::DeletedAwaitable(_) => {
                        return Self::take_exclusively_owned_awaitable_to_own(
                            lock_mode, new_owner, deadline,
                        );
                    }
                    LockMode::Shared(owner) => {
                        if let Relationship::Linearizable = owner.grant_write_access(new_owner) {
                            // TODO: lock-promotion.
                            return Err(Error::SerializationFailure);
                        }
                        *lock_mode =
                            LockMode::SharedAwaitable(SharedAwaitable::with_owner(owner.clone()));
                        return Ok(Some(true));
                    }
                    LockMode::SharedAwaitable(shared_awaitable) => {
                        if shared_awaitable.cleanup_inactive_owners()
                            && shared_awaitable.wait_queue.is_empty()
                        {
                            if shared_awaitable.creation_instant == S::Instant::default() {
                                *lock_mode = LockMode::Exclusive(Owner {
                                    anchor: new_owner.clone(),
                                });
                            } else {
                                *lock_mode = LockMode::ExclusiveAwaitable(
                                    ExclusiveAwaitable::with_instant_and_owner(
                                        shared_awaitable.creation_instant,
                                        Owner {
                                            anchor: new_owner.clone(),
                                        },
                                    ),
                                );
                            }
                            return Ok(Some(true));
                        } else if deadline.is_some() {
                            return Ok(None);
                        }

                        // The wait queue is not empty, but no deadline is specified.
                        break;
                    }
                },
                ObjectState::Created(instant) => {
                    *object_state = ObjectState::Locked(LockMode::ExclusiveAwaitable(
                        ExclusiveAwaitable::with_instant_and_owner(
                            *instant,
                            Owner {
                                anchor: new_owner.clone(),
                            },
                        ),
                    ));
                    return Ok(Some(true));
                }
                ObjectState::Deleted(_) => {
                    // Already deleted.
                    break;
                }
            }
        }
        Err(Error::SerializationFailure)
    }

    /// Tries to delete the database object.
    fn try_delete(
        object_state: &mut ObjectState<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        loop {
            match object_state {
                ObjectState::Locked(lock_mode) => match lock_mode {
                    LockMode::Created(_) | LockMode::Exclusive(_) | LockMode::Deleted(_) => {
                        let concluded =
                            Self::take_exclusively_owned_to_delete(lock_mode, new_owner, deadline)?;
                        if let Some(result) = concluded {
                            return Ok(Some(result));
                        }
                    }
                    LockMode::CreatedAwaitable(_)
                    | LockMode::ExclusiveAwaitable(_)
                    | LockMode::DeletedAwaitable(_) => {
                        return Self::take_exclusively_owned_awaitable_to_delete(
                            lock_mode, new_owner, deadline,
                        );
                    }
                    LockMode::Shared(owner) => {
                        if let Relationship::Linearizable = owner.grant_write_access(new_owner) {
                            // TODO: lock-promotion.
                            return Err(Error::SerializationFailure);
                        }
                        *lock_mode =
                            LockMode::SharedAwaitable(SharedAwaitable::with_owner(owner.clone()));
                        return Ok(Some(true));
                    }
                    LockMode::SharedAwaitable(shared_awaitable) => {
                        if shared_awaitable.cleanup_inactive_owners()
                            && shared_awaitable.wait_queue.is_empty()
                        {
                            if shared_awaitable.creation_instant == S::Instant::default() {
                                *lock_mode = LockMode::Deleted(Owner {
                                    anchor: new_owner.clone(),
                                });
                            } else {
                                *lock_mode = LockMode::DeletedAwaitable(
                                    ExclusiveAwaitable::with_instant_and_owner(
                                        shared_awaitable.creation_instant,
                                        Owner {
                                            anchor: new_owner.clone(),
                                        },
                                    ),
                                );
                            }
                            return Ok(Some(true));
                        } else if deadline.is_some() {
                            return Ok(None);
                        }

                        // The wait queue is not empty, but no deadline is specified.
                        break;
                    }
                },
                ObjectState::Created(instant) => {
                    *object_state = ObjectState::Locked(LockMode::DeletedAwaitable(
                        ExclusiveAwaitable::with_instant_and_owner(
                            *instant,
                            Owner {
                                anchor: new_owner.clone(),
                            },
                        ),
                    ));
                    return Ok(Some(true));
                }
                ObjectState::Deleted(_) => {
                    // Already deleted.
                    break;
                }
            }
        }
        Err(Error::SerializationFailure)
    }

    /// Takes or exclusive ownership of the exclusively owned `awaitable` database object.
    fn take_exclusively_owned_awaitable_to_create(
        lock_mode: &mut LockMode<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (exclusive_awaitable, _, _) = Self::exclusive_awaitable_access_defails(lock_mode)?;

        // The state of the owner needs to be checked.
        match exclusive_awaitable.owner.grant_write_access(new_owner) {
            Relationship::Committed(_) => {
                // The object was already created.
                return Err(Error::SerializationFailure);
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                if exclusive_awaitable.wait_queue.is_empty() {
                    *lock_mode = LockMode::Created(Owner {
                        anchor: new_owner.clone(),
                    });
                    return Ok(Some(true));
                }
            }
            Relationship::Linearizable => {
                // Already created in the same transaction.
                return Ok(Some(false));
            }
            Relationship::Concurrent => {
                // TODO: intra-transaction deadlock - need to check this.
                return Err(Error::Deadlock);
            }
            Relationship::Unknown => (),
        };

        if deadline.is_some() {
            Ok(None)
        } else {
            // No deadline is specified.
            Err(Error::SerializationFailure)
        }
    }

    /// Takes shared ownership of the exclusively owned database object.
    ///
    /// Returns `Ok(Some(result))` if the [`Journal`] got access to the database object.
    fn take_exclusively_owned_for_share(
        lock_mode: &mut LockMode<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (owner, is_created, is_deleted) = match lock_mode {
            LockMode::Created(owner) => (owner, true, false),
            LockMode::Exclusive(owner) => (owner, false, false),
            LockMode::Deleted(owner) => (owner, false, true),
            _ => return Err(Error::WrongParameter),
        };

        // The state of the owner needs to be checked.
        match owner.grant_write_access(new_owner) {
            Relationship::Committed(commit_instant) => {
                if is_deleted {
                    Err(Error::SerializationFailure)
                } else {
                    *lock_mode = if is_created {
                        LockMode::SharedAwaitable(SharedAwaitable::with_instant_and_owner(
                            commit_instant,
                            Owner {
                                anchor: new_owner.clone(),
                            },
                        ))
                    } else {
                        LockMode::Shared(Owner {
                            anchor: new_owner.clone(),
                        })
                    };
                    Ok(Some(true))
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                *lock_mode = LockMode::Shared(Owner {
                    anchor: new_owner.clone(),
                });
                Ok(Some(true))
            }
            Relationship::Linearizable => {
                // `Created` is stronger than `Shared`, so nothing to do.
                Ok(Some(false))
            }
            Relationship::Concurrent => {
                // TODO: intra-transaction deadlock - need to check this.
                Err(Error::Deadlock)
            }
            Relationship::Unknown => {
                if deadline.is_some() {
                    *lock_mode = Self::augment_wait_queue(is_created, is_deleted, owner.clone());
                    Ok(None)
                } else {
                    // No deadline is specified.
                    Err(Error::SerializationFailure)
                }
            }
        }
    }

    /// Takes shared ownership of the exclusively owned and `awaitable` database object.
    fn take_exclusively_owned_awaitable_for_share(
        lock_mode: &mut LockMode<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (exclusive_awaitable, is_created, is_deleted) =
            Self::exclusive_awaitable_access_defails(lock_mode)?;

        // The state of the owner needs to be checked.
        match exclusive_awaitable.owner.grant_write_access(new_owner) {
            Relationship::Committed(commit_instant) => {
                if is_deleted {
                    return Err(Error::SerializationFailure);
                } else if exclusive_awaitable.wait_queue.is_empty() {
                    // The transaction was committed and no transactions are waiting for the
                    // database object.
                    let creation_instant = if is_created {
                        commit_instant
                    } else {
                        exclusive_awaitable.creation_instant
                    };
                    *lock_mode =
                        LockMode::SharedAwaitable(SharedAwaitable::with_instant_and_owner(
                            creation_instant,
                            Owner {
                                anchor: new_owner.clone(),
                            },
                        ));
                    return Ok(Some(true));
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                if exclusive_awaitable.wait_queue.is_empty() {
                    *lock_mode = LockMode::Shared(Owner {
                        anchor: new_owner.clone(),
                    });
                    return Ok(Some(true));
                }
            }
            Relationship::Linearizable => {
                // The transaction already has the exclusive ownership.
                return Ok(Some(false));
            }
            Relationship::Concurrent => {
                // TODO: intra-transaction deadlock - need to check this.
                return Err(Error::Deadlock);
            }
            Relationship::Unknown => (),
        };

        if deadline.is_some() {
            Ok(None)
        } else {
            // No deadline is specified.
            Err(Error::SerializationFailure)
        }
    }

    /// Takes exclusive ownership of the exclusively owned database object.
    ///
    /// Returns `Ok(Some(result))` if the [`Journal`] got access to the database object.
    fn take_exclusively_owned_to_own(
        lock_mode: &mut LockMode<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (owner, is_created, is_deleted) = match lock_mode {
            LockMode::Created(owner) => (owner, true, false),
            LockMode::Exclusive(owner) => (owner, false, false),
            LockMode::Deleted(owner) => (owner, false, true),
            _ => return Err(Error::WrongParameter),
        };

        // The state of the owner needs to be checked.
        match owner.grant_write_access(new_owner) {
            Relationship::Committed(commit_instant) => {
                if is_deleted {
                    Err(Error::SerializationFailure)
                } else {
                    *lock_mode = if is_created {
                        LockMode::ExclusiveAwaitable(ExclusiveAwaitable::with_instant_and_owner(
                            commit_instant,
                            Owner {
                                anchor: new_owner.clone(),
                            },
                        ))
                    } else {
                        LockMode::Exclusive(Owner {
                            anchor: new_owner.clone(),
                        })
                    };
                    Ok(Some(true))
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                *lock_mode = LockMode::Exclusive(Owner {
                    anchor: new_owner.clone(),
                });
                Ok(Some(true))
            }
            Relationship::Linearizable => {
                // `Exclusive` is the least strongest among `Exclusive`, `Created`, and `Deleted.
                Ok(Some(false))
            }
            Relationship::Concurrent => {
                // TODO: intra-transaction deadlock - need to check this.
                Err(Error::Deadlock)
            }
            Relationship::Unknown => {
                if deadline.is_some() {
                    *lock_mode = Self::augment_wait_queue(is_created, is_deleted, owner.clone());
                    Ok(None)
                } else {
                    // No deadline is specified.
                    Err(Error::SerializationFailure)
                }
            }
        }
    }

    /// Takes or exclusive ownership of the exclusively owned `awaitable` database object.
    fn take_exclusively_owned_awaitable_to_own(
        lock_mode: &mut LockMode<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (exclusive_awaitable, is_created, is_deleted) =
            Self::exclusive_awaitable_access_defails(lock_mode)?;

        // The state of the owner needs to be checked.
        match exclusive_awaitable.owner.grant_write_access(new_owner) {
            Relationship::Committed(commit_instant) => {
                if is_deleted {
                    return Err(Error::SerializationFailure);
                } else if exclusive_awaitable.wait_queue.is_empty() {
                    // The transaction was committed and no transactions are waiting for the
                    // database object.
                    let creation_instant = if is_created {
                        commit_instant
                    } else {
                        exclusive_awaitable.creation_instant
                    };
                    *lock_mode =
                        LockMode::ExclusiveAwaitable(ExclusiveAwaitable::with_instant_and_owner(
                            creation_instant,
                            Owner {
                                anchor: new_owner.clone(),
                            },
                        ));
                    return Ok(Some(true));
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                if exclusive_awaitable.wait_queue.is_empty() {
                    *lock_mode = LockMode::Exclusive(Owner {
                        anchor: new_owner.clone(),
                    });
                    return Ok(Some(true));
                }
            }
            Relationship::Linearizable => {
                // `Exclusive` is the least strongest among `Exclusive`, `Created`, and `Deleted.
                return Ok(Some(false));
            }
            Relationship::Concurrent => {
                // TODO: intra-transaction deadlock - need to check this.
                return Err(Error::Deadlock);
            }
            Relationship::Unknown => (),
        };

        if deadline.is_some() {
            Ok(None)
        } else {
            // No deadline is specified.
            Err(Error::SerializationFailure)
        }
    }

    /// Takes exclusive ownership of the exclusively owned database object to delete it.
    ///
    /// Returns `Ok(Some(result))` if the [`Journal`] got access to the database object.
    fn take_exclusively_owned_to_delete(
        lock_mode: &mut LockMode<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (owner, is_created, is_deleted) = match lock_mode {
            LockMode::Created(owner) => (owner, true, false),
            LockMode::Exclusive(owner) => (owner, false, false),
            LockMode::Deleted(owner) => (owner, false, true),
            _ => return Err(Error::WrongParameter),
        };

        // The state of the owner needs to be checked.
        match owner.grant_write_access(new_owner) {
            Relationship::Committed(commit_instant) => {
                if is_deleted {
                    Err(Error::SerializationFailure)
                } else {
                    *lock_mode = if is_created {
                        LockMode::DeletedAwaitable(ExclusiveAwaitable::with_instant_and_owner(
                            commit_instant,
                            Owner {
                                anchor: new_owner.clone(),
                            },
                        ))
                    } else {
                        LockMode::Deleted(Owner {
                            anchor: new_owner.clone(),
                        })
                    };
                    Ok(Some(true))
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                *lock_mode = LockMode::Deleted(Owner {
                    anchor: new_owner.clone(),
                });
                Ok(Some(true))
            }
            Relationship::Linearizable => {
                // TODO: lock-promotion.
                Err(Error::SerializationFailure)
            }
            Relationship::Concurrent => {
                // TODO: intra-transaction deadlock - need to check this.
                Err(Error::Deadlock)
            }
            Relationship::Unknown => {
                if deadline.is_some() {
                    *lock_mode = Self::augment_wait_queue(is_created, is_deleted, owner.clone());
                    Ok(None)
                } else {
                    // No deadline is specified.
                    Err(Error::SerializationFailure)
                }
            }
        }
    }

    /// Takes exclusive ownership of the exclusively owned `awaitable` database object to delete it.
    fn take_exclusively_owned_awaitable_to_delete(
        lock_mode: &mut LockMode<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (exclusive_awaitable, is_created, is_deleted) =
            Self::exclusive_awaitable_access_defails(lock_mode)?;

        // The state of the owner needs to be checked.
        match exclusive_awaitable.owner.grant_write_access(new_owner) {
            Relationship::Committed(commit_instant) => {
                if is_deleted {
                    return Err(Error::SerializationFailure);
                } else if exclusive_awaitable.wait_queue.is_empty() {
                    // The transaction was committed and no transactions are waiting for the
                    // database object.
                    let creation_instant = if is_created {
                        commit_instant
                    } else {
                        exclusive_awaitable.creation_instant
                    };
                    *lock_mode =
                        LockMode::DeletedAwaitable(ExclusiveAwaitable::with_instant_and_owner(
                            creation_instant,
                            Owner {
                                anchor: new_owner.clone(),
                            },
                        ));
                    return Ok(Some(true));
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                if exclusive_awaitable.wait_queue.is_empty() {
                    *lock_mode = LockMode::Deleted(Owner {
                        anchor: new_owner.clone(),
                    });
                    return Ok(Some(true));
                }
            }
            Relationship::Linearizable => {
                // TODO: lock-promotion.
                return Err(Error::SerializationFailure);
            }
            Relationship::Concurrent => {
                // TODO: intra-transaction deadlock - need to check this.
                return Err(Error::Deadlock);
            }
            Relationship::Unknown => (),
        };

        if deadline.is_some() {
            Ok(None)
        } else {
            // No deadline is specified.
            Err(Error::SerializationFailure)
        }
    }

    /// Extracts exclusive access details from the [`LockMode`].
    ///
    /// Returns an [`Error`] if the supplied [`LockMode`] is wrong.
    fn exclusive_awaitable_access_defails(
        lock_mode: &mut LockMode<S>,
    ) -> Result<(&mut ExclusiveAwaitable<S>, bool, bool), Error> {
        let (exclusive_awaitable, is_created, is_deleted) = match lock_mode {
            LockMode::CreatedAwaitable(exclusive_awaitable) => (exclusive_awaitable, true, false),
            LockMode::ExclusiveAwaitable(exclusive_awaitable) => {
                (exclusive_awaitable, false, false)
            }
            LockMode::DeletedAwaitable(exclusive_awaitable) => (exclusive_awaitable, false, true),
            _ => return Err(Error::WrongParameter),
        };
        Ok((exclusive_awaitable, is_created, is_deleted))
    }

    /// Augments [`WaitQueue`]
    fn augment_wait_queue(is_created: bool, is_deleted: bool, owner: Owner<S>) -> LockMode<S> {
        if is_created {
            LockMode::CreatedAwaitable(ExclusiveAwaitable::with_owner(owner))
        } else if is_deleted {
            LockMode::DeletedAwaitable(ExclusiveAwaitable::with_owner(owner))
        } else {
            LockMode::ExclusiveAwaitable(ExclusiveAwaitable::with_owner(owner))
        }
    }
}

impl<S: Sequencer> Clone for Owner<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            anchor: self.anchor.clone(),
        }
    }
}

impl<S: Sequencer> Deref for Owner<S> {
    type Target = JournalAnchor<S>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.anchor
    }
}

impl<S: Sequencer> Owner<S> {
    /// Sets the resource acquisition result.
    ///
    /// Returns `None` if the result was previously set.
    fn set_result(&self, result: Result<bool, Error>) -> Option<()> {
        if let Ok(mut r) = self.access_request_result_placeholder().lock() {
            if let Err(error) = r.set_result(result) {
                // `Error::Timeout` can be set by the requester.
                debug_assert_eq!(error, Error::Timeout);
                return None;
            }
        }
        Some(())
    }

    /// Checks if a resource acquisition result was set.
    fn is_result_set(&self) -> bool {
        if let Ok(r) = self.access_request_result_placeholder().lock() {
            r.is_result_set()
        } else {
            false
        }
    }
}

impl<S: Sequencer> Eq for Owner<S> {}

impl<'d, 't, S: Sequencer, P: PersistenceLayer<S>> From<&mut Journal<'d, 't, S, P>> for Owner<S> {
    #[inline]
    fn from(journal: &mut Journal<'d, 't, S, P>) -> Self {
        Owner {
            anchor: journal.anchor().clone(),
        }
    }
}

impl<S: Sequencer> Ord for Owner<S> {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.anchor.as_ptr().cmp(&other.anchor.as_ptr())
    }
}

impl<S: Sequencer> PartialEq for Owner<S> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.anchor.as_ptr() == other.anchor.as_ptr()
    }
}

impl<S: Sequencer> PartialOrd for Owner<S> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.anchor.as_ptr().partial_cmp(&other.anchor.as_ptr())
    }
}

impl<S: Sequencer> Request<S> {
    /// Returns a reference to its `owner` field.
    fn owner(&self) -> &Owner<S> {
        match self {
            Request::Create(_, owner)
            | Request::Share(_, owner)
            | Request::Lock(_, owner)
            | Request::Delete(_, owner) => owner,
        }
    }
}

impl<S: Sequencer> Clone for Request<S> {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            Self::Create(instant, owner) => Self::Create(*instant, owner.clone()),
            Self::Share(instant, owner) => Self::Share(*instant, owner.clone()),
            Self::Lock(instant, owner) => Self::Lock(*instant, owner.clone()),
            Self::Delete(instant, owner) => Self::Delete(*instant, owner.clone()),
        }
    }
}

impl<S: Sequencer> SharedAwaitable<S> {
    /// Creates a new [`ExclusiveAwaitable`] with a single owner inserted.
    fn with_owner(owner: Owner<S>) -> Box<SharedAwaitable<S>> {
        let mut owner_set = BTreeSet::new();
        owner_set.insert(owner);
        Box::new(SharedAwaitable {
            creation_instant: S::Instant::default(),
            owner_set,
            wait_queue: WaitQueue::default(),
        })
    }

    /// Creates a new [`SharedAwaitable`] with a single owner inserted and a wait queue set.
    fn with_owner_and_wait_queue(
        owner: Owner<S>,
        wait_queue: WaitQueue<S>,
    ) -> Box<SharedAwaitable<S>> {
        let mut owner_set = BTreeSet::new();
        owner_set.insert(owner);
        Box::new(SharedAwaitable {
            creation_instant: S::Instant::default(),
            owner_set,
            wait_queue,
        })
    }

    /// Creates a new [`SharedAwaitable`] with a single owner inserted.
    fn with_instant_and_owner(
        creation_instant: S::Instant,
        owner: Owner<S>,
    ) -> Box<SharedAwaitable<S>> {
        let mut owner_set = BTreeSet::new();
        owner_set.insert(owner);
        Box::new(SharedAwaitable {
            creation_instant,
            owner_set,
            wait_queue: WaitQueue::default(),
        })
    }

    /// Cleans up committed and rolled back owners.
    ///
    /// Returns `true` if the [`SharedAwaitable`] got empty.
    fn cleanup_inactive_owners(&mut self) -> bool {
        self.owner_set.retain(|o| {
            // Delete the entry if the owner definitely does not hold the shared ownership.
            !o.is_terminated()
        });
        self.owner_set.is_empty()
    }

    /// Pushes a request into the wait queue.
    fn push_request(&mut self, request: Request<S>) {
        self.wait_queue.push_back(request);
        self.owner_set
            .iter()
            .for_each(|o| o.need_to_wake_up_others());
    }
}

impl<S: Sequencer> ExclusiveAwaitable<S> {
    /// Creates a new [`ExclusiveAwaitable`] with a single owner inserted.
    fn with_owner(owner: Owner<S>) -> Box<ExclusiveAwaitable<S>> {
        Box::new(ExclusiveAwaitable {
            creation_instant: S::Instant::default(),
            owner,
            promotion_data: None,
            wait_queue: WaitQueue::default(),
        })
    }

    /// Creates a new [`ExclusiveAwaitable`] with a single owner inserted and a wait queue set.
    fn with_owner_and_wait_queue(
        owner: Owner<S>,
        wait_queue: WaitQueue<S>,
    ) -> Box<ExclusiveAwaitable<S>> {
        Box::new(ExclusiveAwaitable {
            creation_instant: S::Instant::default(),
            owner,
            promotion_data: None,
            wait_queue,
        })
    }

    /// Creates a new [`ExclusiveAwaitable`] with a single owner inserted and the creation instant
    /// set.
    fn with_instant_and_owner(
        creation_instant: S::Instant,
        owner: Owner<S>,
    ) -> Box<ExclusiveAwaitable<S>> {
        Box::new(ExclusiveAwaitable {
            creation_instant,
            owner,
            promotion_data: None,
            wait_queue: WaitQueue::default(),
        })
    }

    /// Pushes a request into the wait queue.
    fn push_request(&mut self, request: Request<S>) {
        self.wait_queue.push_back(request);
        self.owner.need_to_wake_up_others();
    }
}

impl<S: Sequencer> WaitQueue<S> {
    /// Pops the oldest request.
    fn clone_oldest(&self) -> Option<Request<S>> {
        self.front().map(Clone::clone)
    }

    /// Removes the oldest request.
    fn remove_oldest(&mut self) {
        self.pop_front();
    }

    /// Inherits other [`WaitQueue`].
    ///
    /// Returns `true` if `self` is empty.
    fn inherit(&mut self, other: Option<&mut WaitQueue<S>>) -> bool {
        if let Some(other) = other {
            other.iter().for_each(|r| {
                self.push_back(r.clone());
            });
            other.drain(..);
        }
        self.is_empty()
    }
}

impl<S: Sequencer> Deref for WaitQueue<S> {
    type Target = VecDeque<Request<S>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S: Sequencer> DerefMut for WaitQueue<S> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S: Sequencer> Drop for WaitQueue<S> {
    fn drop(&mut self) {
        self.0.drain(..).for_each(|r| {
            // The wait queue is being dropped due to memory allocation failure.
            r.owner().set_result(Err(Error::OutOfMemory));
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{AtomicCounter, Database};
    use std::time::Duration;

    static_assertions::assert_eq_size!(ObjectState<AtomicCounter>, [u8; 16]);

    const TIMEOUT_UNEXPECTED: Duration = Duration::from_secs(256);
    const TIMEOUT_EXPECTED: Duration = Duration::from_millis(256);

    impl ToObjectID for usize {
        fn to_object_id(&self) -> usize {
            *self
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TransactionAction {
        Commit,
        Rewind,
        Rollback,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum AccessAction {
        Create,
        Share,
        Lock,
        Delete,
    }

    async fn take_access_action<S: Sequencer, P: PersistenceLayer<S>>(
        access_action: AccessAction,
        access_controller: &AccessController<S>,
        journal: &mut Journal<'_, '_, S, P>,
    ) -> Result<bool, Error> {
        match access_action {
            AccessAction::Create => access_controller.create(&0, journal, None).await,
            AccessAction::Share => access_controller.share(&0, journal, None).await,
            AccessAction::Lock => access_controller.lock(&0, journal, None).await,
            AccessAction::Delete => access_controller.delete(&0, journal, None).await,
        }
    }

    #[tokio::test]
    async fn read_snapshot() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let mut snapshot = None;
        for access_action in [
            AccessAction::Create,
            AccessAction::Share,
            AccessAction::Lock,
            AccessAction::Share,
            AccessAction::Delete,
        ] {
            let transaction = database.transaction();
            let mut journal = transaction.journal();
            assert_eq!(
                take_access_action(access_action, access_controller, &mut journal).await,
                Ok(true)
            );
            let journal_snapshot = journal.snapshot();
            assert_eq!(
                access_controller
                    .read(
                        &0,
                        &journal_snapshot,
                        Some(Instant::now() + TIMEOUT_UNEXPECTED),
                    )
                    .await,
                Ok(access_action != AccessAction::Delete),
            );
            assert_eq!(journal.submit(), 1);
            let transaction_snapshot = transaction.snapshot();
            assert_eq!(
                access_controller
                    .read(
                        &0,
                        &transaction_snapshot,
                        Some(Instant::now() + TIMEOUT_UNEXPECTED),
                    )
                    .await,
                Ok(access_action != AccessAction::Delete),
            );
            assert!(transaction.commit().await.is_ok());
            let database_snapshot = database.snapshot();
            assert_eq!(
                access_controller
                    .read(
                        &0,
                        &database_snapshot,
                        Some(Instant::now() + TIMEOUT_UNEXPECTED),
                    )
                    .await,
                Ok(access_action != AccessAction::Delete),
            );
            if snapshot.is_none() {
                snapshot.replace(database_snapshot);
            }
        }
        assert_eq!(
            access_controller
                .read(
                    &0,
                    &database.snapshot(),
                    Some(Instant::now() + TIMEOUT_UNEXPECTED),
                )
                .await,
            Ok(false),
        );
        assert_eq!(
            access_controller
                .read(
                    &0,
                    &snapshot.unwrap(),
                    Some(Instant::now() + TIMEOUT_UNEXPECTED),
                )
                .await,
            Ok(true),
        );
    }

    #[tokio::test]
    async fn read_timeout() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert_eq!(
            access_controller.create(&0, &mut journal, None).await,
            Ok(true)
        );
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();
        assert!(database.transaction().commit().await.is_ok());
        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_EXPECTED),)
                .await,
            Err(Error::Timeout),
        );
        drop(prepared);
    }

    #[tokio::test]
    async fn read_rolled_back() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert_eq!(
            access_controller.create(&0, &mut journal, None).await,
            Ok(true)
        );
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();
        let snapshot_old = database.snapshot();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        let (deleted, _) = futures::join!(
            access_controller.delete(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            async { drop(prepared) },
        );
        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(true),
        );
        assert_eq!(deleted, Ok(true));
        assert_eq!(
            access_controller
                .read(&0, &snapshot_old, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(true),
        );
    }

    #[tokio::test]
    async fn create_commit_wait_read() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert_eq!(
            access_controller.create(&0, &mut journal, None).await,
            Ok(true)
        );
        assert_eq!(journal.submit(), 1);
        let _prepared = transaction.prepare().await.unwrap();
        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(false),
        );
    }

    #[tokio::test]
    async fn create_wait_timeout_rollback_create() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert_eq!(
            access_controller.create(&0, &mut journal, None).await,
            Ok(true)
        );
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert_eq!(
            access_controller
                .create(&0, &mut journal, Some(Instant::now() + TIMEOUT_EXPECTED))
                .await,
            Err(Error::Timeout)
        );
        drop(prepared);
        assert_eq!(
            access_controller
                .create(&0, &mut journal, Some(Instant::now() + TIMEOUT_EXPECTED))
                .await,
            Ok(true),
        );
    }

    #[tokio::test]
    async fn create_rollback_read() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .create(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();
        drop(prepared);

        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(false)
        );
    }

    #[tokio::test]
    async fn access_tx_access() {
        for serial_execution in [false, true] {
            for access_action in [
                AccessAction::Create,
                AccessAction::Share,
                AccessAction::Lock,
                AccessAction::Delete,
            ] {
                for transaction_action in [
                    TransactionAction::Commit,
                    TransactionAction::Rewind,
                    TransactionAction::Rollback,
                ] {
                    for post_access_action in [
                        AccessAction::Create,
                        AccessAction::Share,
                        AccessAction::Lock,
                        AccessAction::Delete,
                    ] {
                        let database = Database::default();
                        let access_controller = database.access_controller();
                        let transaction = database.transaction();
                        let mut journal = transaction.journal();
                        assert_eq!(
                            take_access_action(access_action, access_controller, &mut journal)
                                .await,
                            Ok(true)
                        );
                        if transaction_action == TransactionAction::Rewind {
                            drop(journal);
                        } else {
                            assert_eq!(journal.submit(), 1);
                        }
                        let transaction_action_runner = async {
                            if transaction_action == TransactionAction::Commit {
                                assert!(transaction.commit().await.is_ok());
                            } else if transaction_action == TransactionAction::Rollback {
                                transaction.rollback().await;
                            }
                        };
                        let post_action_runner = async {
                            let transaction = database.transaction();
                            let mut journal = transaction.journal();
                            take_access_action(post_access_action, access_controller, &mut journal)
                                .await
                        };

                        let result = if serial_execution {
                            futures::join!(transaction_action_runner, post_action_runner).1
                        } else {
                            transaction_action_runner.await;
                            post_action_runner.await
                        };

                        if ((access_action != AccessAction::Create
                            || transaction_action == TransactionAction::Commit)
                            && post_access_action == AccessAction::Create)
                            || (access_action == AccessAction::Delete
                                && transaction_action == TransactionAction::Commit)
                        {
                            assert_eq!(
                                result,
                                Err(Error::SerializationFailure),
                                "{access_action:?} {transaction_action:?} {post_access_action:?}"
                            );
                        } else {
                            assert_eq!(
                                result,
                                Ok(true),
                                "{access_action:?} {transaction_action:?} {post_access_action:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}
