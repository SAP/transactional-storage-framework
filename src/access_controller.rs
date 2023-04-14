// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::AccessRequestResult;
use super::journal::Anchor as JournalAnchor;
use super::journal::{AwaitResponse, Relationship};
use super::{Error, Journal, PersistenceLayer, Sequencer, Snapshot};
use scc::hash_map::Entry as MapEntry;
use scc::{ebr, HashMap};
use std::cmp;
use std::collections::{BTreeSet, VecDeque};
use std::mem::take;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;

/// [`AccessController`] grants or rejects access to a database object identified as a [`usize`]
/// value.
///
/// [`AccessController`] provides an essential database object access control method for database
/// systems using multi-version concurrency control or read-write locks. As long as a database
/// object can be uniquely identified through [`ToObjectID`], read or write access to the object
/// can be fully transactionally controlled via [`AccessController`].
///
/// [`AccessController`] exactly acts as a transactional [`std::sync::RwLock`] when only
/// [`AccessController::share`] and [`AccessController::lock`] are used; those two methods are
/// suitable for lock-based concurrency control protocols.
///
/// In addition to the aforementioned methods, [`AccessController::create`] and
/// [`AccessController::delete`] can be used when the database system needs to denote the lifetime
/// of a database object; this information is crucial for multi-version concurrency control
/// protocols since the database system must not let a database object be read by readers that
/// started before the creation or after the deletion of the object.
///
/// # Examples
///
/// ```
/// use sap_tsf::{Database, ToObjectID};
///
/// // `O` represents a database object type.
/// struct O(usize);
///
/// // `ToObjectID` is implemented for `O`.
/// impl ToObjectID for O {
///     fn to_object_id(&self) -> usize {
///         self.0
///    }
/// }
///
/// let database = Database::default();
/// let access_controller = database.access_controller();
///
/// async {
///     let transaction = database.transaction();
///     let mut journal = transaction.journal();
///
///     // Let the `Database` know that the database object will be created.
///     assert!(access_controller.create(&O(1), &mut journal, None).await.is_ok());
///     journal.submit();
///
///     // The transaction writes its own commit instant value onto the access data of the database
///     // object, and future readers and writers will be able to gain access to it.
///     assert!(transaction.commit().await.is_ok());
///
///     let snapshot = database.snapshot();
///     assert_eq!(access_controller.read(&O(1), &snapshot, None).await, Ok(true));
///
///     let transaction_succ = database.transaction();
///     let mut journal_succ = transaction_succ.journal();
///
///     // The transaction will own the database object to prevent any other transactions from
///     // gaining write access to it.
///     assert!(access_controller.share(&O(1), &mut journal_succ, None).await.is_ok());
///     assert_eq!(journal_succ.submit(), 1);
///
///     let transaction_fail = database.transaction();
///     let mut journal_fail = transaction_fail.journal();

///     // Another transaction fails to take ownership of the database object.
///     assert!(access_controller.lock(&O(1), &mut journal_fail, None).await.is_err());
///
///     let mut journal_delete = transaction_succ.journal();
//
///     // The transaction will delete the database object.
///     assert!(access_controller.delete(&O(1), &mut journal_delete, None).await.is_ok());
///     assert_eq!(journal_delete.submit(), 2);
///
///     // The transaction deletes the database object by writing its commit instant value onto
///     // the access data associated with the database object.
///     assert!(transaction_succ.commit().await.is_ok());
///
///     // Further access to the database object is prohibited.
///     assert!(access_controller.share(&O(1), &mut journal_fail, None).await.is_err());
/// };
/// ```
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

/// An owner of a database object.
#[derive(Debug)]
pub(super) struct Owner<S: Sequencer> {
    /// The journal is directly pointed to by [`Owner`].
    anchor: ebr::Arc<JournalAnchor<S>>,
}

/// Possible states of a database object.
#[derive(Debug)]
pub(super) enum ObjectState<S: Sequencer> {
    /// The database object is owned by one or more transactions.
    Owned(Ownership<S>),

    /// The database object was created at the instant.
    Created(S::Instant),

    /// The database object was deleted at the instant.
    Deleted(S::Instant),
}

/// Types of ownership of a database object.
#[derive(Debug)]
pub(super) enum Ownership<S: Sequencer> {
    /// The database object is being created.
    Created(Owner<S>),

    /// The database object is being created, and there can be transactions trying to create the
    /// same database object.
    CreatedAwaitable(Box<ExclusiveAwaitable<S>>),

    /// A single transaction is protecting the database object from being modified .
    Protected(Owner<S>),

    /// One of more transactions are protecting the database object from being modified, and there
    /// can be transactions trying to gain access to the database object.
    ProtectedAwaitable(Box<SharedAwaitable<S>>),

    /// The database object is locked by the transaction.
    Locked(Owner<S>),

    /// The database object is locked by the transaction, and there can be transactions trying to
    /// gain access to the database object.
    LockedAwaitable(Box<ExclusiveAwaitable<S>>),

    /// The database object is being deleted by the transaction.
    Deleted(Owner<S>),

    /// The database object is being deleted by the transaction, and there can be transactions
    /// trying to gain access to the database object.
    DeletedAwaitable(Box<ExclusiveAwaitable<S>>),
}

/// Types of ownership transfer requests.
///
/// The wall-clock time instant when the request was made is stored in it, and the value is used by
/// the deadlock detector.
#[derive(Debug)]
pub(super) enum Request<S: Sequencer> {
    /// Requests to create the database object.
    Create(Instant, Owner<S>, Arc<AccessRequestResult>),

    /// Requests to protect the database object.
    Protect(Instant, Owner<S>, Arc<AccessRequestResult>),

    /// Requests to lock the database object.
    Lock(Instant, Owner<S>, Arc<AccessRequestResult>),

    /// Requests to delete the database object.
    Delete(Instant, Owner<S>, Arc<AccessRequestResult>),
}

/// [`SharedAwaitable`] contains multiple owners and a wait queue.
#[derive(Debug)]
pub(super) struct SharedAwaitable<S: Sequencer> {
    /// The commit instant of the transaction that created the database object.
    ///
    /// The value equals to `S::Instant::default()` if the database object is globally visible.
    creation_instant: S::Instant,

    /// A set of owners.
    owner_set: BTreeSet<Owner<S>>,

    /// The wait queue of the database object.
    wait_queue: WaitQueue<S>,
}

/// [`ExclusiveAwaitable`] contains one exclusive owner and a wait queue.
#[derive(Debug)]
pub(super) struct ExclusiveAwaitable<S: Sequencer> {
    /// The commit instant of the transaction that created the database object.
    ///
    /// The value equals to `S::Instant::default()` if the database object is globally visible.
    creation_instant: S::Instant,

    /// The only owner.
    owner: Owner<S>,

    /// The previous ownership before the current owner took the database object.
    ///
    /// This field is only used when ownership is promoted within the same transaction.
    prior_ownership: Option<Box<Ownership<S>>>,

    /// The wait queue of the database object.
    wait_queue: WaitQueue<S>,
}

/// The access request wait queue for a database object.
#[derive(Debug, Default)]
struct WaitQueue<S: Sequencer>(VecDeque<Request<S>>);

impl<S: Sequencer> AccessController<S> {
    /// Reads the database object.
    ///
    /// This method is used by a database system using multi-version concurrency control. It
    /// compares the sequencer instant and transaction information in the supplied [`Snapshot`]
    /// with the creation or deletion sequencer instant, or the owner contained in the access
    /// control data, and then returns `true` if the [`Snapshot`] is eligible to read the database
    /// object. If the creation or deletion sequencer instant cannot be immediately read, e.g., the
    /// transaction is being committed at the moment, it will wait for the sequencer instant value
    /// corresponding to the transaction commit operation to be determined until the specified
    /// timeout is reached.
    ///
    /// This method just returns `true` if no access control is defined for the database object;
    /// therefore any access control mapping must be removed only if the corresponding database
    /// object can always be visible to all the readers, or the database object has become totally
    /// unreachable to readers after being logically deleted.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the specified deadline was reached or memory allocation failed
    /// when pushing a [`Waker`](std::task::Waker) into the owner
    /// [`Transaction`](super::Transaction) if the transaction is being committed.
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
                    ObjectState::Owned(ownership) => {
                        match ownership {
                            Ownership::Created(owner) => {
                                // The database object is being created.
                                owner.grant_read_access(snapshot, deadline)
                            }
                            Ownership::CreatedAwaitable(exclusive_awaitable) => {
                                // The database object is being created.
                                exclusive_awaitable
                                    .owner
                                    .grant_read_access(snapshot, deadline)
                            }
                            Ownership::Protected(_) | Ownership::Locked(_) => {
                                // The database object is temporarily owned, but globally visible.
                                Ok(true)
                            }
                            Ownership::ProtectedAwaitable(shared_awaitable) => {
                                // The database object is temporarily protected, but the creation
                                // instant has to be checked.
                                Ok(*snapshot >= shared_awaitable.creation_instant)
                            }
                            Ownership::LockedAwaitable(exclusive_awaitable) => {
                                // The database object is temporarily locked, but the creation
                                // instant has to be checked.
                                Ok(*snapshot >= exclusive_awaitable.creation_instant)
                            }
                            Ownership::Deleted(owner) => {
                                // The database object is being deleted.
                                //
                                // The result should be negated since seeing the deletion means not
                                // seeing the database object.
                                owner.grant_read_access(snapshot, deadline).map(|r| !r)
                            }
                            Ownership::DeletedAwaitable(exclusive_awaitable) => {
                                if *snapshot >= exclusive_awaitable.creation_instant {
                                    // The database object is being deleted.
                                    exclusive_awaitable
                                        .owner
                                        .anchor
                                        .grant_read_access(snapshot, deadline)
                                        .map(|r| !r)
                                } else {
                                    // The deletion cannot be seen.
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
    /// The access control data is atomically converted into a time point data when the transaction
    /// is committed, so that readers can check if they have the read access right to the data, and
    /// no other transactions can create the same database object.
    ///
    /// Transactions may compete for the same database object, e.g., when the new database object
    /// becomes reachable via an index before being visible to database readers, however at most
    /// one of competing transactions can create access control data corresponding to the database
    /// object.
    ///
    /// The access control data can be deleted from the [`AccessController`] when the information
    /// is no longer needed, e.g., all the readers are allowed to access the database object; in
    /// this case, [`AccessController`] does not prohibit other transactions to successfully create
    /// new access control data for the same database object which may lead to a read consistency
    /// issue. Therefore, transactions must have checked whether the database object is accessible
    /// beforehand.
    ///
    /// `true` is returned if new access control data was created, and `false` if another
    /// transaction has already created the data.
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
                entry.insert_entry(ObjectState::Owned(Ownership::Created(Owner::from(journal))));
                return Ok(true);
            }
        };

        let result = Self::try_create(entry.get_mut(), journal.anchor(), deadline)?;
        if let Some(result) = result {
            return Ok(result);
        } else if let (
            Some(deadline),
            ObjectState::Owned(Ownership::CreatedAwaitable(exclusive_awaitable)),
        ) = (deadline, entry.get_mut())
        {
            let overseer = journal.overseer();
            let result_placeholder = Arc::new(AccessRequestResult::default());
            let request = Request::Create(
                Instant::now(),
                Owner::from(journal),
                result_placeholder.clone(),
            );
            exclusive_awaitable.push_request(request);
            return AwaitResponse::new(entry, overseer, deadline, result_placeholder).await;
        }

        // The database object has been created, deleted, or invisible.
        Err(Error::SerializationFailure)
    }

    /// Acquires a shared lock on the database object to protect it against modification attempts.
    ///
    /// Returns `true` if the journal successfully acquired a new shared lock. `false` is returned
    /// if the transaction already has ownership of the database object.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if the transaction failed to protect the database object, memory
    /// allocation failed, or the specified deadline was reached.
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
                entry.insert_entry(ObjectState::Owned(Ownership::Protected(Owner::from(
                    journal,
                ))));
                return Ok(true);
            }
        };

        let result = Self::try_share(entry.get_mut(), journal.anchor(), deadline)?;
        if let Some(result) = result {
            return Ok(result);
        } else if let ObjectState::Owned(ownership) = entry.get_mut() {
            match ownership {
                Ownership::CreatedAwaitable(exclusive_awaitable)
                | Ownership::LockedAwaitable(exclusive_awaitable)
                | Ownership::DeletedAwaitable(exclusive_awaitable) => {
                    if let Some(deadline) = deadline {
                        let overseer = journal.overseer();
                        let result_placeholder = Arc::new(AccessRequestResult::default());
                        let request = Request::Protect(
                            Instant::now(),
                            Owner::from(journal),
                            result_placeholder.clone(),
                        );
                        exclusive_awaitable.push_request(request);
                        return AwaitResponse::new(entry, overseer, deadline, result_placeholder)
                            .await;
                    }
                }
                Ownership::ProtectedAwaitable(shared_awaitable) => {
                    if let Some(deadline) = deadline {
                        let overseer = journal.overseer();
                        let result_placeholder = Arc::new(AccessRequestResult::default());
                        let request = Request::Protect(
                            Instant::now(),
                            Owner::from(journal),
                            result_placeholder.clone(),
                        );
                        shared_awaitable.push_request(request);
                        return AwaitResponse::new(entry, overseer, deadline, result_placeholder)
                            .await;
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
    /// Returns `true` if the journal successfully acquired the exclusive lock. `false` is returned
    /// if the transaction already has exclusive ownership of the database object.
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
                entry.insert_entry(ObjectState::Owned(Ownership::Locked(Owner::from(journal))));
                return Ok(true);
            }
        };

        let result = Self::try_lock(entry.get_mut(), journal.anchor(), deadline)?;
        if let Some(result) = result {
            return Ok(result);
        } else if let ObjectState::Owned(ownership) = entry.get_mut() {
            match ownership {
                Ownership::CreatedAwaitable(exclusive_awaitable)
                | Ownership::LockedAwaitable(exclusive_awaitable)
                | Ownership::DeletedAwaitable(exclusive_awaitable) => {
                    if let Some(deadline) = deadline {
                        let overseer = journal.overseer();
                        let result_placeholder = Arc::new(AccessRequestResult::default());
                        let request = Request::Lock(
                            Instant::now(),
                            Owner::from(journal),
                            result_placeholder.clone(),
                        );
                        exclusive_awaitable.push_request(request);
                        return AwaitResponse::new(entry, overseer, deadline, result_placeholder)
                            .await;
                    }
                }
                Ownership::ProtectedAwaitable(shared_awaitable) => {
                    if let Some(deadline) = deadline {
                        let overseer = journal.overseer();
                        let result_placeholder = Arc::new(AccessRequestResult::default());
                        let request = Request::Lock(
                            Instant::now(),
                            Owner::from(journal),
                            result_placeholder.clone(),
                        );
                        shared_awaitable.push_request(request);
                        return AwaitResponse::new(entry, overseer, deadline, result_placeholder)
                            .await;
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
    /// The access control data is atomically converted into a time point data when the transaction
    /// is committed, so that readers can check if they have the read access right to the data. No
    /// other transactions are allowed to modify the access control data after it was deleted.
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
                entry.insert_entry(ObjectState::Owned(Ownership::Deleted(Owner::from(journal))));
                return Ok(true);
            }
        };

        let result = Self::try_delete(entry.get_mut(), journal.anchor(), deadline)?;
        if let Some(result) = result {
            return Ok(result);
        } else if let ObjectState::Owned(ownership) = entry.get_mut() {
            match ownership {
                Ownership::CreatedAwaitable(exclusive_awaitable)
                | Ownership::LockedAwaitable(exclusive_awaitable)
                | Ownership::DeletedAwaitable(exclusive_awaitable) => {
                    if let Some(deadline) = deadline {
                        let overseer = journal.overseer();
                        let result_placeholder = Arc::new(AccessRequestResult::default());
                        let request = Request::Delete(
                            Instant::now(),
                            Owner::from(journal),
                            result_placeholder.clone(),
                        );
                        exclusive_awaitable.push_request(request);
                        return AwaitResponse::new(entry, overseer, deadline, result_placeholder)
                            .await;
                    }
                }
                Ownership::ProtectedAwaitable(shared_awaitable) => {
                    if let Some(deadline) = deadline {
                        // Wait for the database resource to be available to the transaction.
                        let overseer = journal.overseer();
                        let result_placeholder = Arc::new(AccessRequestResult::default());
                        let request = Request::Delete(
                            Instant::now(),
                            Owner::from(journal),
                            result_placeholder.clone(),
                        );
                        shared_awaitable.push_request(request);
                        return AwaitResponse::new(entry, overseer, deadline, result_placeholder)
                            .await;
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
                object_state.cleanup_state();
                let wait_queue = if let ObjectState::Owned(ownership) = object_state {
                    match ownership {
                        Ownership::Created(_)
                        | Ownership::Protected(_)
                        | Ownership::Locked(_)
                        | Ownership::Deleted(_) => None,
                        Ownership::CreatedAwaitable(exclusive_awaitable)
                        | Ownership::LockedAwaitable(exclusive_awaitable)
                        | Ownership::DeletedAwaitable(exclusive_awaitable) => {
                            let wait_queue = take(&mut exclusive_awaitable.wait_queue);
                            Self::process_wait_queue(object_state, wait_queue)
                        }
                        Ownership::ProtectedAwaitable(shared_awaitable) => {
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
            let result_placeholder = match &request {
                Request::Create(_, _, result_placeholder)
                | Request::Protect(_, _, result_placeholder)
                | Request::Lock(_, _, result_placeholder)
                | Request::Delete(_, _, result_placeholder) => result_placeholder,
            };
            if let Some(mut result_waker) = result_placeholder.lock_sync() {
                if result_waker.0.is_some() {
                    // The request was timed out.
                    wait_queue.remove_oldest();
                    continue;
                }
                let (result, new_owner) = match &request {
                    Request::Create(_, new_owner, _) => (
                        Self::try_create(object_state, &new_owner.anchor, Some(Instant::now())),
                        new_owner,
                    ),
                    Request::Protect(_, new_owner, _) => (
                        Self::try_share(object_state, &new_owner.anchor, Some(Instant::now())),
                        new_owner,
                    ),
                    Request::Lock(_, new_owner, _) => (
                        Self::try_lock(object_state, &new_owner.anchor, Some(Instant::now())),
                        new_owner,
                    ),
                    Request::Delete(_, new_owner, _) => (
                        Self::try_delete(object_state, &new_owner.anchor, Some(Instant::now())),
                        new_owner,
                    ),
                };
                match result {
                    Ok(Some(result)) => {
                        // The result is out.
                        result_waker.0.replace(Ok(result));

                        // The new owner also needs to wake up other waiting transactions.
                        new_owner.set_wake_up_others();
                    }
                    Ok(None) => {
                        // The request will be retried later.
                        break;
                    }
                    Err(error) => {
                        // An error was returned.
                        result_waker.0.replace(Err(error));
                    }
                }
                if let Some(waker) = result_waker.1.take() {
                    waker.wake();
                }
            } else {
                // The `Mutex` was poisoned.
                wait_queue.remove_oldest();
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
            ObjectState::Owned(ownership) => match ownership {
                Ownership::Created(owner) => {
                    if let Some(wait_queue) = wait_queue {
                        if wait_queue.is_empty() {
                            return false;
                        }
                        *ownership = Ownership::CreatedAwaitable(
                            ExclusiveAwaitable::with_owner_and_wait_queue(
                                owner.clone(),
                                wait_queue,
                            ),
                        );
                    } else {
                        return false;
                    }
                }
                Ownership::CreatedAwaitable(exclusive_awaitable) => {
                    if exclusive_awaitable.wait_queue.inherit(wait_queue.as_mut()) {
                        if exclusive_awaitable.creation_instant == S::Instant::default() {
                            *ownership = Ownership::Created(exclusive_awaitable.owner.clone());
                        }
                        return false;
                    }
                }
                Ownership::Protected(owner) => {
                    if let Some(wait_queue) = wait_queue {
                        if wait_queue.is_empty() {
                            return false;
                        }
                        *ownership = Ownership::ProtectedAwaitable(
                            SharedAwaitable::with_owner_and_wait_queue(owner.clone(), wait_queue),
                        );
                    } else {
                        return false;
                    }
                }
                Ownership::ProtectedAwaitable(shared_awaitable) => {
                    if shared_awaitable.wait_queue.inherit(wait_queue.as_mut()) {
                        if shared_awaitable.creation_instant == S::Instant::default()
                            && shared_awaitable.owner_set.len() == 1
                        {
                            if let Some(owner) = shared_awaitable.owner_set.pop_first() {
                                *ownership = Ownership::Protected(owner);
                            }
                        }
                        return false;
                    }
                }
                Ownership::Locked(owner) => {
                    if let Some(wait_queue) = wait_queue {
                        if wait_queue.is_empty() {
                            return false;
                        }
                        *ownership = Ownership::LockedAwaitable(
                            ExclusiveAwaitable::with_owner_and_wait_queue(
                                owner.clone(),
                                wait_queue,
                            ),
                        );
                    } else {
                        return false;
                    }
                }
                Ownership::LockedAwaitable(exclusive_awaitable) => {
                    if exclusive_awaitable.wait_queue.inherit(wait_queue.as_mut()) {
                        if exclusive_awaitable.creation_instant == S::Instant::default() {
                            *ownership = Ownership::Locked(exclusive_awaitable.owner.clone());
                        }
                        return false;
                    }
                }
                Ownership::Deleted(owner) => {
                    if let Some(wait_queue) = wait_queue {
                        if wait_queue.is_empty() {
                            return false;
                        }
                        *ownership = Ownership::DeletedAwaitable(
                            ExclusiveAwaitable::with_owner_and_wait_queue(
                                owner.clone(),
                                wait_queue,
                            ),
                        );
                    } else {
                        return false;
                    }
                }
                Ownership::DeletedAwaitable(exclusive_awaitable) => {
                    if exclusive_awaitable.wait_queue.inherit(wait_queue.as_mut()) {
                        if exclusive_awaitable.creation_instant == S::Instant::default() {
                            *ownership = Ownership::Deleted(exclusive_awaitable.owner.clone());
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
    ///
    /// Returns `Ok(None)` if the result will be out after waiting.
    fn try_create(
        object_state: &mut ObjectState<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        object_state.cleanup_state();
        if let ObjectState::Owned(ownership) = object_state {
            let (owner, awaitable) = match ownership {
                Ownership::Created(owner) => (owner, false),
                Ownership::CreatedAwaitable(exclusive_awaitable) => {
                    (&mut exclusive_awaitable.owner, true)
                }
                _ => return Err(Error::SerializationFailure),
            };

            // The state of the owner needs to be checked.
            match owner.grant_write_access(new_owner) {
                Relationship::Committed(_) => {
                    // The transaction was committed, implying that the database object has
                    // been successfully created.
                    return Err(Error::SerializationFailure);
                }
                Relationship::RolledBack => {
                    // The transaction or the owner journal was rolled back.
                    *ownership = Ownership::Created(Owner::new(new_owner));
                    return Ok(Some(true));
                }
                Relationship::Linearizable => {
                    // Already created in a previously submitted journal in the same
                    // transaction.
                    return Ok(Some(false));
                }
                Relationship::Concurrent => {
                    // Multiple journals competing for the same database object is regarded
                    // as a deadlock.
                    return Err(Error::Deadlock);
                }
                Relationship::Unknown => {
                    if deadline.is_some() {
                        if !awaitable {
                            // Prepare for awaiting access to the database object.
                            *ownership = Ownership::CreatedAwaitable(
                                ExclusiveAwaitable::with_owner(owner.clone()),
                            );
                        }
                        return Ok(None);
                    }
                }
            }
        }
        Err(Error::SerializationFailure)
    }

    /// Tries to obtain shared ownership of the database object.
    ///
    /// Returns `Ok(None)` if the result will be out after waiting.
    fn try_share(
        object_state: &mut ObjectState<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        loop {
            object_state.cleanup_state();
            match object_state {
                ObjectState::Owned(ownership) => {
                    match ownership {
                        Ownership::Created(_) | Ownership::Locked(_) | Ownership::Deleted(_) => {
                            return Self::try_share_exclusively_owned(
                                ownership, new_owner, deadline,
                            );
                        }
                        Ownership::CreatedAwaitable(_)
                        | Ownership::LockedAwaitable(_)
                        | Ownership::DeletedAwaitable(_) => {
                            let (result_available, result_or_wait) =
                                Self::try_share_exclusive_awaitable(
                                    ownership, new_owner, deadline,
                                )?;
                            if result_available {
                                return Ok(Some(result_or_wait));
                            } else if result_or_wait {
                                return Ok(None);
                            }
                        }
                        Ownership::Protected(owner) => {
                            if let Relationship::Linearizable = owner.grant_write_access(new_owner)
                            {
                                // The transaction already owns the database object.
                                return Ok(Some(false));
                            }
                            let mut shared_awaitable = SharedAwaitable::with_owner(owner.clone());
                            shared_awaitable.owner_set.insert(Owner::new(new_owner));
                            *ownership = Ownership::ProtectedAwaitable(shared_awaitable);
                            return Ok(Some(true));
                        }
                        Ownership::ProtectedAwaitable(shared_awaitable) => {
                            if shared_awaitable.owner_set.iter().any(|o| {
                                matches!(
                                    o.grant_write_access(new_owner),
                                    Relationship::Linearizable
                                )
                            }) {
                                // The transaction already owns the database object.
                                return Ok(Some(false));
                            } else if shared_awaitable.wait_queue.is_empty() {
                                shared_awaitable.owner_set.insert(Owner::new(new_owner));
                                return Ok(Some(true));
                            } else if deadline.is_some() {
                                return Ok(None);
                            }
                            break;
                        }
                    }
                }
                ObjectState::Created(instant) => {
                    // The database object is not owned or locked.
                    *object_state = ObjectState::Owned(Ownership::ProtectedAwaitable(
                        SharedAwaitable::with_instant_and_owner(*instant, Owner::new(new_owner)),
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
    ///
    /// Returns `Ok(None)` if the result will be out after waiting.
    fn try_lock(
        object_state: &mut ObjectState<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        loop {
            object_state.cleanup_state();
            match object_state {
                ObjectState::Owned(ownership) => match ownership {
                    Ownership::Created(_) | Ownership::Locked(_) | Ownership::Deleted(_) => {
                        return Self::try_lock_exclusively_owned(ownership, new_owner, deadline);
                    }
                    Ownership::CreatedAwaitable(_)
                    | Ownership::LockedAwaitable(_)
                    | Ownership::DeletedAwaitable(_) => {
                        let (result_available, result_or_wait) =
                            Self::try_lock_exclusive_awaitable(ownership, new_owner, deadline)?;
                        if result_available {
                            return Ok(Some(result_or_wait));
                        } else if result_or_wait {
                            return Ok(None);
                        }
                    }
                    Ownership::Protected(owner) => {
                        if let Relationship::Linearizable = owner.grant_write_access(new_owner) {
                            // It is possible to promote the access permission in this transaction.
                            let mut exclusive_awaitable =
                                ExclusiveAwaitable::with_owner(Owner::new(new_owner));
                            exclusive_awaitable
                                .set_prior_state(Ownership::Protected(owner.clone()));
                            *ownership = Ownership::LockedAwaitable(exclusive_awaitable);
                            return Ok(Some(true));
                        } else if owner.is_terminated() {
                            *ownership = Ownership::Locked(Owner::new(new_owner));
                            return Ok(Some(true));
                        } else if deadline.is_some() {
                            *ownership = Ownership::ProtectedAwaitable(
                                SharedAwaitable::with_owner(owner.clone()),
                            );
                            return Ok(None);
                        }
                        break;
                    }
                    Ownership::ProtectedAwaitable(shared_awaitable) => {
                        if shared_awaitable.cleanup_inactive_owners() {
                            if shared_awaitable.creation_instant == S::Instant::default() {
                                *ownership = Ownership::Locked(Owner::new(new_owner));
                            } else {
                                *ownership = Ownership::LockedAwaitable(
                                    ExclusiveAwaitable::with_instant_and_owner(
                                        shared_awaitable.creation_instant,
                                        Owner::new(new_owner),
                                    ),
                                );
                            }
                            return Ok(Some(true));
                        } else if let Some(exclusive_awaitable) =
                            shared_awaitable.try_promote_to_exclusive(new_owner)
                        {
                            *ownership = Ownership::LockedAwaitable(exclusive_awaitable);
                            return Ok(Some(true));
                        } else if deadline.is_some() {
                            return Ok(None);
                        }
                        break;
                    }
                },
                ObjectState::Created(instant) => {
                    *object_state = ObjectState::Owned(Ownership::LockedAwaitable(
                        ExclusiveAwaitable::with_instant_and_owner(*instant, Owner::new(new_owner)),
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
    ///
    /// Returns `Ok(None)` if the result will be out after waiting.
    fn try_delete(
        object_state: &mut ObjectState<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        loop {
            object_state.cleanup_state();
            match object_state {
                ObjectState::Owned(ownership) => match ownership {
                    Ownership::Created(_) | Ownership::Locked(_) | Ownership::Deleted(_) => {
                        return Self::try_delete_exclusively_owned(ownership, new_owner, deadline);
                    }
                    Ownership::CreatedAwaitable(_)
                    | Ownership::LockedAwaitable(_)
                    | Ownership::DeletedAwaitable(_) => {
                        let (result_available, result_or_wait) =
                            Self::try_delete_exclusive_awaitable(ownership, new_owner, deadline)?;
                        if result_available {
                            return Ok(Some(result_or_wait));
                        } else if result_or_wait {
                            return Ok(None);
                        }
                    }
                    Ownership::Protected(owner) => {
                        if let Relationship::Linearizable = owner.grant_write_access(new_owner) {
                            // It is possible to promote the access permission in this transaction.
                            let mut exclusive_awaitable =
                                ExclusiveAwaitable::with_owner(Owner::new(new_owner));
                            exclusive_awaitable
                                .set_prior_state(Ownership::Protected(owner.clone()));
                            *ownership = Ownership::DeletedAwaitable(exclusive_awaitable);
                            return Ok(Some(true));
                        } else if owner.is_terminated() {
                            *ownership = Ownership::Deleted(Owner::new(new_owner));
                            return Ok(Some(true));
                        } else if deadline.is_some() {
                            // Allocate a wait queue and retry.
                            *ownership = Ownership::ProtectedAwaitable(
                                SharedAwaitable::with_owner(owner.clone()),
                            );
                            return Ok(None);
                        }
                        break;
                    }
                    Ownership::ProtectedAwaitable(shared_awaitable) => {
                        if shared_awaitable.cleanup_inactive_owners() {
                            if shared_awaitable.creation_instant == S::Instant::default() {
                                *ownership = Ownership::Deleted(Owner::new(new_owner));
                            } else {
                                *ownership = Ownership::DeletedAwaitable(
                                    ExclusiveAwaitable::with_instant_and_owner(
                                        shared_awaitable.creation_instant,
                                        Owner::new(new_owner),
                                    ),
                                );
                            }
                            return Ok(Some(true));
                        }
                        if let Some(exclusive_awaitable) =
                            shared_awaitable.try_promote_to_exclusive(new_owner)
                        {
                            *ownership = Ownership::DeletedAwaitable(exclusive_awaitable);
                            return Ok(Some(true));
                        } else if deadline.is_some() {
                            return Ok(None);
                        }
                        break;
                    }
                },
                ObjectState::Created(instant) => {
                    *object_state = ObjectState::Owned(Ownership::DeletedAwaitable(
                        ExclusiveAwaitable::with_instant_and_owner(*instant, Owner::new(new_owner)),
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

    /// Takes shared ownership of the exclusively owned database object.
    ///
    /// Returns `Ok(None)` if the result will be out after waiting.
    fn try_share_exclusively_owned(
        ownership: &mut Ownership<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (owner, is_created, is_deleted) = match ownership {
            Ownership::Created(owner) => (owner, true, false),
            Ownership::Locked(owner) => (owner, false, false),
            Ownership::Deleted(owner) => (owner, false, true),
            _ => return Err(Error::WrongParameter),
        };

        // The state of the owner needs to be checked.
        match owner.grant_write_access(new_owner) {
            Relationship::Committed(commit_instant) => {
                if is_deleted {
                    return Err(Error::SerializationFailure);
                }
                *ownership = if is_created {
                    Ownership::ProtectedAwaitable(SharedAwaitable::with_instant_and_owner(
                        commit_instant,
                        Owner::new(new_owner),
                    ))
                } else {
                    Ownership::Protected(Owner::new(new_owner))
                };
                Ok(Some(true))
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                *ownership = Ownership::Protected(Owner::new(new_owner));
                Ok(Some(true))
            }
            Relationship::Linearizable => {
                // The transaction already owns the database object.
                Ok(Some(false))
            }
            Relationship::Concurrent => {
                // Competing with other operations in the same transaction is
                // considered to be a deadlock.
                Err(Error::Deadlock)
            }
            Relationship::Unknown => {
                if deadline.is_some() {
                    *ownership = Self::augment_wait_queue(is_created, is_deleted, owner.clone());
                    Ok(None)
                } else {
                    // No deadline is specified.
                    Err(Error::SerializationFailure)
                }
            }
        }
    }

    /// Takes shared ownership of the exclusively owned and `awaitable` database object.
    ///
    /// Returns `(true, result)` if the result is available, `(false, true)` if the result will be
    /// out after waiting, or `(false, false)` if the operation needs to be retried.
    fn try_share_exclusive_awaitable(
        ownership: &mut Ownership<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<(bool, bool), Error> {
        let (exclusive_awaitable, is_created, is_deleted) =
            Self::exclusive_awaitable_access_defails(ownership)?;

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
                    *ownership =
                        Ownership::ProtectedAwaitable(SharedAwaitable::with_instant_and_owner(
                            creation_instant,
                            Owner::new(new_owner),
                        ));
                    return Ok((true, true));
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                if exclusive_awaitable.prior_ownership.is_some() {
                    return Ok((false, false));
                }
                if exclusive_awaitable.wait_queue.is_empty() {
                    *ownership = Ownership::Protected(Owner::new(new_owner));
                    return Ok((true, true));
                }
            }
            Relationship::Linearizable => {
                // The transaction already has the exclusive ownership.
                return Ok((true, false));
            }
            Relationship::Concurrent => {
                // Competing with other operations in the same transaction is considered to be a
                // deadlock.
                return Err(Error::Deadlock);
            }
            Relationship::Unknown => (),
        };

        if deadline.is_some() {
            Ok((false, true))
        } else {
            // No deadline is specified.
            Err(Error::SerializationFailure)
        }
    }

    /// Takes exclusive ownership of the exclusively owned database object.
    ///
    /// Returns `Ok(None)` if the result will be out after waiting.
    fn try_lock_exclusively_owned(
        ownership: &mut Ownership<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (owner, is_created, is_deleted) = match ownership {
            Ownership::Created(owner) => (owner, true, false),
            Ownership::Locked(owner) => (owner, false, false),
            Ownership::Deleted(owner) => (owner, false, true),
            _ => return Err(Error::WrongParameter),
        };

        // The state of the owner needs to be checked.
        match owner.grant_write_access(new_owner) {
            Relationship::Committed(commit_instant) => {
                if is_deleted {
                    Err(Error::SerializationFailure)
                } else {
                    *ownership = if is_created {
                        Ownership::LockedAwaitable(ExclusiveAwaitable::with_instant_and_owner(
                            commit_instant,
                            Owner::new(new_owner),
                        ))
                    } else {
                        Ownership::Locked(Owner::new(new_owner))
                    };
                    Ok(Some(true))
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                *ownership = Ownership::Locked(Owner::new(new_owner));
                Ok(Some(true))
            }
            Relationship::Linearizable => {
                // Exclusive ownership is weaker than ownership for creation or deletion.
                Ok(Some(false))
            }
            Relationship::Concurrent => {
                // Competing with other operations in the same transaction is considered to be a
                // deadlock.
                Err(Error::Deadlock)
            }
            Relationship::Unknown => {
                if deadline.is_some() {
                    *ownership = Self::augment_wait_queue(is_created, is_deleted, owner.clone());
                    Ok(None)
                } else {
                    // No deadline is specified.
                    Err(Error::SerializationFailure)
                }
            }
        }
    }

    /// Takes or exclusive ownership of the exclusively owned `awaitable` database object.
    ///
    /// Returns `(true, result)` if the result is available, `(false, true)` if the result will be
    /// out after waiting, or `(false, false)` if the operation needs to be retried.
    fn try_lock_exclusive_awaitable(
        ownership: &mut Ownership<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<(bool, bool), Error> {
        let (exclusive_awaitable, is_created, is_deleted) =
            Self::exclusive_awaitable_access_defails(ownership)?;

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
                    *ownership =
                        Ownership::LockedAwaitable(ExclusiveAwaitable::with_instant_and_owner(
                            creation_instant,
                            Owner::new(new_owner),
                        ));
                    return Ok((true, true));
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                if exclusive_awaitable.prior_ownership.is_some() {
                    return Ok((false, false));
                }
                if exclusive_awaitable.wait_queue.is_empty() {
                    *ownership = Ownership::Locked(Owner::new(new_owner));
                    return Ok((true, true));
                }
            }
            Relationship::Linearizable => {
                // Exclusive ownership is weaker than ownership for creation or deletion.
                return Ok((true, false));
            }
            Relationship::Concurrent => {
                // Competing with other operations in the same transaction is considered to be a
                // deadlock.
                return Err(Error::Deadlock);
            }
            Relationship::Unknown => (),
        };

        if deadline.is_some() {
            Ok((false, true))
        } else {
            // No deadline is specified.
            Err(Error::SerializationFailure)
        }
    }

    /// Takes exclusive ownership of the exclusively owned database object to delete it.
    ///
    /// Returns `Ok(None)` if the result will be out after waiting.
    fn try_delete_exclusively_owned(
        ownership: &mut Ownership<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<Option<bool>, Error> {
        let (owner, is_created, is_deleted) = match ownership {
            Ownership::Created(owner) => (owner, true, false),
            Ownership::Locked(owner) => (owner, false, false),
            Ownership::Deleted(owner) => (owner, false, true),
            _ => return Err(Error::WrongParameter),
        };

        // The state of the owner needs to be checked.
        match owner.grant_write_access(new_owner) {
            Relationship::Committed(commit_instant) => {
                if is_deleted {
                    Err(Error::SerializationFailure)
                } else {
                    *ownership = if is_created {
                        Ownership::DeletedAwaitable(ExclusiveAwaitable::with_instant_and_owner(
                            commit_instant,
                            Owner::new(new_owner),
                        ))
                    } else {
                        Ownership::Deleted(Owner::new(new_owner))
                    };
                    Ok(Some(true))
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                *ownership = Ownership::Deleted(Owner::new(new_owner));
                Ok(Some(true))
            }
            Relationship::Linearizable => {
                if is_deleted {
                    // The transaction owns the database object to delete it.
                    return Ok(Some(false));
                }
                // The database object is locked by the transaction, therefore the journal is
                // eligible to promote the access mode.
                let mut exclusive_awaitable = ExclusiveAwaitable::with_owner(Owner::new(new_owner));
                if is_created {
                    exclusive_awaitable.set_prior_state(Ownership::Created(owner.clone()));
                } else {
                    exclusive_awaitable.set_prior_state(Ownership::Locked(owner.clone()));
                }
                *ownership = Ownership::DeletedAwaitable(exclusive_awaitable);
                Ok(Some(true))
            }
            Relationship::Concurrent => {
                // Competing with other operations in the same transaction is considered to be a
                // deadlock.
                Err(Error::Deadlock)
            }
            Relationship::Unknown => {
                if deadline.is_some() {
                    *ownership = Self::augment_wait_queue(is_created, is_deleted, owner.clone());
                    Ok(None)
                } else {
                    // No deadline is specified.
                    Err(Error::SerializationFailure)
                }
            }
        }
    }

    /// Takes exclusive ownership of the exclusively owned `awaitable` database object to delete it.
    ///
    /// Returns `(true, result)` if the result is available, `(false, true)` if the result will be
    /// out after waiting, or `(false, false)` if the operation needs to be retried.
    fn try_delete_exclusive_awaitable(
        ownership: &mut Ownership<S>,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
        deadline: Option<Instant>,
    ) -> Result<(bool, bool), Error> {
        let (exclusive_awaitable, is_created, is_deleted) =
            Self::exclusive_awaitable_access_defails(ownership)?;

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
                    *ownership =
                        Ownership::DeletedAwaitable(ExclusiveAwaitable::with_instant_and_owner(
                            creation_instant,
                            Owner::new(new_owner),
                        ));
                    return Ok((true, true));
                }
            }
            Relationship::RolledBack => {
                // The owner was rolled back.
                if exclusive_awaitable.prior_ownership.is_some() {
                    return Ok((false, false));
                }
                if exclusive_awaitable.wait_queue.is_empty() {
                    *ownership = Ownership::Deleted(Owner::new(new_owner));
                    return Ok((true, true));
                }
            }
            Relationship::Linearizable => {
                if is_deleted {
                    // The transaction owns the database object to delete it.
                    return Ok((true, false));
                } else if exclusive_awaitable.wait_queue.is_empty() {
                    // The database object is locked by the transaction and the wait queue is
                    // empty, therefore the journal is eligible to promote the access mode.
                    let mut new_exclusive_awaitable = ExclusiveAwaitable::with_instant_and_owner(
                        exclusive_awaitable.creation_instant,
                        Owner::new(new_owner),
                    );
                    let old_exclusive_awaitable =
                        ExclusiveAwaitable::take_other(exclusive_awaitable);
                    if is_created {
                        new_exclusive_awaitable
                            .set_prior_state(Ownership::CreatedAwaitable(old_exclusive_awaitable));
                    } else {
                        new_exclusive_awaitable
                            .set_prior_state(Ownership::LockedAwaitable(old_exclusive_awaitable));
                    }
                    *ownership = Ownership::DeletedAwaitable(new_exclusive_awaitable);
                    return Ok((true, true));
                }
            }
            Relationship::Concurrent => {
                // Competing with other operations in the same transaction is considered to be a
                // deadlock.
                return Err(Error::Deadlock);
            }
            Relationship::Unknown => (),
        };

        if deadline.is_some() {
            Ok((false, true))
        } else {
            // No deadline is specified.
            Err(Error::SerializationFailure)
        }
    }

    /// Extracts exclusive access details from the [`Ownership`].
    ///
    /// Returns an [`Error`] if the supplied [`Ownership`] is wrong.
    fn exclusive_awaitable_access_defails(
        ownership: &mut Ownership<S>,
    ) -> Result<(&mut ExclusiveAwaitable<S>, bool, bool), Error> {
        let (exclusive_awaitable, is_created, is_deleted) = match ownership {
            Ownership::CreatedAwaitable(exclusive_awaitable) => (exclusive_awaitable, true, false),
            Ownership::LockedAwaitable(exclusive_awaitable) => (exclusive_awaitable, false, false),
            Ownership::DeletedAwaitable(exclusive_awaitable) => (exclusive_awaitable, false, true),
            _ => return Err(Error::WrongParameter),
        };
        Ok((exclusive_awaitable, is_created, is_deleted))
    }

    /// Augments [`WaitQueue`]
    fn augment_wait_queue(is_created: bool, is_deleted: bool, owner: Owner<S>) -> Ownership<S> {
        if is_created {
            Ownership::CreatedAwaitable(ExclusiveAwaitable::with_owner(owner))
        } else if is_deleted {
            Ownership::DeletedAwaitable(ExclusiveAwaitable::with_owner(owner))
        } else {
            Ownership::LockedAwaitable(ExclusiveAwaitable::with_owner(owner))
        }
    }
}

impl<S: Sequencer> Owner<S> {
    /// Creates a new [`Owner`].
    fn new(anchor: &ebr::Arc<JournalAnchor<S>>) -> Owner<S> {
        Owner {
            anchor: anchor.clone(),
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

impl<S: Sequencer> ObjectState<S> {
    /// Cleans up the [`ObjectState`].
    fn cleanup_state(&mut self) {
        if let ObjectState::Owned(ownership) = self {
            // Try to revoke previously promoted access privileges if the owner was rolled back.
            while let Ownership::LockedAwaitable(exclusive_awaitable)
            | Ownership::DeletedAwaitable(exclusive_awaitable) = ownership
            {
                // Check if the owner was rolled back.
                if exclusive_awaitable.owner.is_rolled_back() {
                    if let Some(prior_state) = exclusive_awaitable.prior_ownership.take() {
                        let wait_queue = take(&mut exclusive_awaitable.wait_queue);
                        *ownership = *prior_state;
                        if !wait_queue.is_empty() {
                            match ownership {
                                Ownership::Locked(owner) => {
                                    *ownership = Ownership::LockedAwaitable(
                                        ExclusiveAwaitable::with_owner_and_wait_queue(
                                            owner.clone(),
                                            wait_queue,
                                        ),
                                    );
                                    continue;
                                }
                                Ownership::LockedAwaitable(exclusive_awaitable) => {
                                    exclusive_awaitable.wait_queue = wait_queue;
                                    continue;
                                }
                                Ownership::Protected(owner) => {
                                    *ownership = Ownership::ProtectedAwaitable(
                                        SharedAwaitable::with_owner_and_wait_queue(
                                            owner.clone(),
                                            wait_queue,
                                        ),
                                    );
                                    continue;
                                }
                                Ownership::ProtectedAwaitable(shared_awaitable) => {
                                    shared_awaitable.wait_queue = wait_queue;
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
                break;
            }

            // Try to convert `Locked` into `Created` or `Deleted` if the owner was committed.
            //
            // TODO: return `false` if the whole entry can be removed.
            match ownership {
                Ownership::Created(owner) => {
                    if let Some(commit_instant) = owner.eot_instant() {
                        if commit_instant != S::Instant::default() {
                            *self = ObjectState::Created(commit_instant);
                        }
                    }
                }
                Ownership::CreatedAwaitable(exclusive_awaitable) => {
                    if exclusive_awaitable.is_empty() {
                        if let Some(commit_instant) = exclusive_awaitable.owner.eot_instant() {
                            if commit_instant == S::Instant::default() {
                                *self = ObjectState::Owned(Ownership::Created(
                                    exclusive_awaitable.owner.clone(),
                                ));
                            } else {
                                *self = ObjectState::Created(commit_instant);
                            }
                        }
                    }
                }
                Ownership::Protected(_)
                | Ownership::ProtectedAwaitable(_)
                | Ownership::Locked(_) => (),
                Ownership::LockedAwaitable(exclusive_awaitable) => {
                    if exclusive_awaitable.is_empty() && exclusive_awaitable.owner.is_terminated() {
                        if exclusive_awaitable.creation_instant == S::Instant::default() {
                            *self = ObjectState::Owned(Ownership::Locked(
                                exclusive_awaitable.owner.clone(),
                            ));
                        } else {
                            *self = ObjectState::Created(exclusive_awaitable.creation_instant);
                        }
                    }
                }
                Ownership::Deleted(owner) => {
                    if let Some(commit_instant) = owner.eot_instant() {
                        if commit_instant != S::Instant::default() {
                            *self = ObjectState::Deleted(commit_instant);
                        }
                    }
                }
                Ownership::DeletedAwaitable(exclusive_awaitable) => {
                    if exclusive_awaitable.is_empty() {
                        if let Some(commit_instant) = exclusive_awaitable.owner.eot_instant() {
                            if commit_instant == S::Instant::default() {
                                *self = ObjectState::Owned(Ownership::Deleted(
                                    exclusive_awaitable.owner.clone(),
                                ));
                            } else {
                                *self = ObjectState::Deleted(commit_instant);
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<S: Sequencer> Clone for Request<S> {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            Self::Create(instant, owner, result_placeholder) => {
                Self::Create(*instant, owner.clone(), result_placeholder.clone())
            }
            Self::Protect(instant, owner, result_placeholder) => {
                Self::Protect(*instant, owner.clone(), result_placeholder.clone())
            }
            Self::Lock(instant, owner, result_placeholder) => {
                Self::Lock(*instant, owner.clone(), result_placeholder.clone())
            }
            Self::Delete(instant, owner, result_placeholder) => {
                Self::Delete(*instant, owner.clone(), result_placeholder.clone())
            }
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

    /// Checks if the specified owner is linearizable with all the owners in the owner set.
    fn try_promote_to_exclusive(
        &mut self,
        new_owner: &ebr::Arc<JournalAnchor<S>>,
    ) -> Option<Box<ExclusiveAwaitable<S>>> {
        if self.owner_set.iter().any(|o| {
            // Delete the entry if the owner definitely does not hold the shared ownership.
            !matches!(
                o.grant_write_access(new_owner),
                Relationship::<S>::Linearizable
            )
        }) || self.owner_set.is_empty()
        {
            // One of the owners is not compatible.
            return None;
        }

        // If the requester is the only shared owner of the database object, it
        // is eligible to promote it to `ExclusiveAwaitable`
        let wait_queue = take(&mut self.wait_queue);
        let mut exclusive_awaitable =
            ExclusiveAwaitable::with_owner_and_wait_queue(Owner::new(new_owner), wait_queue);
        exclusive_awaitable.set_prior_state(Ownership::ProtectedAwaitable(Box::new(self.clone())));
        Some(exclusive_awaitable)
    }

    /// Cleans up committed and rolled back owners.
    ///
    /// Returns `true` if the [`SharedAwaitable`] got totally empty.
    fn cleanup_inactive_owners(&mut self) -> bool {
        self.owner_set.retain(|o| {
            // Delete the entry if the owner definitely does not hold the shared ownership.
            !o.is_terminated()
        });
        self.owner_set.is_empty() && self.wait_queue.is_empty()
    }

    /// Pushes a request into the wait queue.
    fn push_request(&mut self, request: Request<S>) {
        self.wait_queue.push_back(request);
        self.owner_set.iter().for_each(|o| {
            o.set_wake_up_others();
        });
    }
}

impl<S: Sequencer> Clone for SharedAwaitable<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            creation_instant: self.creation_instant,
            owner_set: self.owner_set.clone(),
            wait_queue: self.wait_queue.clone(),
        }
    }
}

impl<S: Sequencer> ExclusiveAwaitable<S> {
    /// Creates a new [`ExclusiveAwaitable`] from another instance of it.
    fn take_other(other: &mut ExclusiveAwaitable<S>) -> Box<ExclusiveAwaitable<S>> {
        Box::new(ExclusiveAwaitable {
            creation_instant: other.creation_instant,
            owner: other.owner.clone(),
            prior_ownership: other.prior_ownership.take(),
            wait_queue: take(&mut other.wait_queue),
        })
    }

    /// Creates a new [`ExclusiveAwaitable`] with a single owner inserted.
    fn with_owner(owner: Owner<S>) -> Box<ExclusiveAwaitable<S>> {
        Box::new(ExclusiveAwaitable {
            creation_instant: S::Instant::default(),
            owner,
            prior_ownership: None,
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
            prior_ownership: None,
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
            prior_ownership: None,
            wait_queue: WaitQueue::default(),
        })
    }

    /// Returns `true` if the [`ExclusiveAwaitable`] is empty.
    fn is_empty(&self) -> bool {
        self.prior_ownership.is_none() && self.wait_queue.is_empty()
    }

    /// Sets the old access data when promoting the access mode.
    fn set_prior_state(&mut self, old_access_data: Ownership<S>) {
        debug_assert!(self.prior_ownership.is_none());
        self.prior_ownership.replace(Box::new(old_access_data));
    }

    /// Pushes a request into the wait queue.
    fn push_request(&mut self, request: Request<S>) {
        self.wait_queue.push_back(request);
        self.owner.set_wake_up_others();
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

impl<S: Sequencer> Clone for WaitQueue<S> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
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
    #[inline]
    fn drop(&mut self) {
        self.0.drain(..).for_each(|r| {
            // The wait queue is being dropped due to memory allocation failure.

            let result_placeholder = match &r {
                Request::Create(_, _, result_placeholder)
                | Request::Protect(_, _, result_placeholder)
                | Request::Lock(_, _, result_placeholder)
                | Request::Delete(_, _, result_placeholder) => result_placeholder,
            };
            if let Some(mut result_waker) = result_placeholder.lock_sync() {
                if result_waker.0.is_none() {
                    result_waker.0.replace(Err(Error::OutOfMemory));
                }
                if let Some(waker) = result_waker.1.take() {
                    waker.wake();
                }
            }
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{AtomicCounter, Database};
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Barrier;

    static_assertions::assert_eq_size!(ObjectState<AtomicCounter>, [u8; 16]);

    const TIMEOUT_UNEXPECTED: Duration = Duration::from_secs(60);
    const TIMEOUT_EXPECTED: Duration = Duration::from_millis(1);

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
        deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        match access_action {
            AccessAction::Create => access_controller.create(&0, journal, deadline).await,
            AccessAction::Share => access_controller.share(&0, journal, deadline).await,
            AccessAction::Lock => access_controller.lock(&0, journal, deadline).await,
            AccessAction::Delete => access_controller.delete(&0, journal, deadline).await,
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
                take_access_action(access_action, access_controller, &mut journal, None).await,
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
    async fn access_promote_rewind() {
        for num_shared_locks in 0..4 {
            for promotion_action in [AccessAction::Lock, AccessAction::Delete] {
                let database = Database::default();
                let access_controller = database.access_controller();
                let mut transaction = database.transaction();
                let mut journal = transaction.journal();
                for i in 0..num_shared_locks {
                    let mut journal = transaction.journal();
                    assert_eq!(
                        take_access_action(
                            AccessAction::Share,
                            access_controller,
                            &mut journal,
                            None
                        )
                        .await,
                        Ok(i == 0)
                    );
                    assert_eq!(journal.submit(), i + 1);
                }
                if num_shared_locks != 0 {
                    assert_eq!(
                        take_access_action(promotion_action, access_controller, &mut journal, None)
                            .await,
                        Err(Error::SerializationFailure),
                    );
                }
                drop(journal);
                for _ in 0..2 {
                    let mut journal = transaction.journal();
                    assert_eq!(
                        take_access_action(promotion_action, access_controller, &mut journal, None)
                            .await,
                        Ok(true)
                    );
                    assert_eq!(journal.submit(), num_shared_locks + 1);
                    let mut journal = transaction.journal();
                    assert_eq!(
                        take_access_action(promotion_action, access_controller, &mut journal, None)
                            .await,
                        Ok(false)
                    );
                    drop(journal);
                    if promotion_action == AccessAction::Lock {
                        for _ in 0..2 {
                            let mut journal = transaction.journal();
                            assert_eq!(
                                take_access_action(
                                    AccessAction::Delete,
                                    access_controller,
                                    &mut journal,
                                    None
                                )
                                .await,
                                Ok(true)
                            );
                        }
                        let mut journal = transaction.journal();
                        assert_eq!(
                            take_access_action(
                                AccessAction::Share,
                                access_controller,
                                &mut journal,
                                None
                            )
                            .await,
                            Ok(false)
                        );
                    }
                    assert_eq!(transaction.rewind(num_shared_locks), Ok(num_shared_locks));
                }
            }
        }
    }

    #[tokio::test]
    async fn access_promote_wait() {
        for num_shared_locks in 1..3 {
            for block_action in [AccessAction::Lock, AccessAction::Delete] {
                for promotion_action in [AccessAction::Lock, AccessAction::Delete] {
                    let database = Database::default();
                    let access_controller = database.access_controller();
                    let transaction = database.transaction();
                    for i in 0..num_shared_locks {
                        let mut journal = transaction.journal();
                        assert_eq!(
                            take_access_action(
                                AccessAction::Share,
                                access_controller,
                                &mut journal,
                                None
                            )
                            .await,
                            Ok(i == 0)
                        );
                        assert_eq!(journal.submit(), i + 1);
                    }
                    let blocker_transaction = database.transaction();
                    let mut blocker_journal = blocker_transaction.journal();
                    let mut journal = transaction.journal();
                    let (blocker_result, result) = futures::join!(
                        take_access_action(
                            block_action,
                            access_controller,
                            &mut blocker_journal,
                            Some(Instant::now() + TIMEOUT_EXPECTED)
                        ),
                        take_access_action(
                            promotion_action,
                            access_controller,
                            &mut journal,
                            Some(Instant::now() + TIMEOUT_UNEXPECTED)
                        )
                    );
                    assert_eq!(result, Ok(true));
                    assert_eq!(blocker_result, Err(Error::Timeout));
                }
            }
        }
    }

    #[tokio::test]
    async fn access_promote_commit() {
        for num_shared_locks in 1..3 {
            for waiting_action in [AccessAction::Lock, AccessAction::Delete] {
                for promotion_action in [AccessAction::Lock, AccessAction::Delete] {
                    let database = Database::default();
                    let access_controller = database.access_controller();
                    let transaction = database.transaction();
                    for i in 0..num_shared_locks {
                        let mut journal = transaction.journal();
                        assert_eq!(
                            take_access_action(
                                AccessAction::Share,
                                access_controller,
                                &mut journal,
                                None
                            )
                            .await,
                            Ok(i == 0)
                        );
                        assert_eq!(journal.submit(), i + 1);
                    }
                    let waiting_transaction = database.transaction();
                    let mut waiting_journal = waiting_transaction.journal();
                    let (waiting_result, _) = futures::join!(
                        take_access_action(
                            waiting_action,
                            access_controller,
                            &mut waiting_journal,
                            Some(Instant::now() + TIMEOUT_UNEXPECTED)
                        ),
                        async {
                            let mut journal = transaction.journal();
                            assert_eq!(
                                take_access_action(
                                    promotion_action,
                                    access_controller,
                                    &mut journal,
                                    Some(Instant::now() + TIMEOUT_UNEXPECTED),
                                )
                                .await,
                                Ok(true)
                            );
                            assert_eq!(journal.submit(), num_shared_locks + 1);
                            assert!(transaction.commit().await.is_ok());
                        }
                    );
                    if promotion_action == AccessAction::Delete {
                        assert_eq!(waiting_result, Err(Error::SerializationFailure));
                    } else {
                        assert_eq!(waiting_result, Ok(true));
                    }
                }
            }
        }
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
                            take_access_action(
                                access_action,
                                access_controller,
                                &mut journal,
                                None
                            )
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
                                transaction.rollback();
                            }
                        };
                        let post_action_runner = async {
                            let transaction = database.transaction();
                            let mut journal = transaction.journal();
                            take_access_action(
                                post_access_action,
                                access_controller,
                                &mut journal,
                                Some(Instant::now() + TIMEOUT_UNEXPECTED),
                            )
                            .await
                        };

                        let result = if serial_execution {
                            transaction_action_runner.await;
                            post_action_runner.await
                        } else {
                            futures::join!(transaction_action_runner, post_action_runner).1
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

    #[tokio::test]
    async fn action_wait() {
        for pre_access_action in [
            AccessAction::Create,
            AccessAction::Share,
            AccessAction::Lock,
            AccessAction::Delete,
        ] {
            for access_action in [
                AccessAction::Create,
                AccessAction::Share,
                AccessAction::Lock,
                AccessAction::Delete,
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
                        take_access_action(
                            pre_access_action,
                            access_controller,
                            &mut journal,
                            None
                        )
                        .await,
                        Ok(true)
                    );
                    assert_eq!(journal.submit(), 1);
                    let prepared = transaction.prepare().await.unwrap();
                    let action_runner = async {
                        let transaction_outer = database.transaction();
                        let mut journal_outer = transaction_outer.journal();
                        take_access_action(
                            access_action,
                            access_controller,
                            &mut journal_outer,
                            Some(Instant::now() + TIMEOUT_UNEXPECTED),
                        )
                        .await
                    };
                    let post_action_runner = async {
                        let transaction_inner = database.transaction();
                        let mut journal_inner = transaction_inner.journal();
                        take_access_action(
                            post_access_action,
                            access_controller,
                            &mut journal_inner,
                            Some(Instant::now() + TIMEOUT_UNEXPECTED),
                        )
                        .await
                    };
                    let (_, result, result_post) = futures::join!(
                        async { assert!(prepared.await.is_ok()) },
                        action_runner,
                        post_action_runner
                    );
                    match (pre_access_action, access_action, post_access_action) {
                        (_, AccessAction::Create, _) => {
                            assert_eq!(result, Err(Error::SerializationFailure));
                        }
                        (_, _, AccessAction::Create) => {
                            assert_eq!(result_post, Err(Error::SerializationFailure));
                        }
                        (AccessAction::Create | AccessAction::Share | AccessAction::Lock, _, _) => {
                            assert_eq!(result, Ok(true));
                            assert_eq!(result_post, Ok(true));
                        }
                        (AccessAction::Delete, _, _) => {
                            assert_eq!(result, Err(Error::SerializationFailure));
                            assert_eq!(result_post, Err(Error::SerializationFailure));
                        }
                    };
                }
            }
        }
    }

    #[tokio::test]
    async fn action_timeout() {
        for commit in [false, true] {
            for access_action in [
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
                    take_access_action(access_action, access_controller, &mut journal, None).await,
                    Ok(true)
                );
                assert_eq!(journal.submit(), 1);
                let prepared = transaction.prepare().await.unwrap();
                let snapshot = database.snapshot();
                assert_eq!(
                    access_controller
                        .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_EXPECTED),)
                        .await,
                    Ok(access_action != AccessAction::Create),
                );
                for post_access_action in [
                    AccessAction::Create,
                    AccessAction::Share,
                    AccessAction::Lock,
                    AccessAction::Delete,
                ] {
                    let transaction = database.transaction();
                    let mut journal = transaction.journal();
                    let result = take_access_action(
                        post_access_action,
                        access_controller,
                        &mut journal,
                        Some(Instant::now() + TIMEOUT_EXPECTED),
                    )
                    .await;
                    if access_action == AccessAction::Share
                        && post_access_action == AccessAction::Share
                    {
                        assert_eq!(result, Ok(true));
                    } else if access_action != AccessAction::Create
                        && post_access_action == AccessAction::Create
                    {
                        assert_eq!(
                            result,
                            Err(Error::SerializationFailure),
                            "{access_action:?} {post_access_action:?}"
                        );
                    } else {
                        assert_eq!(
                            result,
                            Err(Error::Timeout),
                            "{access_action:?} {post_access_action:?}"
                        );
                    }
                    drop(journal);
                    assert!(transaction.commit().await.is_ok());
                }
                let snapshot = database.snapshot();
                let result = access_controller
                    .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_EXPECTED))
                    .await;
                if access_action == AccessAction::Create || access_action == AccessAction::Delete {
                    assert_eq!(result, Err(Error::Timeout), "{access_action:?}");
                } else {
                    assert_eq!(result, Ok(true), "{access_action:?}");
                }

                if commit {
                    assert!(prepared.await.is_ok());
                    let snapshot = database.snapshot();
                    let result = access_controller
                        .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_EXPECTED))
                        .await;
                    assert_eq!(
                        result,
                        Ok(access_action != AccessAction::Delete),
                        "{access_action:?}"
                    );
                } else {
                    drop(prepared);
                    let snapshot = database.snapshot();
                    let result = access_controller
                        .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_EXPECTED))
                        .await;
                    assert_eq!(
                        result,
                        Ok(access_action != AccessAction::Create),
                        "{access_action:?}"
                    );
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn parallel_mutex() {
        let num_tasks = 16;
        let num_operations = 256;
        let barrier = Arc::new(Barrier::new(num_tasks));
        let database = Arc::new(Database::default());
        let data = Arc::new(AtomicUsize::default());
        let mut task_handles = Vec::with_capacity(num_tasks);
        for _ in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let database_clone = database.clone();
            let data_clone = data.clone();
            task_handles.push(tokio::spawn(async move {
                barrier_clone.wait().await;
                for i in 0..num_operations {
                    let transaction = database_clone.transaction();
                    let mut journal = transaction.journal();
                    assert_eq!(
                        database_clone
                            .access_controller()
                            .lock(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED))
                            .await,
                        Ok(true)
                    );
                    assert_eq!(journal.submit(), 1);
                    let data_current = data_clone.load(Relaxed);
                    data_clone.store(data_current + 1, Relaxed);
                    if i % 2 == 0 {
                        assert!(transaction.commit().await.is_ok());
                    } else {
                        transaction.rollback();
                    }

                    let transaction = database_clone.transaction();
                    let mut journal = transaction.journal();
                    assert_eq!(
                        database_clone
                            .access_controller()
                            .share(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED))
                            .await,
                        Ok(true)
                    );
                    assert_eq!(journal.submit(), 1);
                    assert!(data_clone.load(Relaxed) >= data_current);
                    if i % 2 == 1 {
                        assert!(transaction.commit().await.is_ok());
                    } else {
                        transaction.rollback();
                    }
                }
            }));
        }
        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }
        assert_eq!(data.load(Relaxed), num_operations * num_tasks);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn parallel_create_delete() {
        let num_tasks = 16;
        let barrier = Arc::new(Barrier::new(num_tasks));
        let database = Arc::new(Database::default());
        let data = Arc::new(AtomicUsize::new(usize::MAX));
        let mut task_handles = Vec::with_capacity(num_tasks);
        for _ in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let database_clone = database.clone();
            let data_clone = data.clone();
            task_handles.push(tokio::spawn(async move {
                barrier_clone.wait().await;
                let transaction = database_clone.transaction();
                let mut journal = transaction.journal();
                match database_clone
                    .access_controller()
                    .create(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED))
                    .await
                {
                    Ok(result) => {
                        assert!(result);
                        assert_eq!(data_clone.load(Relaxed), usize::MAX);
                        data_clone.store(0, Relaxed);
                    }
                    Err(error) => {
                        assert_eq!(error, Error::SerializationFailure);
                    }
                }
                assert_eq!(journal.submit(), 1);
                assert!(transaction.commit().await.is_ok());

                assert_ne!(data_clone.load(Relaxed), usize::MAX);

                let transaction = database_clone.transaction();
                let mut journal = transaction.journal();
                assert_eq!(
                    database_clone
                        .access_controller()
                        .lock(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED))
                        .await,
                    Ok(true)
                );
                assert_eq!(journal.submit(), 1);

                let current = data_clone.load(Relaxed);
                data_clone.store(current + 1, Relaxed);
                assert!(transaction.commit().await.is_ok());

                let transaction = database_clone.transaction();
                let mut journal = transaction.journal();
                match database_clone
                    .access_controller()
                    .delete(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED))
                    .await
                {
                    Ok(result) => {
                        assert!(result);
                        assert_eq!(journal.submit(), 1);
                        let current = data_clone.load(Relaxed);
                        if current == num_tasks {
                            data_clone.store(num_tasks * 2, Relaxed);
                            assert!(transaction.commit().await.is_ok());
                        } else {
                            transaction.rollback();
                        }
                    }
                    Err(error) => {
                        assert_eq!(error, Error::SerializationFailure);
                        assert_eq!(journal.submit(), 1);
                        transaction.rollback();
                    }
                }
            }));
        }
        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }
        assert_eq!(data.load(Relaxed), num_tasks * 2);
    }
}
