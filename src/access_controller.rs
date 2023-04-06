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

    /// Promoted to `marked` from `shared`.
    #[allow(dead_code)]
    SharedToMarked(Owner<S>),

    /// Promoted to `marked` from `exclusive`.
    #[allow(dead_code)]
    ExclusiveToMarked(Owner<S>),
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
    Created(S::Instant),

    /// The database object was deleted at the instant.
    #[allow(dead_code)]
    Deleted(S::Instant),
}

/// Locking modes.
#[derive(Debug)]
pub(super) enum LockMode<S: Sequencer> {
    /// The database object is prepared to be created.
    Reserved(Owner<S>),

    /// The database object is prepared to be created, but there are waiting transactions.
    ReservedAwaitable(Box<ExclusiveAwaitable<S>>),

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
    Marked(Owner<S>),

    /// The database object which may not be visible to some readers is being deleted by the
    /// transaction.
    MarkedAwaitable(Box<ExclusiveAwaitable<S>>),
}

/// Types of access requests.
#[derive(Debug)]
pub(super) enum Request<S: Sequencer> {
    /// A request to reserve a database object.
    Reserve(Owner<S>),

    /// A request to acquire a shared lock on the database object.
    Share(Owner<S>),

    /// A request to acquire the exclusive lock on the database object.
    #[allow(dead_code)]
    Own(Owner<S>),

    /// A request to acquire the exclusive lock on the database object for deletion.
    #[allow(dead_code)]
    Marked(Owner<S>),
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
    ///     assert!(access_controller.reserve(&O(1), &mut journal, None).await.is_ok());
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
                    ObjectState::Locked(locked) => {
                        match locked {
                            LockMode::Reserved(owner) => {
                                // The database object is being created.
                                owner.grant_read_access(snapshot, deadline)
                            }
                            LockMode::ReservedAwaitable(exclusive_awaitable) => {
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
                            LockMode::Marked(owner) => {
                                // The database object is being deleted.
                                //
                                // The result should be negated since seeing the deletion means not
                                // seeing the database object.
                                owner.grant_read_access(snapshot, deadline).map(|r| !r)
                            }
                            LockMode::MarkedAwaitable(exclusive_awaitable) => {
                                if *snapshot >= exclusive_awaitable.creation_instant {
                                    // The database object is being deleted.
                                    exclusive_awaitable
                                        .owner
                                        .anchor
                                        .grant_read_access(snapshot, deadline)
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

    /// Reserves access control data for a database object to create it.
    ///
    /// Transactions can compete for the same database object, e.g., when the new database object
    /// becomes reachable via an index before being visible to database readers, however at most
    /// one of competing transactions can reserve the database object and finally be committed.
    ///
    /// [`AccessController`] does not prohibit a situation where an already created database object
    /// becomes globally visible, e.g., the object was created and the access data has been
    /// removed, and a new transaction tries to reserve the same database object; this usually
    /// causes a correctness issue where a database reader cannot have a consistent view of the
    /// database. Therefore, a transaction must have checked whether the database object is
    /// visible to database readers or not beforehand.
    ///
    /// It returns `true` if the database object was newly reserved in this transaction.
    ///
    /// # Errors
    ///
    /// An [`Error`] is returned if memory allocation failed, the database object was already
    /// created, or another transaction did not complete creating the database object until the
    /// specified deadline was reached.
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
    ///     assert!(access_controller.reserve(&O(1), &mut journal, None).await.is_ok());
    /// };
    /// ```
    #[inline]
    pub async fn reserve<O: ToObjectID, P: PersistenceLayer<S>>(
        &self,
        object: &O,
        journal: &mut Journal<'_, '_, S, P>,
        deadline: Option<Instant>,
    ) -> Result<bool, Error> {
        let mut entry = match self.table.entry_async(object.to_object_id()).await {
            MapEntry::Occupied(entry) => entry,
            MapEntry::Vacant(entry) => {
                entry.insert_entry(ObjectState::Locked(LockMode::Reserved(Owner::from(
                    journal,
                ))));
                return Ok(true);
            }
        };
        while let ObjectState::Locked(locked) = entry.get_mut() {
            match locked {
                LockMode::Reserved(owner) => {
                    // The state of the owner needs to be checked.
                    match owner.grant_write_access(journal.anchor()) {
                        Relationship::Committed(_) => {
                            // The transaction was committed, meaning that the database object has
                            // been successfully created.
                            return Err(Error::SerializationFailure);
                        }
                        Relationship::RolledBack => {
                            // The transaction or the owner journal was rolled back.
                            *entry.get_mut() =
                                ObjectState::Locked(LockMode::Reserved(Owner::from(journal)));
                            return Ok(true);
                        }
                        Relationship::Linearizable => {
                            // Already reserved in a previously submitted journal in the same
                            // transaction.
                            return Ok(false);
                        }
                        Relationship::Concurrent => {
                            // TODO: intra-transaction deadlock - need to check this.
                            return Err(Error::Deadlock);
                        }
                        Relationship::Unknown => {
                            if deadline.is_some() {
                                // Prepare for awaiting access to the database object.
                                *locked = LockMode::ReservedAwaitable(
                                    ExclusiveAwaitable::with_instant_and_owner(
                                        S::Instant::default(),
                                        owner.clone(),
                                    ),
                                );
                            } else {
                                // No deadline is specified.
                                break;
                            }
                        }
                    }
                }
                LockMode::ReservedAwaitable(exclusive_awaitable) => {
                    // TODO: intra-transaction ownership transfer.
                    if let Some(deadline) = deadline {
                        // If the owner gets rolled back before the deadline, this transaction may
                        // get access to the database object.
                        let message_sender = journal.message_sender();
                        let owner = Owner::from(journal);
                        let request = Request::Reserve(owner.clone());
                        exclusive_awaitable.push_request(request);
                        return AwaitResponse::new(owner, entry, message_sender, deadline).await;
                    }
                    // No deadline is specified.
                    break;
                }
                _ => break,
            }
        }

        // The database object has been created, deleted, or invisible.
        Err(Error::SerializationFailure)
    }

    /// Acquires a shared lock on the database object.
    ///
    /// Returns `true` if the lock is newly acquired in the transaction.
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
    ///     assert!(access_controller.reserve(&O(1), &mut journal, None).await.is_ok());
    ///     journal.submit();
    ///     assert!(transaction.commit().await.is_ok());
    ///
    ///     let transaction = database.transaction();
    ///     let mut journal = transaction.journal();
    ///     assert_eq!(access_controller.share(&O(1), &mut journal, None).await, Ok(true));
    /// };
    /// ```
    #[allow(clippy::too_many_lines)]
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

        loop {
            match entry.get_mut() {
                ObjectState::Locked(locked) => {
                    match locked {
                        LockMode::Reserved(owner) => {
                            // The state of the owner needs to be checked.
                            match owner.grant_write_access(journal.anchor()) {
                                Relationship::Committed(commit_instant) => {
                                    // The transaction was committed.
                                    *entry.get_mut() =
                                        ObjectState::Locked(LockMode::SharedAwaitable(
                                            SharedAwaitable::with_instant_and_owner(
                                                commit_instant,
                                                Owner::from(journal),
                                            ),
                                        ));
                                    return Ok(true);
                                }
                                Relationship::RolledBack => {
                                    // The transaction or the owner journal was rolled back.
                                    *entry.get_mut() =
                                        ObjectState::Locked(LockMode::Shared(Owner::from(journal)));
                                    return Ok(true);
                                }
                                Relationship::Linearizable => {
                                    // `Reserved` is stronger than `Shared`, so nothing to do.
                                    return Ok(false);
                                }
                                Relationship::Concurrent => {
                                    // TODO: intra-transaction deadlock - need to check this.
                                    return Err(Error::Deadlock);
                                }
                                Relationship::Unknown => {
                                    if deadline.is_some() {
                                        // Prepare for awaiting access to the database object.
                                        *locked = LockMode::ReservedAwaitable(
                                            ExclusiveAwaitable::with_instant_and_owner(
                                                S::Instant::default(),
                                                owner.clone(),
                                            ),
                                        );
                                    } else {
                                        // No deadline is specified.
                                        break;
                                    }
                                }
                            }
                        }
                        LockMode::ReservedAwaitable(exclusive_awaitable)
                        | LockMode::ExclusiveAwaitable(exclusive_awaitable)
                        | LockMode::MarkedAwaitable(exclusive_awaitable) => {
                            // TODO: intra-transaction ownership transfer.
                            if let Some(deadline) = deadline {
                                let message_sender = journal.message_sender();
                                let owner = Owner::from(journal);
                                let request = Request::Share(owner.clone());
                                exclusive_awaitable.push_request(request);
                                return AwaitResponse::new(owner, entry, message_sender, deadline)
                                    .await;
                            }
                            // No deadline is specified.
                            break;
                        }
                        LockMode::Shared(owner) => {
                            // TODO: intra-transaction ownership transfer.
                            let mut shared_awaitable = SharedAwaitable::with_instant_and_owner(
                                S::Instant::default(),
                                owner.clone(),
                            );
                            shared_awaitable.owner_set.insert(Owner::from(journal));
                            *entry.get_mut() =
                                ObjectState::Locked(LockMode::SharedAwaitable(shared_awaitable));
                            return Ok(true);
                        }
                        LockMode::SharedAwaitable(shared_awaitable) => {
                            // TODO: intra-transaction ownership transfer.
                            shared_awaitable.owner_set.insert(Owner::from(journal));
                            return Ok(true);
                        }
                        LockMode::Exclusive(owner) => {
                            // TODO: intra-transaction ownership transfer.
                            if deadline.is_some() {
                                // Prepare for awaiting access to the database object.
                                *locked = LockMode::ExclusiveAwaitable(
                                    ExclusiveAwaitable::with_instant_and_owner(
                                        S::Instant::default(),
                                        owner.clone(),
                                    ),
                                );
                            } else {
                                // No deadline is specified.
                                break;
                            }
                        }
                        LockMode::Marked(owner) => {
                            // TODO: intra-transaction ownership transfer.
                            if deadline.is_some() {
                                // Prepare for awaiting access to the database object.
                                *locked = LockMode::MarkedAwaitable(
                                    ExclusiveAwaitable::with_instant_and_owner(
                                        S::Instant::default(),
                                        owner.clone(),
                                    ),
                                );
                            } else {
                                // No deadline is specified.
                                break;
                            }
                        }
                    }
                }
                ObjectState::Created(instant) => {
                    // The database object is not owned or locked.
                    *entry.get_mut() = ObjectState::Locked(LockMode::SharedAwaitable(
                        SharedAwaitable::with_instant_and_owner(*instant, Owner::from(journal)),
                    ));
                    return Ok(true);
                }
                ObjectState::Deleted(_) => {
                    // Already deleted.
                    break;
                }
            }
        }

        // The database object has been created, deleted, or invisible.
        Err(Error::SerializationFailure)
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
                entry.insert_entry(ObjectState::Locked(LockMode::Exclusive(Owner::from(
                    journal,
                ))));
                return Ok(true);
            }
        };
        match entry.get_mut() {
            ObjectState::Locked(_) => {
                // TODO: try to acquire the lock after cleaning up the entry.
                Err(Error::Conflict)
            }
            ObjectState::Created(instant) => {
                *entry.get_mut() = ObjectState::Locked(LockMode::ExclusiveAwaitable(
                    ExclusiveAwaitable::with_instant_and_owner(*instant, Owner::from(journal)),
                ));
                Ok(true)
            }
            ObjectState::Deleted(_) => {
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
                entry.insert_entry(ObjectState::Locked(LockMode::Marked(Owner::from(journal))));
                return Ok(true);
            }
        };
        match entry.get_mut() {
            ObjectState::Locked(_) => {
                // TODO: try to mark it after cleaning up the entry.
                Err(Error::Conflict)
            }
            ObjectState::Created(instant) => {
                *entry.get_mut() = ObjectState::Locked(LockMode::MarkedAwaitable(
                    ExclusiveAwaitable::with_instant_and_owner(*instant, Owner::from(journal)),
                ));
                Ok(true)
            }
            ObjectState::Deleted(_) => {
                // Already deleted.
                Err(Error::SerializationFailure)
            }
        }
    }

    /// Transfers ownership to all the eligible waiting transactions.
    ///
    /// If the database object still need to be monitored, it returns `true`. It is a blocking and
    /// synchronous method invoked by [`Overseer`](super::overseer::Overseer).
    pub(super) fn transfer_ownership(&self, object_id: usize) -> bool {
        self.table
            .update(&object_id, |_, state| {
                loop {
                    if let ObjectState::Locked(locked) = state {
                        match locked {
                            LockMode::Reserved(_)
                            | LockMode::Shared(_)
                            | LockMode::Exclusive(_)
                            | LockMode::Marked(_) => {
                                // No waiting transactions.
                                return false;
                            }
                            LockMode::ReservedAwaitable(exclusive_awaitable) => {
                                if let Some(request) = exclusive_awaitable.wait_queue.clone_first()
                                {
                                    match exclusive_awaitable
                                        .owner
                                        .grant_write_access(request.owner())
                                    {
                                        Relationship::Committed(commit_instant) => {
                                            if let Some(new_state) =
                                                Self::transfer_committed_reserved_ownership(
                                                    request,
                                                    commit_instant,
                                                    &mut exclusive_awaitable.wait_queue,
                                                )
                                            {
                                                *state = new_state;
                                            }
                                        }
                                        Relationship::RolledBack => {
                                            if let Some(new_state) =
                                                Self::transfer_rolled_back_reserved_ownership(
                                                    request,
                                                    &mut exclusive_awaitable.wait_queue,
                                                )
                                            {
                                                *state = new_state;
                                            }
                                        }
                                        Relationship::Linearizable => {
                                            // TODO: intra-transaction ownership transfer.
                                            unimplemented!()
                                        }
                                        Relationship::Concurrent | Relationship::Unknown => {
                                            // The first waiting transaction cannot gain access to
                                            // the database object.
                                            return true;
                                        }
                                    }
                                } else {
                                    // No waiting transactions.
                                    *locked = LockMode::Reserved(exclusive_awaitable.owner.clone());
                                    return false;
                                }
                            }
                            LockMode::ExclusiveAwaitable(_) => {
                                // TODO: unlock handling.
                                todo!()
                            }
                            LockMode::MarkedAwaitable(_) => {
                                // TODO: deletion handling.
                                unimplemented!()
                            }
                            LockMode::SharedAwaitable(shared_awaitable) => {
                                if let Some(_request) = shared_awaitable.wait_queue.clone_first() {
                                    // TODO: shared-lock handling
                                    panic!("unimplemented");
                                }
                                return false;
                            }
                        }
                    } else {
                        // The database object was created or deleted.
                        return false;
                    }
                }
            })
            .map_or(false, |r| r)
    }

    /// Transfer reserve ownership from the committed transaction to the waiting transaction.
    ///
    /// Returns an updated database object state if there was a change to it. This function always
    /// pops the first request from the wait queue, therefore progress of the caller is guaranteed.
    fn transfer_committed_reserved_ownership(
        request: Request<S>,
        creation_instant: S::Instant,
        wait_queue: &mut WaitQueue<S>,
    ) -> Option<ObjectState<S>> {
        let wait_queue_empty = wait_queue.remove_first();
        match request {
            Request::Reserve(requester) => {
                // It is not possible to reserve the database object after another transaction has
                // successfully created it.
                requester.set_result(Err(Error::SerializationFailure), None)?;
                if wait_queue_empty {
                    Some(ObjectState::Created(creation_instant))
                } else {
                    None
                }
            }
            Request::Share(requester) => {
                let mut shared_awaitable =
                    SharedAwaitable::with_instant_and_owner(creation_instant, requester.clone());
                requester.set_result(Ok(true), None)?;
                shared_awaitable.wait_queue = take(wait_queue);
                Some(ObjectState::Locked(LockMode::SharedAwaitable(
                    shared_awaitable,
                )))
            }
            Request::Own(requester) => {
                let mut exclusive_awaitable =
                    ExclusiveAwaitable::with_instant_and_owner(creation_instant, requester.clone());
                requester.set_result(Ok(true), None)?;
                exclusive_awaitable.wait_queue = take(wait_queue);
                Some(ObjectState::Locked(LockMode::ExclusiveAwaitable(
                    exclusive_awaitable,
                )))
            }
            Request::Marked(requester) => {
                let mut exclusive_awaitable =
                    ExclusiveAwaitable::with_instant_and_owner(creation_instant, requester.clone());
                requester.set_result(Ok(true), None)?;
                exclusive_awaitable.wait_queue = take(wait_queue);
                Some(ObjectState::Locked(LockMode::MarkedAwaitable(
                    exclusive_awaitable,
                )))
            }
        }
    }

    /// Transfer reserve ownership from the rolled back journal to the waiting transaction.
    ///
    /// Returns an updated database object state. This function always pops the first request from
    /// the wait queue, therefore progress of the caller is guaranteed.
    fn transfer_rolled_back_reserved_ownership(
        request: Request<S>,
        wait_queue: &mut WaitQueue<S>,
    ) -> Option<ObjectState<S>> {
        let wait_queue_empty = wait_queue.remove_first();
        match request {
            Request::Reserve(requester) => {
                if wait_queue_empty {
                    requester.set_result(Ok(true), None)?;
                    Some(ObjectState::Locked(LockMode::Reserved(requester)))
                } else {
                    let mut exclusive_awaitable = ExclusiveAwaitable::with_instant_and_owner(
                        S::Instant::default(),
                        requester.clone(),
                    );
                    requester.set_result(Ok(true), None)?;
                    exclusive_awaitable.wait_queue = take(wait_queue);
                    Some(ObjectState::Locked(LockMode::ReservedAwaitable(
                        exclusive_awaitable,
                    )))
                }
            }
            Request::Share(requester) => {
                if wait_queue_empty {
                    requester.set_result(Ok(true), None)?;
                    Some(ObjectState::Locked(LockMode::Shared(requester)))
                } else {
                    let mut shared_awaitable = SharedAwaitable::with_instant_and_owner(
                        S::Instant::default(),
                        requester.clone(),
                    );
                    requester.set_result(Ok(true), None)?;
                    shared_awaitable.wait_queue = take(wait_queue);
                    Some(ObjectState::Locked(LockMode::SharedAwaitable(
                        shared_awaitable,
                    )))
                }
            }
            Request::Own(requester) => {
                if wait_queue_empty {
                    requester.set_result(Ok(true), None)?;
                    Some(ObjectState::Locked(LockMode::Exclusive(requester)))
                } else {
                    let mut exclusive_awaitable = ExclusiveAwaitable::with_instant_and_owner(
                        S::Instant::default(),
                        requester.clone(),
                    );
                    requester.set_result(Ok(true), None)?;
                    exclusive_awaitable.wait_queue = take(wait_queue);
                    Some(ObjectState::Locked(LockMode::ExclusiveAwaitable(
                        exclusive_awaitable,
                    )))
                }
            }
            Request::Marked(requester) => {
                if wait_queue_empty {
                    requester.set_result(Ok(true), None)?;
                    Some(ObjectState::Locked(LockMode::Marked(requester)))
                } else {
                    let mut exclusive_awaitable = ExclusiveAwaitable::with_instant_and_owner(
                        S::Instant::default(),
                        requester.clone(),
                    );
                    requester.set_result(Ok(true), None)?;
                    exclusive_awaitable.wait_queue = take(wait_queue);
                    Some(ObjectState::Locked(LockMode::MarkedAwaitable(
                        exclusive_awaitable,
                    )))
                }
            }
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
    fn set_result(
        &self,
        result: Result<bool, Error>,
        promotion_result: Option<PromotedAccess<S>>,
    ) -> Option<()> {
        if let Ok(mut r) = self.access_request_result_placeholder().lock() {
            if let Err(error) = r.set_result(result, promotion_result) {
                // `Error::Timeout` can be set by the requester.
                debug_assert_eq!(error, Error::Timeout);
                return None;
            }
        }
        Some(())
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
            Request::Reserve(owner)
            | Request::Share(owner)
            | Request::Own(owner)
            | Request::Marked(owner) => owner,
        }
    }
}

impl<S: Sequencer> Clone for Request<S> {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            Self::Reserve(owner) => Self::Reserve(owner.clone()),
            Self::Share(owner) => Self::Share(owner.clone()),
            Self::Own(owner) => Self::Own(owner.clone()),
            Self::Marked(owner) => Self::Marked(owner.clone()),
        }
    }
}

impl<S: Sequencer> SharedAwaitable<S> {
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

    /// Pushes a request into the wait queue.
    #[allow(dead_code)]
    fn push_request(&mut self, request: Request<S>) {
        self.wait_queue.push_back(request);
        self.owner_set
            .iter()
            .for_each(|o| o.need_to_wake_up_others());
    }
}

impl<S: Sequencer> ExclusiveAwaitable<S> {
    /// Creates a new [`ExclusiveAwaitable`] with a single owner inserted.
    fn with_instant_and_owner(
        creation_instant: S::Instant,
        owner: Owner<S>,
    ) -> Box<ExclusiveAwaitable<S>> {
        Box::new(ExclusiveAwaitable {
            creation_instant,
            owner,
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
    /// Peeks the first waiting transaction.
    fn clone_first(&self) -> Option<Request<S>> {
        self.front().map(Clone::clone)
    }

    /// Removes the first waiting transaction.
    ///
    /// Returns `true` if the wait queue is empty.
    fn remove_first(&mut self) -> bool {
        self.pop_front();
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{AtomicCounter, Database};
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    static_assertions::assert_eq_size!(ObjectState<AtomicCounter>, [u8; 16]);

    const TIMEOUT_UNEXPECTED: Duration = Duration::from_secs(256);
    const TIMEOUT_EXPECTED: Duration = Duration::from_millis(256);

    impl ToObjectID for usize {
        fn to_object_id(&self) -> usize {
            *self
        }
    }

    struct ShortSleep(Instant);

    impl Future for ShortSleep {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.0 + TIMEOUT_EXPECTED < Instant::now() {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    #[tokio::test]
    async fn reserve_read() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(false),
        );
    }

    #[tokio::test]
    async fn reserve_prepare_read() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();
        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(false),
        );
        drop(prepared);
    }

    #[tokio::test]
    async fn reserve_rewind_reserve() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        drop(journal);
        assert!(transaction.commit().await.is_ok());

        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn reserve_rollback_reserve() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        let rollback_runner = async { transaction.rollback().await };

        let transaction = database.transaction();
        let mut journal = transaction.journal();
        let (result, _) = futures::join!(
            access_controller.reserve(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            rollback_runner
        );
        assert_eq!(result, Ok(true));
    }

    #[tokio::test]
    async fn reserve_rollback_reserve_timeout_reserve() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();

        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert_eq!(
            access_controller
                .reserve(&0, &mut journal, Some(Instant::now() + TIMEOUT_EXPECTED))
                .await,
            Err(Error::Timeout)
        );
        drop(journal);
        drop(prepared);

        let mut journal = transaction.journal();
        assert_eq!(
            access_controller
                .reserve(&0, &mut journal, Some(Instant::now() + TIMEOUT_EXPECTED))
                .await,
            Ok(true),
        );
    }

    #[tokio::test]
    async fn reserve_prepare_read_timeout() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
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
    async fn reserve_commit_read() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();
        let mut database_snapshot = 0;
        let read_runner = async {
            let snapshot = database.snapshot();
            database_snapshot = snapshot.database_snapshot();
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED))
                .await
        };

        let (visible, committed) = futures::join!(read_runner, prepared);
        let commit_instant = committed.unwrap();
        let visible = visible.unwrap();
        assert_eq!(visible, commit_instant <= database_snapshot);
    }

    #[tokio::test]
    async fn reserve_commit_reserve() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();

        let transaction = database.transaction();
        let mut journal = transaction.journal();
        let (reserved, committed) = futures::join!(
            access_controller.reserve(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            prepared
        );
        assert_eq!(reserved, Err(Error::SerializationFailure));
        assert!(committed.is_ok());
    }

    #[tokio::test]
    async fn reserve_rollback_share_read() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        transaction.rollback().await;

        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .share(&0, &mut journal, None)
            .await
            .unwrap());
        assert_eq!(journal.submit(), 1);
        assert!(transaction.commit().await.is_ok());

        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(true),
        );
    }

    #[tokio::test]
    async fn reserve_commit_share_read() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();

        let old_snapshot = database.snapshot();

        let transaction = database.transaction();
        let mut journal = transaction.journal();
        let (shared, committed) = futures::join!(
            access_controller.share(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            prepared
        );

        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(true),
        );

        assert!(committed.is_ok());
        assert_eq!(shared, Ok(true));

        assert_eq!(
            access_controller
                .read(&0, &old_snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(false),
        );
    }

    #[tokio::test]
    async fn reserve_rollback_read() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
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
    async fn share_read() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .share(&0, &mut journal, None)
            .await
            .unwrap());
        assert_eq!(journal.submit(), 1);

        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(true),
        );

        assert!(transaction.commit().await.is_ok());

        let snapshot = database.snapshot();
        assert_eq!(
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(true),
        );
    }

    #[tokio::test]
    async fn share_share_share() {
        let database = Database::default();
        let access_controller = database.access_controller();
        let transaction_1 = database.transaction();
        let mut journal_1 = transaction_1.journal();
        let transaction_2 = database.transaction();
        let mut journal_2 = transaction_2.journal();
        let transaction_3 = database.transaction();
        let mut journal_3 = transaction_3.journal();
        let (shared_1, shared_2, shared_3) = futures::join!(
            access_controller.share(&0, &mut journal_1, None),
            access_controller.share(&0, &mut journal_2, None),
            access_controller.share(&0, &mut journal_3, None)
        );
        assert_eq!(shared_1, Ok(true));
        assert_eq!(shared_2, Ok(true));
        assert_eq!(shared_3, Ok(true));
    }

    #[tokio::test]
    async fn lock() {
        let database = Database::default();
        let access_controller = database.access_controller();
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
    async fn mark() {
        let database = Database::default();
        let access_controller = database.access_controller();
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
