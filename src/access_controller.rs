// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor as JournalAnchor;
use super::journal::WritePermission;
use super::overseer::Task;
use super::{Error, Journal, PersistenceLayer, Sequencer, Snapshot};
use scc::hash_map::Entry as MapEntry;
use scc::hash_map::OccupiedEntry;
use scc::{ebr, HashMap};
use std::cmp;
use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::mpsc::SyncSender;
use std::task::{Context, Poll, Waker};
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

#[derive(Debug)]
enum ObjectState<S: Sequencer> {
    /// The database object is locked.
    Locked(LockMode<S>),

    /// The database object was created at the instant.
    #[allow(dead_code)]
    Created(S::Instant),

    /// The database object was deleted at the instant.
    #[allow(dead_code)]
    Deleted(S::Instant),
}

#[derive(Debug)]
enum LockMode<S: Sequencer> {
    /// The database object is prepared to be created.
    Reserved(Owner<S>),

    /// The database object is prepared to be created, but there are waiting transactions.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    MarkedAwaitable(Box<ExclusiveAwaitable<S>>),
}

#[derive(Debug)]
enum Desire<S: Sequencer> {
    /// Desires to acquire a shared lock on the database object.
    #[allow(dead_code)]
    Shared(Owner<S>, Option<Waker>),

    /// Desires to acquire the exclusive lock on the database object.
    #[allow(dead_code)]
    Exclusive(Owner<S>, Option<Waker>),

    /// Desires to acquire the exclusive lock on the database object for deletion.
    #[allow(dead_code)]
    Marked(Owner<S>, Option<Waker>),
}

#[derive(Debug, Default)]
struct WaitQueue<S: Sequencer> {
    #[allow(dead_code)]
    wait_queue: VecDeque<Desire<S>>,
}

#[derive(Debug)]
struct SharedAwaitable<S: Sequencer> {
    /// The instant when the database object was created.
    ///
    /// The value is equal to `S::Instant::default()` if the database object is universally
    /// visible.
    creation_instant: S::Instant,

    /// A set of owners.
    #[allow(dead_code)]
    owner_set: BTreeSet<Owner<S>>,

    /// The wait queue of the database object.
    #[allow(dead_code)]
    wait_queue: WaitQueue<S>,
}

#[derive(Debug)]
struct ExclusiveAwaitable<S: Sequencer> {
    /// The instant when the database object was created.
    ///
    /// The value is equal to `S::Instant::default()` if the database object is universally
    /// visible.
    creation_instant: S::Instant,

    /// The only owner.
    owner: Owner<S>,

    /// The wait queue of the database object.
    #[allow(dead_code)]
    wait_queue: WaitQueue<S>,
}

#[derive(Debug)]
struct AwaitAccessPermission<'a, 'd, S: Sequencer> {
    /// The [`AccessController`].
    #[allow(dead_code)]
    access_controller: &'a AccessController<S>,

    /// The identifier of the desired object.
    #[allow(dead_code)]
    object_id: usize,

    /// The owner.
    #[allow(dead_code)]
    owner: Owner<S>,

    /// The message sender to which send a wake up message.
    message_sender: &'d SyncSender<Task>,

    /// The deadline.
    deadline: Instant,
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
    /// An [`Error`] is returned if the specified deadline was reached or memory allocation failed
    /// when pushing a [`Waker`](std::task::Waker) into the owner
    /// [`Transaction`](super::Transaction).
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
                                // The database object is temporarily locked.
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
                entry.insert_entry(ObjectState::Locked(LockMode::Reserved(Owner::from(
                    journal,
                ))));
                return Ok(());
            }
        };
        if let ObjectState::Locked(locked) = entry.get_mut() {
            // TODO: wait for the owner to be rolled or committed.
            match locked {
                LockMode::Reserved(owner) => {
                    // The state of the owner needs to be checked.
                    match owner.grant_write_access(journal) {
                        WritePermission::Committed(_) => {
                            // The transaction was committed.
                            return Err(Error::SerializationFailure);
                        }
                        WritePermission::RolledBack => {
                            // The transaction or the owner journal was rolled back.
                            *entry.get_mut() =
                                ObjectState::Locked(LockMode::Reserved(Owner::from(journal)));
                            return Ok(());
                        }
                        WritePermission::Linearizable => {
                            // Already reserved in a previous journal.
                            return Ok(());
                        }
                        WritePermission::Concurrent => {
                            // TODO: intra-transaction deadlock - need to check this.
                            return Err(Error::Deadlock);
                        }
                        WritePermission::Rejected => {
                            // TODO: wait.
                            return Err(Error::Conflict);
                        }
                    }
                }
                LockMode::ReservedAwaitable(_) => {
                    // TODO: wait.
                    return Err(Error::Conflict);
                }
                _ => (),
            }
        }

        // The database object has already been created or deleted.
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
                entry.insert_entry(ObjectState::Locked(LockMode::Shared(Owner::from(journal))));
                return Ok(true);
            }
        };

        match entry.get_mut() {
            ObjectState::Locked(locked) => {
                match locked {
                    LockMode::Reserved(owner) => {
                        // The state of the owner needs to be checked.
                        match owner.grant_write_access(journal) {
                            WritePermission::Committed(commit_instant) => {
                                // The transaction was committed.
                                *entry.get_mut() = ObjectState::Locked(LockMode::SharedAwaitable(
                                    SharedAwaitable::with_instant_and_owner(
                                        commit_instant,
                                        Owner::from(journal),
                                    ),
                                ));
                                Ok(true)
                            }
                            WritePermission::RolledBack => {
                                // The transaction or the owner journal was rolled back.
                                *entry.get_mut() =
                                    ObjectState::Locked(LockMode::Shared(Owner::from(journal)));
                                Ok(true)
                            }
                            WritePermission::Linearizable => {
                                // `Reserved` is stronger than `Shared`, so nothing to do.
                                Ok(true)
                            }
                            WritePermission::Concurrent => {
                                // TODO: intra-transaction deadlock - need to check this.
                                Err(Error::Deadlock)
                            }
                            WritePermission::Rejected => {
                                // TODO: wait.
                                Err(Error::Conflict)
                            }
                        }
                    }
                    _ => {
                        // TODO: try to add the transaction to the owner set.
                        Err(Error::Conflict)
                    }
                }
            }
            ObjectState::Created(instant) => {
                // The database object is not owned or locked.
                *entry.get_mut() = ObjectState::Locked(LockMode::SharedAwaitable(
                    SharedAwaitable::with_instant_and_owner(*instant, Owner::from(journal)),
                ));
                Ok(true)
            }
            ObjectState::Deleted(_) => {
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

    /// Awaits access to the database object.
    #[allow(dead_code)]
    async fn await_access<'a>(
        &'a self,
        entry: OccupiedEntry<'a, usize, ObjectState<S>>,
        desired_access: Desire<S>,
        message_sender: &SyncSender<Task>,
        deadline: Instant,
    ) -> Result<(), Error> {
        let awaitable = AwaitAccessPermission {
            access_controller: self,
            object_id: *entry.key(),
            owner: desired_access.owner().clone(),
            message_sender,
            deadline,
        };
        awaitable.await
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

impl<S: Sequencer> Desire<S> {
    /// Returns a reference to its `owner` field.
    fn owner(&self) -> &Owner<S> {
        match self {
            Desire::Shared(owner, _) | Desire::Exclusive(owner, _) | Desire::Marked(owner, _) => {
                owner
            }
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
}

impl<'a, 'd, S: Sequencer> Future for AwaitAccessPermission<'a, 'd, S> {
    type Output = Result<(), Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.deadline < Instant::now() {
            // The deadline was reached.
            return Poll::Ready(Err(Error::Timeout));
        }

        // TODO: try to acquire the resource.

        if self
            .message_sender
            .try_send(Task::WakeUp(self.deadline, cx.waker().clone()))
            .is_err()
        {
            // The message channel is congested.
            cx.waker().wake_by_ref();
        }
        Poll::Pending
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

    #[tokio::test]
    async fn reserve_read() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
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
        let access_controller = AccessController::<AtomicCounter>::default();
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
    async fn reserve_prepare_reserve() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
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
        assert_eq!(reserved, Err(Error::Conflict));
        assert!(committed.is_ok());
    }

    #[tokio::test]
    async fn reserve_rewind_reserve() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
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
        let access_controller = AccessController::<AtomicCounter>::default();
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
            .reserve(&0, &mut journal, Some(Instant::now() + TIMEOUT_UNEXPECTED))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn reserve_prepare_timeout() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
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
        let access_controller = AccessController::<AtomicCounter>::default();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);
        let prepared = transaction.prepare().await.unwrap();
        let mut database_snapshot = 0;
        let reader = async {
            let snapshot = database.snapshot();
            database_snapshot = snapshot.database_snapshot();
            access_controller
                .read(&0, &snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED))
                .await
        };

        let result = futures::join!(prepared, reader);
        let commit_instant = result.0.unwrap();
        let visible = result.1.unwrap();
        assert_eq!(visible, commit_instant <= database_snapshot);
    }

    #[tokio::test]
    async fn reserve_commit_share_read() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
        let transaction = database.transaction();
        let mut journal = transaction.journal();
        assert!(access_controller
            .reserve(&0, &mut journal, None)
            .await
            .is_ok());
        assert_eq!(journal.submit(), 1);

        let old_snapshot = database.snapshot();

        assert!(transaction.commit().await.is_ok());

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
        assert_eq!(
            access_controller
                .read(&0, &old_snapshot, Some(Instant::now() + TIMEOUT_UNEXPECTED),)
                .await,
            Ok(false),
        );
    }

    #[tokio::test]
    async fn reserve_rollback_share_read() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
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
    async fn reserve_rollback_read() {
        let database = Database::default();
        let access_controller = AccessController::<AtomicCounter>::default();
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
        let access_controller = AccessController::<AtomicCounter>::default();
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
    async fn lock() {
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
    async fn mark() {
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
