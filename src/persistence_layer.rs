// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

mod file_io;
pub use file_io::FileIO;

use super::{Database, Error, JournalID, Sequencer, TransactionID};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::num::{NonZeroU32, NonZeroU64};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

/// The [`PersistenceLayer`] trait defines the interface between [`Database`](super::Database) and
/// the persistence layer of the database.
///
/// There is no ordering among IO operations except for [`PersistenceLayer::rewind`],
/// [`PersistenceLayer::prepare`] and [`PersistenceLayer::commit`]; other operations can be
/// strictly ordered by locking the corresponding database resources through
/// [`AccessController`](super::AccessController), whereas those operations are followed by
/// releasing any acquired locks; therefore, before other transactions acquire the released locks,
/// all the dependent log records must reach the log buffer.
///
/// The content of each log record must be *idempotent*; the same log record can be applied to the
/// database more than once on recovery if the log record is close to a checkpoint.
pub trait PersistenceLayer<S: Sequencer>: 'static + Debug + Send + Sized + Sync {
    /// [`PersistenceLayer::LogBuffer`] is kept in a transaction journal to store own log records
    /// until the transaction or journal is ended.
    type LogBuffer: LogBufferInterface;

    /// Recovers the database before serving any other requests.
    ///
    /// A call to `recover` must precede any other calls to other methods in the
    /// [`PersistenceLayer`], and the [`PersistenceLayer`] must not serve any other requests until
    /// fully recovered. If a specific logical instant is specified, it only recovers the storage
    /// up until the time point.
    ///
    /// The supplied [`Database`] must be in an initial state.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the database could not be recovered.
    fn recover(
        &self,
        database: Database<S, Self>,
        until: Option<S::Instant>,
        deadline: Option<Instant>,
    ) -> Result<AwaitRecovery<S, Self>, Error>;

    /// Backs up the complete database.
    ///
    /// If a path is specified, backed up data is stored in it, otherwise a default path set by the
    /// persistence layer will be used.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the database could not be backed up.
    fn backup(
        &self,
        database: &Database<S, Self>,
        catalog_only: bool,
        path: Option<&str>,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, Self>;

    /// Manually generates a checkpoint.
    fn checkpoint(
        &self,
        database: &Database<S, Self>,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, Self>;

    /// The transaction is participating in a distributed transaction.
    fn participate(
        &self,
        transaction_id: TransactionID,
        xid: &[u8],
        deadline: Option<Instant>,
    ) -> AwaitIO<S, Self>;

    /// Writes the fact that the supplied database objects have been created.
    ///
    /// Full buffers used in the method are automatically submitted to the persistence layer, and a
    /// new log buffer is allocated, therefore the supplied log buffer and the returned one may
    /// differ.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if it fails to write the data to the log buffer.
    fn create(
        &self,
        log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        journal_id: JournalID,
        object_ids: &[u64],
    ) -> Result<Arc<Self::LogBuffer>, Error>;

    /// Writes the fact that the supplied database objects have been deleted.
    ///
    /// Full buffers used in the method are automatically submitted to the persistence layer, and a
    /// new log buffer is allocated, therefore the supplied log buffer and the returned one may
    /// differ.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if it fails to write the data to the log buffer.
    fn delete(
        &self,
        log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        journal_id: JournalID,
        object_ids: &[u64],
    ) -> Result<Arc<Self::LogBuffer>, Error>;

    /// Submits the content of the log buffer.
    ///
    /// This method is invoked when the associated journal is submitted or a log buffer is full.
    ///
    /// `transaction_instant` is given `None` if the journal is still usable.
    fn submit(
        &self,
        log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        journal_id: JournalID,
        transaction_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    );

    /// Discards the content of the log buffer.
    ///
    /// This method is invoked when the associated journal is discarded without being submitted to
    /// the transaction.
    fn discard(
        &self,
        log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        journal_id: JournalID,
        deadline: Option<Instant>,
    );

    /// A transaction is being rewound.
    ///
    /// Rewinding the transaction to `transaction_instant == 0` amounts to rolling back the entire
    /// transaction. This only generates a log record indicating that the transaction was rolled
    /// back, and the corresponding unreachable database objects are cleaned up in the background.
    fn rewind(
        &self,
        log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        transaction_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    );

    /// A transaction is being prepared for commit.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the device.
    fn prepare(
        &self,
        log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        prepare_instant: S::Instant,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, Self>;

    /// A transaction is being committed.
    ///
    /// It constructs the content of a log record containing the fact that the transaction is being
    /// committed at the specified time point, and then returns a [`Future`] that actually waits
    /// for the content to be persisted.
    fn commit(
        &self,
        log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        commit_instant: S::Instant,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, Self>;

    /// Returns the current flush epoch.
    ///
    /// If the persistence layer has never been synchronized with the device, `None` is returned.
    fn current_flush_epoch(&self) -> Option<NonZeroU64>;

    /// Checks if the IO operation became durable by comparing the current flush epoch of the
    /// persistence layer and the specified one.
    ///
    /// If `None` is given as `expected_flush_epoch` or the current flush epoch has not reached the
    /// specified one, it stores the supplied [`Waker`] in it and returns `false`.
    fn check_io_completion(&self, expected_flush_epoch: Option<NonZeroU64>, waker: &Waker) -> bool;

    /// Checks if the database has been recovered from the persistence layer.
    ///
    /// Returns `None` if the database is being recovered.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if something went wrong during recovery.
    fn check_recovery(&self, waker: &Waker) -> Result<RecoveryResult<S, Self>, Error>;

    /// Cancels database recovery.
    fn cancel_recovery(&self);
}

/// The interface between a log buffer and the persistence layer.
pub trait LogBufferInterface: Debug + Default + Send + Sized {
    /// The closest flush epoch when the log buffer will be durable.
    ///
    /// The flush epoch values are set only once, calling the method more than once is undefined
    /// behavior.
    fn set_durable_flush_epoch(&self, flush_epoch: u64);

    /// The [`AwaitIO`] associated with the log buffer uses the flush epoch value assigned by the
    /// log buffer processor to check whether the persistence layer is synchronized with the device
    /// after the log buffer was processed.
    ///
    /// Returns `None` if a durable flush epoch has yet to be assigned to it.
    fn get_durable_flush_epoch(&self) -> Option<NonZeroU64>;
}

/// The result of database recovery.
#[derive(Debug)]
pub enum RecoveryResult<S: Sequencer, P: PersistenceLayer<S>> {
    /// Unable to check the recovery status.
    ///
    /// Needs to poll the recovery status.
    Unknown,

    /// The database is being recovered.
    InProgress,

    /// The database has been recovered.
    Recovered(Database<S, P>),
}

/// [`AwaitIO`] is returned by a [`PersistenceLayer`] if the content of a log record was
/// successfully materialized in memory and ready for being persisted.
///
/// Dropping an [`AwaitIO`] without awaiting it is allowed if the user does not need to wait for an
/// IO completion. If an [`AwaitIO`] returns an [`Error`], all the future [`PersistenceLayer`]
/// operations shall fail until recovered.
#[derive(Debug)]
pub struct AwaitIO<'p, S: Sequencer, P: PersistenceLayer<S>> {
    /// The persistence layer by which the IO operation is performed.
    persistence_layer: &'p P,

    /// The log buffer.
    log_buffer: Arc<P::LogBuffer>,

    /// The deadline of the IO operation.
    deadline: Option<Instant>,
}

/// [`AwaitRecovery`] is returned by a [`PersistenceLayer`] after triggering a database recovery.
///
/// Dropping an [`AwaitRecovery`] without awaiting it is allowed if a timeout value is specified.
#[derive(Debug)]
pub struct AwaitRecovery<'p, S: Sequencer, P: PersistenceLayer<S>> {
    /// The persistence layer from which the data is read.
    persistence_layer: &'p P,

    /// The deadline of the IO operation.
    deadline: Option<Instant>,

    /// Phantom to use `S`.
    _phantom: PhantomData<S>,
}

impl<'p, S: Sequencer, P: PersistenceLayer<S>> AwaitIO<'p, S, P> {
    /// Creates an [`AwaitIO`] from a log buffer.
    #[inline]
    pub fn with_log_buffer(
        persistence_layer: &'p P,
        log_buffer: Arc<P::LogBuffer>,
        deadline: Option<Instant>,
    ) -> AwaitIO<'p, S, P> {
        AwaitIO {
            persistence_layer,
            log_buffer,
            deadline,
        }
    }
}

impl<'p, S: Sequencer, P: PersistenceLayer<S>> Future for AwaitIO<'p, S, P> {
    type Output = Result<(), Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let flush_epoch = self.log_buffer.get_durable_flush_epoch();
        if self
            .persistence_layer
            .check_io_completion(flush_epoch, cx.waker())
        {
            Poll::Ready(Ok(()))
        } else if self
            .deadline
            .as_ref()
            .map_or(false, |d| *d < Instant::now())
        {
            Poll::Ready(Err(Error::Timeout))
        } else {
            // It assumes that the persistence layer will wake up the executor when ready.
            let new_flush_epoch = self.log_buffer.get_durable_flush_epoch();
            if flush_epoch != new_flush_epoch {
                // Poll again since the flush epoch has changed.
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}

impl<'p, S: Sequencer, P: PersistenceLayer<S>> Future for AwaitRecovery<'p, S, P> {
    type Output = Result<Database<S, P>, Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.persistence_layer.check_recovery(cx.waker()) {
            Ok(recovery_result) => match recovery_result {
                RecoveryResult::Unknown => {
                    cx.waker().wake_by_ref();
                }
                RecoveryResult::InProgress => (),
                RecoveryResult::Recovered(database) => {
                    return Poll::Ready(Ok(database));
                }
            },
            Err(error) => return Poll::Ready(Err(error)),
        };
        if self
            .deadline
            .as_ref()
            .map_or(false, |d| *d < Instant::now())
        {
            self.persistence_layer.cancel_recovery();
            Poll::Ready(Err(Error::Timeout))
        } else {
            Poll::Pending
        }
    }
}
