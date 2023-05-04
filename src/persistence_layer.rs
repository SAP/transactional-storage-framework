// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

mod file_io;
pub use file_io::FileIO;

use super::{Database, Error, JournalID, Sequencer, TransactionID};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::num::NonZeroU32;
use std::pin::Pin;
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
    type LogBuffer: BufferredLogger<S, Self>;

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
    ) -> Result<AwaitIO<S, Self>, Error>;

    /// Manually generates a checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if a checkpoint could not be generated within the specified deadline.
    fn checkpoint(
        &self,
        database: &Database<S, Self>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error>;

    /// The transaction is participating in a distributed transaction.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the device.
    fn participate(
        &self,
        id: TransactionID,
        xid: &[u8],
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error>;

    /// A transaction is being rewound.
    ///
    /// Rewinding the transaction to `transaction_instant == 0` amounts to rolling back the entire
    /// transaction. This only generates a log record indicating that the transaction was rolled
    /// back, and the corresponding unreachable database objects are cleaned up in the background.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the device.
    fn rewind(
        &self,
        id: TransactionID,
        transaction_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error>;

    /// A transaction is being prepared for commit.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the device.
    fn prepare(
        &self,
        id: TransactionID,
        prepare_instant: S::Instant,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error>;

    /// A transaction is being committed.
    ///
    /// It constructs the content of a log record containing the fact that the transaction is being
    /// committed at the specified time point, and then returns a [`Future`] that actually waits
    /// for the content to be persisted.
    fn commit(
        &self,
        log_buffer: Box<Self::LogBuffer>,
        id: TransactionID,
        commit_instant: S::Instant,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, Self>;

    /// Checks if the IO operation associated with the log sequence number was completed.
    ///
    /// If the IO operation is still in progress, the supplied [`Waker`] is kept in the
    /// [`PersistenceLayer`] and notifies it when the operation is completed.
    ///
    /// It returns the latest known logical instant value of the database.
    fn check_io_completion(&self, lsn: u64, waker: &Waker) -> Option<Result<S::Instant, Error>>;

    /// Checks if the database has been recovered from the persistence layer.
    ///
    /// Returns `None` if the database is being recovered.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if something went wrong during recovery.
    fn check_recovery(&self, waker: &Waker) -> Result<Option<Database<S, Self>>, Error>;

    /// Cancels database recovery.
    fn cancel_recovery(&self);
}

/// [`BufferredLogger`] keeps log records until an explicit call to [`BufferredLogger::flush`] is
/// made.
pub trait BufferredLogger<S: Sequencer, P: PersistenceLayer<S>>:
    Debug + Default + Send + Sized
{
    /// Records database changes to the buffer.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the buffer.
    fn record<W: FnOnce(&mut [MaybeUninit<u8>])>(
        &mut self,
        id: TransactionID,
        journal_id: JournalID,
        len: usize,
        writer: W,
    ) -> Result<(), Error>;

    /// Flushes the content.
    ///
    /// This method is invoked when the associated journal is submitted or a log record is created
    /// and immediately submitted.
    fn flush(
        self: Box<Self>,
        persistence_layer: &P,
        submit_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, P>;
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

    /// The log sequence number to await.
    lsn: u64,

    /// The deadline of the IO operation.
    deadline: Option<Instant>,

    /// Phantom to use `S`.
    _phantom: PhantomData<S>,
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
    /// Creates an [`AwaitIO`] from a log sequence number.
    #[inline]
    pub fn with_lsn(persistence_layer: &'p P, lsn: u64) -> AwaitIO<'p, S, P> {
        AwaitIO {
            persistence_layer,
            lsn,
            deadline: None,
            _phantom: PhantomData,
        }
    }

    /// Sets the deadline.
    #[inline]
    #[must_use]
    pub fn set_deadline(self, deadline: Option<Instant>) -> AwaitIO<'p, S, P> {
        AwaitIO {
            persistence_layer: self.persistence_layer,
            lsn: self.lsn,
            deadline,
            _phantom: PhantomData,
        }
    }

    /// Forgets the IO operation.
    #[inline]
    pub fn forget(self) {
        // Do nothing.
    }
}

impl<'p, S: Sequencer, P: PersistenceLayer<S>> Future for AwaitIO<'p, S, P> {
    type Output = Result<S::Instant, Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self
            .persistence_layer
            .check_io_completion(self.lsn, cx.waker())
        {
            Poll::Ready(result)
        } else if self
            .deadline
            .as_ref()
            .map_or(false, |d| *d < Instant::now())
        {
            Poll::Ready(Err(Error::Timeout))
        } else {
            // It assumes that the persistence layer will wake up the executor when ready.
            Poll::Pending
        }
    }
}

impl<'p, S: Sequencer, P: PersistenceLayer<S>> Future for AwaitRecovery<'p, S, P> {
    type Output = Result<Database<S, P>, Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.persistence_layer.check_recovery(cx.waker()) {
            Ok(Some(database)) => Poll::Ready(Ok(database)),
            Ok(None) => {
                if self
                    .deadline
                    .as_ref()
                    .map_or(false, |d| *d < Instant::now())
                {
                    self.persistence_layer.cancel_recovery();
                    Poll::Ready(Err(Error::Timeout))
                } else {
                    // It assumes that the persistence layer will wake up the executor when ready.
                    Poll::Pending
                }
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}
