// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::utils;
use super::{Database, Error, JournalID, Sequencer, TransactionID};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};
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

    /// Recovers the database.
    ///
    /// If a specific logical instant is specified, it only recovers the storage up until the time
    /// point.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the database could not be recovered.
    fn recover(
        &self,
        database: &mut Database<S, Self>,
        until: Option<S::Instant>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error>;

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

    /// A [`Journal`](super::Journal) was submitted.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the device.
    fn submit(
        &self,
        id: TransactionID,
        journal_id: JournalID,
        transaction_instant: usize,
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
        transaction_instant: u32,
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
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the device.
    fn commit(
        &self,
        id: TransactionID,
        commit_instant: S::Instant,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error>;

    /// Checks if the IO operation was completed.
    ///
    /// If the IO operation is still in progress, the supplied [`Waker`] is kept in the
    /// [`PersistenceLayer`] and notifies it when the operation is completed.
    ///
    /// It returns the latest known logical instant value of the database.
    fn check(&self, io_id: usize, waker: &Waker) -> Option<Result<S::Instant, Error>>;
}

/// [`BufferredLogger`] keeps log records until an explicit call to [`BufferredLogger::flush`] is
/// made.
pub trait BufferredLogger<S: Sequencer, P: PersistenceLayer<S>>: Debug + Send + Sized {
    /// Records database changes to the buffer.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the buffer.
    fn record<W: FnOnce(&mut [u8])>(
        &self,
        id: TransactionID,
        journal_id: JournalID,
        len: usize,
        writer: W,
    ) -> Result<(), Error>;

    /// Flushes the content.
    ///
    /// This method is invoked when the associated journal is submitted.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the
    /// persistence layer.
    fn flush(self: Box<Self>, persistence_layer: &P) -> Result<(), Error>;
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

    /// The IO operation identifier.
    io_id: usize,

    /// The deadline of the IO operation.
    deadline: Option<Instant>,

    /// Phantom to use `S`.
    _phantom: PhantomData<S>,
}

/// [`File`] abstracts the OS file system layer to implement [`PersistenceLayer`].
///
/// [`File`] spawns a thread for blocking IO operation.
///
/// TODO: implement page cache.
/// TODO: implement checkpoint.
/// TODO: use IDL.
#[derive(Debug)]
pub struct FileIO<S: Sequencer> {
    /// The IO worker thread.
    worker: Option<JoinHandle<()>>,

    /// [`FileLogBuffer`] link.
    ///
    /// The whole link must be consumed at once otherwise it is susceptible to ABA problems.
    log_buffer_link: Arc<AtomicUsize>,

    /// The IO task sender.
    sender: SyncSender<IOTask>,

    /// This pacifies `Clippy` complaining the lack of usage of `S`.
    _phantom: PhantomData<S>,
}

/// [`FileLogBuffer`] implements [`BufferredLogger`].
#[derive(Debug, Default)]
pub struct FileLogBuffer<S: Sequencer, P: PersistenceLayer<S>> {
    /// The log sequence number.
    lsn: u64,

    /// The address of the next [`FileLogBuffer`].
    next: usize,

    /// Phantom.
    _phantom: PhantomData<(S, P)>,
}

#[derive(Debug)]
pub enum IOTask {
    /// The [`FileIO`] needs to flush log buffers.
    Flush,

    /// The [`FileIO`] is shutting down.
    Shutdown,
}

impl<'p, S: Sequencer, P: PersistenceLayer<S>> AwaitIO<'p, S, P> {
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
        if let Some(result) = self.persistence_layer.check(self.io_id, cx.waker()) {
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

impl<S: Sequencer> FileIO<S> {
    #[allow(clippy::needless_pass_by_value)]
    fn process(
        _log0: File,
        _log1: File,
        _checkpoint: File,
        receiver: Receiver<IOTask>,
        log_buffer_link: &AtomicUsize,
    ) {
        // Insert the log buffer head to force the log sequence number ever increasing.
        let mut log_buffer_head = FileLogBuffer::default();
        let log_buffer_head_ptr: *mut FileLogBuffer<S, Self> = addr_of_mut!(log_buffer_head);
        let mut current_head = log_buffer_link.load(Acquire);
        loop {
            let current_head_ptr = current_head as *const FileLogBuffer<S, Self>;

            // SAFETY: `log_buffer_link` only stores valid `FileLogBuffer` addresses.
            unsafe {
                // The log sequence number is determined by the previously buffered log.
                log_buffer_head.lsn = if current_head_ptr.is_null() {
                    0
                } else {
                    (*current_head_ptr).lsn + 1
                };
            }
            log_buffer_head.next = current_head;
            if let Err(actual) = log_buffer_link.compare_exchange(
                current_head,
                log_buffer_head_ptr as usize,
                AcqRel,
                Acquire,
            ) {
                current_head = actual;
            } else {
                break;
            }
        }

        while let Ok(task) = receiver.recv() {
            if matches!(task, IOTask::Shutdown) {
                break;
            }
        }
    }
}

impl<S: Sequencer> Default for FileIO<S> {
    /// Creates a default [`FileIO`].
    ///
    /// The default log and checkpoint files are set to `0.log`, `1.log`, and `c.dat` in the
    /// current working directory.
    ///
    /// # Panics
    ///
    /// Panics if `0.log`, `1.log`, or `c.dat` could not be opened.
    #[inline]
    fn default() -> Self {
        let log0 = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open("0.log")
            .expect("0.log could not be opened");

        let log1 = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open("1.log")
            .expect("1.log could not be opened");
        let checkpoint = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open("c.dat")
            .expect("c.dat could not be opened");
        let log_buffer_link = Arc::new(AtomicUsize::default());
        let log_buffer_link_clone = log_buffer_link.clone();
        let (sender, receiver) = mpsc::sync_channel::<IOTask>(utils::advise_num_shards() * 4);
        FileIO {
            worker: Some(thread::spawn(move || {
                Self::process(log0, log1, checkpoint, receiver, &log_buffer_link_clone);
            })),
            log_buffer_link,
            sender,
            _phantom: PhantomData,
        }
    }
}

impl<S: Sequencer> Drop for FileIO<S> {
    #[inline]
    fn drop(&mut self) {
        loop {
            match self.sender.try_send(IOTask::Shutdown) {
                Ok(_) | Err(TrySendError::Disconnected(_)) => break,
                _ => (),
            }
        }
        if let Some(worker) = self.worker.take() {
            drop(worker.join());
        }
    }
}

impl<S: Sequencer> PersistenceLayer<S> for FileIO<S> {
    type LogBuffer = FileLogBuffer<S, FileIO<S>>;

    #[inline]
    fn recover(
        &self,
        _database: &mut Database<S, Self>,
        _until: Option<<S as Sequencer>::Instant>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO {
            persistence_layer: self,
            io_id: 0,
            deadline,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn backup(
        &self,
        _database: &Database<S, Self>,
        _catalog_only: bool,
        _path: Option<&str>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO {
            persistence_layer: self,
            io_id: 0,
            deadline,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn checkpoint(
        &self,
        _database: &Database<S, Self>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO {
            persistence_layer: self,
            io_id: 0,
            deadline,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn participate(
        &self,
        _id: TransactionID,
        _xid: &[u8],
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO {
            persistence_layer: self,
            io_id: 0,
            deadline,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn submit(
        &self,
        _id: TransactionID,
        _journal_id: JournalID,
        _transaction_instant: usize,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO {
            persistence_layer: self,
            io_id: 0,
            deadline,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn rewind(
        &self,
        _id: TransactionID,
        _transaction_instant: u32,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO {
            persistence_layer: self,
            io_id: 0,
            deadline,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn prepare(
        &self,
        _id: TransactionID,
        _prepare_instant: <S as Sequencer>::Instant,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO {
            persistence_layer: self,
            io_id: 0,
            deadline,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn commit(
        &self,
        _id: TransactionID,
        _commit_instant: <S as Sequencer>::Instant,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO {
            persistence_layer: self,
            io_id: 0,
            deadline,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn check(&self, _io_id: usize, _waker: &Waker) -> Option<Result<S::Instant, Error>> {
        Some(Ok(S::Instant::default()))
    }
}

impl<S: Sequencer> BufferredLogger<S, FileIO<S>> for FileLogBuffer<S, FileIO<S>> {
    #[inline]
    fn record<W: FnOnce(&mut [u8])>(
        &self,
        _id: TransactionID,
        _journal_id: JournalID,
        _len: usize,
        _writer: W,
    ) -> Result<(), Error> {
        Ok(())
    }

    #[inline]
    fn flush(self: Box<Self>, persistence_layer: &FileIO<S>) -> Result<(), Error> {
        let self_ptr = Box::into_raw(self);
        let mut current_head = persistence_layer.log_buffer_link.load(Acquire);
        loop {
            let current_head_ptr = current_head as *const Self;

            // SAFETY: `self` was just unboxed.
            unsafe {
                // The log sequence number is determined by the previously buffered log.
                (*self_ptr).lsn = if current_head_ptr.is_null() {
                    0
                } else {
                    (*current_head_ptr).lsn + 1
                };
                (*self_ptr).next = current_head;
            }
            if let Err(actual) = persistence_layer.log_buffer_link.compare_exchange(
                current_head,
                self_ptr as usize,
                AcqRel,
                Acquire,
            ) {
                current_head = actual;
            } else {
                break;
            }
        }
        drop(persistence_layer.sender.try_send(IOTask::Flush));
        Ok(())
    }
}
