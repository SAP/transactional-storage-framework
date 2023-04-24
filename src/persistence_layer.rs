// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::utils;
use super::{Database, Error, JournalID, Sequencer, TransactionID};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{create_dir_all, File, OpenOptions};
use std::future::Future;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::mem::{size_of, MaybeUninit};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr::{addr_of, addr_of_mut};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
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
    fn recover<'d, 'p>(
        &'p self,
        database: &'d mut Database<S, Self>,
        until: Option<S::Instant>,
        deadline: Option<Instant>,
    ) -> Result<AwaitRecovery<'d, 'p, S, Self>, Error>;

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

    /// Checks if the IO operation associated with the log sequence number was completed.
    ///
    /// If the IO operation is still in progress, the supplied [`Waker`] is kept in the
    /// [`PersistenceLayer`] and notifies it when the operation is completed.
    ///
    /// It returns the latest known logical instant value of the database.
    fn check_io_completion(&self, lsn: u64, waker: &Waker) -> Option<Result<S::Instant, Error>>;

    /// Reads a log record during recovery.
    ///
    /// Returns `None` if there are no more log records to read.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if reading data from files failed.
    fn read_log_record(&self, waker: &Waker) -> Result<Option<Box<[u8]>>, Error>;
}

/// [`BufferredLogger`] keeps log records until an explicit call to [`BufferredLogger::flush`] is
/// made.
pub trait BufferredLogger<S: Sequencer, P: PersistenceLayer<S>>: Debug + Send + Sized {
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
    /// This method is invoked when the associated journal is submitted.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the content of the log record could not be passed to the
    /// persistence layer.
    fn flush(
        self,
        persistence_layer: &P,
        submit_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, P>, Error>;

    /// Expresses itself as a `u8` slice.
    fn as_u8_slice(&self) -> &[u8];
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
pub struct AwaitRecovery<'d, 'p, S: Sequencer, P: PersistenceLayer<S>> {
    /// The database to recover.
    #[allow(dead_code)]
    database: &'d mut Database<S, P>,

    /// The persistence layer from which the data is read.
    persistence_layer: &'p P,

    /// The logical instant associated with the recovered database snapshot.
    recovered: S::Instant,

    /// The deadline of the IO operation.
    deadline: Option<Instant>,
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

    /// Shared data among the worker and database threads.
    ///
    /// TODO: need sharding of `FileIOSharedData`.
    shared_data: Arc<FileIOSharedData>,

    /// The IO task sender.
    sender: SyncSender<IOTask<S>>,

    /// This pacifies `Clippy` complaining the lack of usage of `S`.
    _phantom: PhantomData<S>,
}

/// [`FileLogBuffer`] implements [`BufferredLogger`].
#[derive(Debug, Default)]
pub struct FileLogBuffer {
    /// Log buffer content.
    content: Vec<MaybeUninit<u8>>,

    /// The log sequence number.
    lsn: u64,

    /// The address of the next [`FileLogBuffer`].
    next: usize,
}

#[derive(Debug)]
pub enum IOTask<S: Sequencer> {
    /// The [`FileIO`] needs to flush log buffers.
    Flush,

    /// The [`FileIO`] needs to recover the database.
    Recover(Option<S::Instant>),

    /// The [`FileIO`] is shutting down.
    Shutdown,
}

/// [`FileIOSharedData`] is shared among the worker and database threads.
#[derive(Debug, Default)]
struct FileIOSharedData {
    /// [`FileLogBuffer`] link.
    ///
    /// The whole link must be consumed at once otherwise it is susceptible to ABA problems.
    log_buffer_link: AtomicUsize,

    /// The log sequence number of the last flushed log buffer.
    last_flushed_lsn: AtomicU64,

    /// Log sequence number and [`Waker`] map.
    waker_map: Mutex<BTreeMap<u64, Waker>>,
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

impl<'d, 'p, S: Sequencer, P: PersistenceLayer<S>> Future for AwaitRecovery<'d, 'p, S, P> {
    type Output = Result<S::Instant, Error>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.persistence_layer.read_log_record(cx.waker()) {
            Ok(Some(_log_record)) => {
                // TODO: implement it.
                if self
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
            Ok(None) => Poll::Ready(Ok(self.recovered)),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl<S: Sequencer> FileIO<S> {
    /// Creates a default [`FileIO`].
    ///
    /// The default log and checkpoint files are set to `0.log`, `1.log`, `0.dat`, `1.dat` in the
    /// specified [`Path`].
    ///
    /// # Errors
    ///
    /// Returns an error if memory allocation failed, spawning a thread failed, the specified
    /// directory could not be created, or database files could not be opened.
    #[inline]
    pub fn with_path(path: &Path) -> Result<Self, Error> {
        if create_dir_all(path).is_err() {
            return Err(Error::Generic("the path could not be created"));
        }

        let mut path_buffer = PathBuf::with_capacity(path.as_os_str().len() + 6);
        path_buffer.push(path);

        let mut log0 = Self::open_file(&mut path_buffer, "0.log")?;
        let mut log1 = Self::open_file(&mut path_buffer, "1.log")?;
        let mut data0 = Self::open_file(&mut path_buffer, "0.dat")?;
        let mut data1 = Self::open_file(&mut path_buffer, "1.dat")?;
        let shared_data = Arc::new(FileIOSharedData::default());
        let shared_data_clone = shared_data.clone();
        let (sender, mut receiver) =
            mpsc::sync_channel::<IOTask<S>>(utils::advise_num_shards() * 4);
        Ok(FileIO {
            worker: Some(thread::spawn(move || {
                Self::process(
                    &mut log0,
                    &mut log1,
                    &mut data0,
                    &mut data1,
                    &mut receiver,
                    &shared_data_clone,
                );
            })),
            shared_data,
            sender,
            _phantom: PhantomData,
        })
    }

    /// Opens the specified file.
    fn open_file(path_buffer: &mut PathBuf, file_name: &'static str) -> Result<File, Error> {
        path_buffer.push(Path::new(file_name));
        let Some(file_path) = path_buffer.to_str() else {
            return Err(Error::Generic(file_name));
        };
        let Ok(file) = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(file_path) else {
                return Err(Error::Generic(file_name));
            };
        path_buffer.pop();
        Ok(file)
    }

    /// Processes IO tasks.
    fn process(
        log0: &mut File,
        _log1: &mut File,
        _data0: &mut File,
        _data1: &mut File,
        receiver: &mut Receiver<IOTask<S>>,
        shared_data: &FileIOSharedData,
    ) {
        let mut log_writer = BufWriter::new(log0);
        let mut last_flushed_lsn = 0;

        // Insert the log buffer head to force the log sequence number ever increasing.
        let mut log_buffer_head = FileLogBuffer::default();
        let log_buffer_head_addr = addr_of_mut!(log_buffer_head) as usize;
        let _: Result<usize, usize> =
            shared_data
                .log_buffer_link
                .compare_exchange(0, log_buffer_head_addr, Release, Relaxed);

        while let Ok(task) = receiver.recv() {
            match task {
                IOTask::Flush => (),
                IOTask::Recover(_until) => (),
                IOTask::Shutdown => break,
            }
            if let Some(mut log_buffer) =
                Self::take_log_buffer_link(&shared_data.log_buffer_link, &mut log_buffer_head)
            {
                let first_lsn = last_flushed_lsn;
                loop {
                    log_writer
                        .write_all(BufferredLogger::<S, Self>::as_u8_slice(&log_buffer.content))
                        .expect("write failed");
                    debug_assert!(last_flushed_lsn < log_buffer.lsn);
                    last_flushed_lsn = log_buffer.lsn;
                    if let Some(next_log_buffer) = log_buffer.take_next_if_not(log_buffer_head_addr)
                    {
                        log_buffer = next_log_buffer;
                    } else {
                        drop(log_buffer);
                        break;
                    }
                }
                log_writer.flush().expect("flush failed");
                shared_data
                    .last_flushed_lsn
                    .store(last_flushed_lsn, Release);
                if let Ok(mut waker_map) = shared_data.waker_map.lock() {
                    for lsn in first_lsn + 1..=last_flushed_lsn {
                        if let Some(waker) = waker_map.remove(&lsn) {
                            waker.wake();
                        }
                    }
                }
            }
        }
    }

    /// Pushes a [`FileLogBuffer`] into the log buffer linked list, and returns the log sequence
    /// number of it.
    fn push_log_buffer(log_buffer_link: &AtomicUsize, log_buffer_ptr: *mut FileLogBuffer) -> u64 {
        let mut current_head = log_buffer_link.load(Acquire);
        loop {
            let current_head_ptr = current_head as *const FileLogBuffer;

            // SAFETY: it assumes that the caller provided a valid pointer.
            let lsn = unsafe {
                (*log_buffer_ptr).lsn = if current_head_ptr.is_null() {
                    1
                } else {
                    (*current_head_ptr).lsn + 1
                };
                (*log_buffer_ptr).next = current_head;
                (*log_buffer_ptr).lsn
            };
            if let Err(actual) = log_buffer_link.compare_exchange(
                current_head,
                log_buffer_ptr as usize,
                AcqRel,
                Acquire,
            ) {
                current_head = actual;
            } else {
                return lsn;
            }
        }
    }

    /// Takes the specified [`FileLogBuffer`] linked list.
    fn take_log_buffer_link(
        log_buffer_link: &AtomicUsize,
        log_buffer_head: &mut FileLogBuffer,
    ) -> Option<Box<FileLogBuffer>> {
        let mut current_head = log_buffer_link.load(Acquire);
        let log_buffer_head_addr = addr_of_mut!(*log_buffer_head) as usize;
        while current_head != 0 && current_head != log_buffer_head_addr {
            let current_head_ptr = current_head as *mut FileLogBuffer;
            // Safety: `current_head_ptr` not being zero was checked.
            log_buffer_head.lsn = unsafe { (*current_head_ptr).lsn };
            if let Err(actual) = log_buffer_link.compare_exchange(
                current_head,
                log_buffer_head_addr,
                AcqRel,
                Acquire,
            ) {
                current_head = actual;
            } else {
                // Safety: the pointer was provided by `Box::into_raw`.
                let mut current_log_buffer = unsafe { Box::from_raw(current_head_ptr) };
                let mut next_log_buffer_opt =
                    current_log_buffer.take_next_if_not(log_buffer_head_addr);
                while let Some(mut next_log_buffer) = next_log_buffer_opt.take() {
                    // Invert the direction of links to make it a FIFO queue.
                    debug_assert_eq!(current_log_buffer.lsn, next_log_buffer.lsn + 1);
                    let next_after_next_log_buffer =
                        next_log_buffer.take_next_if_not(log_buffer_head_addr);
                    next_log_buffer.next = Box::into_raw(current_log_buffer) as usize;
                    if let Some(next_after_next_log_buffer) = next_after_next_log_buffer {
                        current_log_buffer = next_log_buffer;
                        next_log_buffer_opt.replace(next_after_next_log_buffer);
                    } else {
                        return Some(next_log_buffer);
                    }
                }
                return Some(current_log_buffer);
            }
        }
        None
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
    type LogBuffer = Vec<MaybeUninit<u8>>;

    #[inline]
    fn recover<'d, 'p>(
        &'p self,
        database: &'d mut Database<S, Self>,
        until: Option<<S as Sequencer>::Instant>,
        deadline: Option<Instant>,
    ) -> Result<AwaitRecovery<'d, 'p, S, Self>, Error> {
        if self.sender.try_send(IOTask::Recover(until)).is_err() {
            // `Recover` must be the first request.
            return Err(Error::UnexpectedState);
        }
        Ok(AwaitRecovery {
            database,
            persistence_layer: self,
            recovered: S::Instant::default(),
            deadline,
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
        Ok(AwaitIO::with_lsn(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn checkpoint(
        &self,
        _database: &Database<S, Self>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO::with_lsn(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn participate(
        &self,
        _id: TransactionID,
        _xid: &[u8],
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO::with_lsn(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn rewind(
        &self,
        _id: TransactionID,
        _transaction_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO::with_lsn(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn prepare(
        &self,
        _id: TransactionID,
        _prepare_instant: <S as Sequencer>::Instant,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO::with_lsn(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn commit(
        &self,
        id: TransactionID,
        commit_instant: <S as Sequencer>::Instant,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        let mut log_buffer = Vec::<MaybeUninit<u8>>::new();
        BufferredLogger::<S, Self>::record(&mut log_buffer, id, 0, size_of::<S::Instant>(), |w| {
            let bytes: *const u8 = addr_of!(commit_instant).cast::<u8>();
            for i in 0..size_of::<S::Instant>() {
                // Safety: the length of the data is checked.
                unsafe {
                    *w.get_unchecked_mut(i).as_mut_ptr() = *bytes.add(i);
                }
            }
        })?;
        log_buffer.flush(self, None, deadline)
    }

    #[inline]
    fn read_log_record(&self, _waker: &Waker) -> Result<Option<Box<[u8]>>, Error> {
        Ok(None)
    }

    #[inline]
    fn check_io_completion(&self, lsn: u64, waker: &Waker) -> Option<Result<S::Instant, Error>> {
        if self.shared_data.last_flushed_lsn.load(Acquire) >= lsn {
            Some(Ok(S::Instant::default()))
        } else if let Ok(mut waker_map) = self.shared_data.waker_map.try_lock() {
            // Push the `Waker` into the bag, and check the value again.
            waker_map.insert(lsn, waker.clone());
            if self.shared_data.last_flushed_lsn.load(Acquire) >= lsn {
                waker_map.remove(&lsn);
                Some(Ok(S::Instant::default()))
            } else {
                None
            }
        } else {
            waker.wake_by_ref();
            None
        }
    }
}

impl FileLogBuffer {
    /// Takes the next [`FileLogBuffer`] if the address if not `nil` or `0`.
    fn take_next_if_not(&mut self, nil: usize) -> Option<Box<FileLogBuffer>> {
        if self.next == 0 {
            return None;
        } else if self.next == nil {
            self.next = 0;
            return None;
        }
        let log_buffer_ptr = self.next as *mut FileLogBuffer;
        // Safety: the pointer was provided by `Box::into_raw`.
        let log_buffer = unsafe { Box::from_raw(log_buffer_ptr) };
        self.next = 0;
        Some(log_buffer)
    }
}

impl<S: Sequencer> BufferredLogger<S, FileIO<S>> for Vec<MaybeUninit<u8>> {
    #[inline]
    fn record<W: FnOnce(&mut [MaybeUninit<u8>])>(
        &mut self,
        id: TransactionID,
        journal_id: JournalID,
        len: usize,
        writer: W,
    ) -> Result<(), Error> {
        let reserved = if self.is_empty() {
            let header_len = size_of::<TransactionID>() + size_of::<JournalID>();
            self.reserve(header_len + len);

            // Write the header.
            id.to_le_bytes()
                .iter()
                .for_each(|i| self.push(MaybeUninit::new(*i)));
            journal_id
                .to_le_bytes()
                .iter()
                .for_each(|i| self.push(MaybeUninit::new(*i)));

            self.resize_with(header_len + len, MaybeUninit::uninit);
            &mut self[header_len..]
        } else {
            let prev_len = self.len();
            self.resize_with(len, MaybeUninit::uninit);
            &mut self[prev_len..]
        };
        debug_assert_eq!(reserved.len(), len);
        writer(reserved);
        Ok(())
    }

    #[inline]
    fn flush(
        self,
        persistence_layer: &FileIO<S>,
        _submit_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, FileIO<S>>, Error> {
        let mut file_log_buffer = Box::<FileLogBuffer>::default();
        file_log_buffer.content = self;
        let file_log_buffer_ptr = Box::into_raw(file_log_buffer);
        let lsn = FileIO::<S>::push_log_buffer(
            &persistence_layer.shared_data.log_buffer_link,
            file_log_buffer_ptr,
        );
        debug_assert_ne!(lsn, 0);
        drop(persistence_layer.sender.try_send(IOTask::Flush));
        Ok(AwaitIO::with_lsn(persistence_layer, lsn).set_deadline(deadline))
    }

    // TODO: generalize it, e.g., `trait Log`.
    #[inline]
    fn as_u8_slice(&self) -> &[u8] {
        let maybe_uninit_slice = self.as_slice();
        // Safety: casting to `u8` is safe.
        unsafe { &*(maybe_uninit_slice as *const [MaybeUninit<u8>] as *const [u8]) }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::AtomicCounter;
    use std::time::Duration;
    use tokio::fs::remove_dir_all;

    const TIMEOUT_UNEXPECTED: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn open_close() {
        const DIR: &str = "file_io_open_close_test";
        let path = Path::new(DIR);
        let file_io = FileIO::<AtomicCounter>::with_path(path).unwrap();
        drop(file_io);
        assert!(remove_dir_all(path).await.is_ok());
    }

    #[tokio::test]
    async fn log_buffer() {
        const DIR: &str = "file_io_log_buffer_test";
        let path = Path::new(DIR);
        let file_io = FileIO::<AtomicCounter>::with_path(path).unwrap();

        let log_buffer_1 = Vec::<MaybeUninit<u8>>::new();
        let log_buffer_2 = Vec::<MaybeUninit<u8>>::new();
        let log_buffer_3 = Vec::<MaybeUninit<u8>>::new();
        let log_buffer_4 = Vec::<MaybeUninit<u8>>::new();
        let (result_3, result_1, result_4, result_2) = match (
            log_buffer_3.flush(&file_io, None, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            log_buffer_1.flush(&file_io, None, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            log_buffer_4.flush(&file_io, None, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            log_buffer_2.flush(&file_io, None, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
        ) {
            (Ok(await_io_3), Ok(await_io_1), Ok(await_io_4), Ok(await_io_2)) => {
                futures::join!(await_io_3, await_io_1, await_io_4, await_io_2)
            }
            _ => unreachable!(),
        };

        assert!(result_1.is_ok());
        assert!(result_2.is_ok());
        assert!(result_3.is_ok());
        assert!(result_4.is_ok());

        drop(file_io);
        assert!(remove_dir_all(path).await.is_ok());
    }
}
