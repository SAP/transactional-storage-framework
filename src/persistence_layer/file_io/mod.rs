// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The module implements IO subsystem on top of the OS file system layer to function as the
//! persistence layer of a database system.
//!
//! The [`FileIO`] persistence layer only supports `u64` [`Sequencer`] types.

mod db_header;
mod evictable_page;
mod io_task_processor;
mod log_record;
mod page_manager;
mod random_access_file;
mod recovery;

use super::Fingerprint;
use crate::persistence_layer::{AwaitIO, AwaitRecovery, RecoveryResult};
use crate::{utils, Database, Error, JournalID, PersistenceLayer, Sequencer, TransactionID};
use io_task_processor::IOTask;
use log_record::LogRecord;
use page_manager::PageManager;
use random_access_file::RandomAccessFile;
use recovery::RecoveryData;
use scc::Bag;
use std::fs::{create_dir_all, OpenOptions};
use std::io;
use std::marker::PhantomData;
use std::mem::take;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::thread::{self, JoinHandle};
use std::time::Instant;

/// [`FileIO`] abstracts the OS file system layer to implement [`PersistenceLayer`].
///
/// [`FileIO`] spawns a thread for file operations and synchronization with the device. Any
/// [`Sequencer`] implementations generating `u64` clock values can be used for [`FileIO`].
///
/// [`FileIO`] spawns two additional threads that are dedicated to file IO operations.
///
/// TODO: implement page cache.
/// TODO: implement checkpoint.
#[derive(Debug)]
pub struct FileIO<S: Sequencer<Instant = u64>> {
    /// The file IO worker thread.
    file_io_worker: Option<JoinHandle<()>>,

    /// The file IO task sender.
    file_io_task_sender: SyncSender<IOTask>,

    /// Shared data among the workers and database threads.
    file_io_data: Arc<FileIOData<S>>,

    /// This pacifies `Clippy` complaining the lack of usage of `S`.
    _phantom: PhantomData<S>,
}

/// [`FileLogBuffer`] is the log buffer type for [`FileIO`].
#[derive(Debug, Default)]
pub struct FileLogBuffer {
    /// The associated log record.
    buffer: [u8; 32],

    /// The number of byes written to the buffer.
    bytes_written: AtomicU8,

    /// Extended buffer to accommodate a single end-of-journal log record.
    submit_instant: AtomicU32,

    /// Flag indicating that the end-of-journal log record should be generated on-the-fly.
    eoj_logging: AtomicBool,

    /// Fingerprint.
    fingerprint: AtomicU64,

    /// The address of the next [`FileLogBuffer`].
    next: AtomicUsize,
}

/// [`FileIOData`] is shared among the worker and database threads.
#[derive(Debug)]
struct FileIOData<S: Sequencer<Instant = u64>> {
    /// The database to recover.
    recovery_data: Mutex<Option<Box<RecoveryData<S>>>>,

    /// Recovery cancelled.
    recovery_cancelled: AtomicBool,

    /// The log file.
    ///
    /// TODO: replace it with database pages.
    log: RandomAccessFile,

    /// [`FileLogBuffer`] link.
    ///
    /// The whole link must be consumed at once otherwise it is susceptible to ABA problems.
    log_buffer_link: AtomicUsize,

    /// The page manager.
    #[allow(dead_code)]
    page_manager: PageManager,

    /// Increments every time a log file is flushed.
    batch_sequence_number: AtomicU64,

    /// [`Waker`] bag.
    waker_bag: Bag<Waker>,
}

impl<S: Sequencer<Instant = u64>> FileIO<S> {
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

        let log = Self::open_file(&mut path_buffer, "l.log")?;
        let db = Self::open_file(&mut path_buffer, "db.dat")?;
        let (file_io_task_sender, mut file_io_task_receiver) =
            mpsc::sync_channel::<IOTask>(utils::advise_num_shards() * 4);
        let page_manager = PageManager::from_db(db, file_io_task_sender.clone())?;
        let file_io_data = Arc::new(FileIOData {
            recovery_data: Mutex::default(),
            recovery_cancelled: AtomicBool::new(false),
            log,
            log_buffer_link: AtomicUsize::new(0),
            page_manager,
            batch_sequence_number: AtomicU64::new(0),
            waker_bag: Bag::default(),
        });
        let file_io_data_clone = file_io_data.clone();
        Ok(FileIO {
            file_io_worker: Some(thread::spawn(move || {
                io_task_processor::process_sync(&mut file_io_task_receiver, &file_io_data_clone);
            })),
            file_io_task_sender,
            file_io_data,
            _phantom: PhantomData,
        })
    }

    /// Opens the specified file.
    fn open_file(
        path_buffer: &mut PathBuf,
        file_name: &'static str,
    ) -> Result<RandomAccessFile, Error> {
        path_buffer.push(Path::new(file_name));
        let Some(file_path) = path_buffer.to_str() else {
            return Err(Error::IO(io::ErrorKind::NotFound));
        };
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_path)
            .map_err(|e| Error::IO(e.kind()))?;
        let metadata = file.metadata().map_err(|e| Error::IO(e.kind()))?;
        path_buffer.pop();
        Ok(RandomAccessFile::from_file(file, &metadata))
    }

    /// Pushes a [`FileLogBuffer`] into the log buffer linked list.
    fn push_log_buffer(log_buffer_link: &AtomicUsize, log_buffer_ptr: *const FileLogBuffer) {
        let mut head = log_buffer_link.load(Acquire);
        loop {
            // SAFETY: it assumes that the caller provided a valid pointer.
            let log_buffer = unsafe { &*log_buffer_ptr };
            debug_assert_ne!(log_buffer.bytes_written.load(Relaxed), 0);
            log_buffer.next.store(head, Relaxed);

            // `Acquire` is needed to correctly load `batch_sequence_number` afterwards.
            if let Err(actual) =
                log_buffer_link.compare_exchange(head, log_buffer_ptr as usize, AcqRel, Acquire)
            {
                head = actual;
            } else {
                return;
            }
        }
    }

    /// Flushes a log buffer.
    fn flush(&self, log_buffer: Arc<FileLogBuffer>, deadline: Option<Instant>) -> AwaitIO<S, Self> {
        let log_buffer_clone = log_buffer.clone();
        let file_log_buffer_ptr = Arc::into_raw(log_buffer);
        Self::push_log_buffer(&self.file_io_data.log_buffer_link, file_log_buffer_ptr);
        drop(self.file_io_task_sender.try_send(IOTask::Flush));
        AwaitIO::with_log_buffer(self, log_buffer_clone, deadline)
    }
}

impl<S: Sequencer<Instant = u64>> Drop for FileIO<S> {
    #[inline]
    fn drop(&mut self) {
        loop {
            match self.file_io_task_sender.try_send(IOTask::Shutdown) {
                Ok(_) | Err(TrySendError::Disconnected(_)) => break,
                _ => (),
            }
        }
        if let Some(worker) = self.file_io_worker.take() {
            drop(worker.join());
        }
    }
}

impl<S: Sequencer<Instant = u64>> PersistenceLayer<S> for FileIO<S> {
    type LogBuffer = FileLogBuffer;

    #[inline]
    fn recover(
        &self,
        database: Database<S, Self>,
        until: Option<u64>,
        deadline: Option<Instant>,
    ) -> Result<AwaitRecovery<S, Self>, Error> {
        if let Ok(mut recovery_data) = self.file_io_data.recovery_data.lock() {
            debug_assert!(recovery_data.is_none());
            recovery_data.replace(Box::new(RecoveryData::new(database, until)));
        } else {
            // Locking unexpectedly failed.
            return Err(Error::UnexpectedState);
        }
        if self.file_io_task_sender.try_send(IOTask::Recover).is_err() {
            // `Recover` must be the first request.
            return Err(Error::UnexpectedState);
        }

        Ok(AwaitRecovery {
            persistence_layer: self,
            deadline,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn participate(
        &self,
        _id: TransactionID,
        _xid: &[u8],
        _deadline: Option<Instant>,
    ) -> AwaitIO<S, Self> {
        todo!()
    }

    #[inline]
    fn create(
        &self,
        mut log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        journal_id: JournalID,
        object_ids: &[u64],
    ) -> Result<Arc<Self::LogBuffer>, Error> {
        let mut current_log: Option<LogRecord<S>> = None;
        for id in object_ids {
            let new_log = if let Some(log) = current_log.take() {
                let new_log = match log {
                    LogRecord::JournalCreatedObjectSingle(_, _, prev_id) => {
                        if let Some(interval) = id.checked_sub(prev_id) {
                            if let Ok(interval) = u32::try_from(interval) {
                                Some(LogRecord::JournalCreatedObjectRange(
                                    transaction_id,
                                    journal_id,
                                    prev_id,
                                    interval,
                                    2,
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    LogRecord::JournalCreatedObjectRange(_, _, start_id, interval, num_objects) => {
                        if let Some(diff) = id.checked_sub(start_id) {
                            if num_objects != u32::MAX
                                && diff == u64::from(interval) * u64::from(num_objects)
                            {
                                Some(LogRecord::JournalCreatedObjectRange(
                                    transaction_id,
                                    journal_id,
                                    start_id,
                                    interval,
                                    num_objects + 1,
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                new_log.map_or_else(
                    || {
                        let bytes_written =
                            if let Some(bytes_written) = log.write(log_buffer.buffer_mut()) {
                                bytes_written
                            } else {
                                // The log buffer is full, therefore flush it.
                                self.flush(take(&mut log_buffer), None);
                                log.write(log_buffer.buffer_mut()).unwrap()
                            };
                        log_buffer.set_buffer_position(log_buffer.pos() + bytes_written);
                        LogRecord::JournalCreatedObjectSingle(transaction_id, journal_id, *id)
                    },
                    |l| l,
                )
            } else {
                LogRecord::JournalCreatedObjectSingle(transaction_id, journal_id, *id)
            };
            current_log.replace(new_log);
        }

        if let Some(log) = current_log {
            let bytes_written = if let Some(bytes_written) = log.write(log_buffer.buffer_mut()) {
                bytes_written
            } else {
                // The log buffer is full, therefore flush it.
                self.flush(take(&mut log_buffer), None);
                log.write(log_buffer.buffer_mut()).unwrap()
            };
            log_buffer.set_buffer_position(log_buffer.pos() + bytes_written);
        }

        Ok(log_buffer)
    }

    #[inline]
    fn delete(
        &self,
        mut log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        journal_id: JournalID,
        object_ids: &[u64],
    ) -> Result<Arc<Self::LogBuffer>, Error> {
        let mut current_log: Option<LogRecord<S>> = None;
        for id in object_ids {
            let new_log = if let Some(log) = current_log.take() {
                let new_log = match log {
                    LogRecord::JournalDeletedObjectSingle(_, _, prev_id) => {
                        if let Some(interval) = id.checked_sub(prev_id) {
                            if let Ok(interval) = u32::try_from(interval) {
                                Some(LogRecord::JournalDeletedObjectRange(
                                    transaction_id,
                                    journal_id,
                                    prev_id,
                                    interval,
                                    2,
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    LogRecord::JournalDeletedObjectRange(_, _, start_id, interval, num_objects) => {
                        if let Some(diff) = id.checked_sub(start_id) {
                            if num_objects != u32::MAX
                                && diff == u64::from(interval) * u64::from(num_objects)
                            {
                                Some(LogRecord::JournalDeletedObjectRange(
                                    transaction_id,
                                    journal_id,
                                    start_id,
                                    interval,
                                    num_objects + 1,
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                new_log.map_or_else(
                    || {
                        let bytes_written =
                            if let Some(bytes_written) = log.write(log_buffer.buffer_mut()) {
                                bytes_written
                            } else {
                                // The log buffer is full, therefore flush it.
                                self.flush(take(&mut log_buffer), None);
                                log.write(log_buffer.buffer_mut()).unwrap()
                            };
                        log_buffer.set_buffer_position(log_buffer.pos() + bytes_written);
                        LogRecord::JournalDeletedObjectSingle(transaction_id, journal_id, *id)
                    },
                    |l| l,
                )
            } else {
                LogRecord::JournalDeletedObjectSingle(transaction_id, journal_id, *id)
            };
            current_log.replace(new_log);
        }

        if let Some(log) = current_log {
            let bytes_written = if let Some(bytes_written) = log.write(log_buffer.buffer_mut()) {
                bytes_written
            } else {
                // The log buffer is full, therefore flush it.
                self.flush(take(&mut log_buffer), None);
                log.write(log_buffer.buffer_mut()).unwrap()
            };
            log_buffer.set_buffer_position(log_buffer.pos() + bytes_written);
        }

        Ok(log_buffer)
    }

    #[inline]
    fn submit(
        &self,
        mut log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        journal_id: JournalID,
        transaction_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    ) {
        debug_assert!(!log_buffer.eoj_logging.load(Relaxed));
        debug_assert_eq!(log_buffer.submit_instant.load(Relaxed), 0);
        if let Some(transaction_instant) = transaction_instant {
            if log_buffer.bytes_written.load(Relaxed) == 0 {
                // The buffer is empty, therefore it needs to write its identification information.
                let submit_log_record = LogRecord::<S>::JournalSubmitted(
                    transaction_id,
                    journal_id,
                    transaction_instant.get(),
                );
                let bytes_written = submit_log_record.write(log_buffer.buffer_mut()).unwrap();
                log_buffer.set_buffer_position(bytes_written);
            } else {
                log_buffer
                    .submit_instant
                    .store(transaction_instant.get(), Relaxed);
                log_buffer.eoj_logging.store(true, Relaxed);
            }
        }
        self.flush(log_buffer, deadline);
    }

    #[inline]
    fn discard(
        &self,
        mut log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        journal_id: JournalID,
        deadline: Option<Instant>,
    ) {
        debug_assert!(!log_buffer.eoj_logging.load(Relaxed));
        debug_assert_eq!(log_buffer.submit_instant.load(Relaxed), 0);
        if log_buffer.bytes_written.load(Relaxed) == 0 {
            // The buffer is empty, therefore it needs to write its identification information.
            let discard_log_record = LogRecord::<S>::JournalDiscarded(transaction_id, journal_id);
            let bytes_written = discard_log_record.write(log_buffer.buffer_mut()).unwrap();
            log_buffer.set_buffer_position(bytes_written);
        } else {
            log_buffer.eoj_logging.store(true, Relaxed);
        }
        self.flush(log_buffer, deadline);
    }

    #[inline]
    fn rewind(
        &self,
        transaction_id: TransactionID,
        transaction_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    ) {
        let mut log_buffer = Arc::<Self::LogBuffer>::default();
        let Some(new_pos) = LogRecord::<S>::TransactionRolledBack(
            transaction_id,
            transaction_instant.map_or(0, NonZeroU32::get))
            .write(log_buffer.buffer_mut()) else {
            unreachable!("logic error");
        };
        log_buffer.set_buffer_position(new_pos);
        self.flush(log_buffer, deadline);
    }

    #[inline]
    fn prepare(
        &self,
        transaction_id: TransactionID,
        prepare_instant: u64,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, Self> {
        let mut log_buffer = Arc::<Self::LogBuffer>::default();
        let Some(new_pos) = LogRecord::<S>::TransactionPrepared(transaction_id, prepare_instant)
            .write(log_buffer.buffer_mut()) else {
            unreachable!("logic error");
        };
        log_buffer.set_buffer_position(new_pos);
        self.flush(log_buffer, deadline)
    }

    #[inline]
    fn commit(
        &self,
        mut log_buffer: Arc<Self::LogBuffer>,
        transaction_id: TransactionID,
        commit_instant: u64,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, Self> {
        let Some(new_pos) = LogRecord::<S>::TransactionCommitted(transaction_id, commit_instant)
            .write(log_buffer.buffer_mut()) else {
            unreachable!("logic error");
        };
        log_buffer.set_buffer_position(new_pos);
        self.flush(log_buffer, deadline)
    }

    #[inline]
    fn check_io_completion(
        &self,
        fingerprint: Option<NonZeroU64>,
        waker: &Waker,
    ) -> Option<Result<(), Error>> {
        if let Some(fingerprint) = fingerprint {
            if self.file_io_data.batch_sequence_number.load(Acquire) >= fingerprint.get() {
                Some(Ok(()))
            } else {
                // Push the `Waker` into the bag, and check the value again.
                self.file_io_data.waker_bag.push(waker.clone());
                if self.file_io_data.batch_sequence_number.load(Relaxed) >= fingerprint.get() {
                    Some(Ok(()))
                } else {
                    None
                }
            }
        } else {
            self.file_io_data.waker_bag.push(waker.clone());
            None
        }
    }

    #[inline]
    fn check_recovery(&self, waker: &Waker) -> Result<RecoveryResult<S, Self>, Error> {
        if let Ok(mut guard) = self.file_io_data.recovery_data.try_lock() {
            if let Some(mut recovery_data) = guard.take() {
                if let Some(database) = recovery_data.get_result() {
                    let database = database?;
                    // Recovery completed.
                    return Ok(RecoveryResult::Recovered(database));
                }
                recovery_data.set_waker(waker.clone());
                guard.replace(recovery_data);
                return Ok(RecoveryResult::InProgress);
            }
        }

        // Locking failed.
        Ok(RecoveryResult::Unknown)
    }

    #[inline]
    fn cancel_recovery(&self) {
        self.file_io_data.recovery_cancelled.store(true, Release);
        if let Ok(mut guard) = self.file_io_data.recovery_data.try_lock() {
            guard.as_mut().unwrap().cancel();
        }
    }
}

impl FileLogBuffer {
    /// Returns the current buffer starting position.
    fn pos(&self) -> usize {
        self.bytes_written.load(Relaxed) as usize
    }

    /// Returns the current remaining buffer size.
    fn buffer_mut<'s>(self: &'s mut Arc<Self>) -> &'s mut [u8] {
        let self_mut = Arc::get_mut(self).unwrap();
        &mut self_mut.buffer[self_mut.bytes_written.load(Relaxed) as usize..]
    }

    /// Sets the new buffer starting position.
    fn set_buffer_position(&self, pos: usize) {
        debug_assert!(pos <= self.buffer.len());
        {
            #![allow(clippy::cast_possible_truncation)]
            self.bytes_written.store(pos as u8, Relaxed);
        }
    }

    /// Takes the next [`FileLogBuffer`].
    fn take_next(&self) -> Option<Arc<FileLogBuffer>> {
        if self.next.load(Relaxed) == 0 {
            return None;
        }
        let log_buffer_ptr = self.next.swap(0, Relaxed) as *mut FileLogBuffer;
        // Safety: the pointer was provided by `Box::into_raw`.
        let log_buffer = unsafe { Arc::from_raw(log_buffer_ptr) };
        Some(log_buffer)
    }
}

impl Fingerprint for FileLogBuffer {
    #[inline]
    fn set_fingerprint(&self, fingerprint: u64) {
        debug_assert_ne!(fingerprint, 0);
        let prev = self.fingerprint.swap(fingerprint, Relaxed);
        debug_assert_eq!(prev, 0);
    }

    #[inline]
    fn get_fingerprint(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.fingerprint.load(Acquire))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::MonotonicU64;
    use static_assertions::assert_eq_size;
    use std::time::Duration;
    use tokio::fs::remove_dir_all;

    assert_eq_size!(FileLogBuffer, ([u64; 6], usize));

    const TIMEOUT_UNEXPECTED: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn open_close() {
        const DIR: &str = "file_io_open_close_test";
        let path = Path::new(DIR);
        let file_io = FileIO::<MonotonicU64>::with_path(path).unwrap();
        drop(file_io);
        assert!(remove_dir_all(path).await.is_ok());
    }

    #[tokio::test]
    async fn log_buffer() {
        const DIR: &str = "file_io_log_buffer_test";
        let path = Path::new(DIR);
        let file_io = FileIO::<MonotonicU64>::with_path(path).unwrap();

        let mut log_buffer_1 = Arc::<FileLogBuffer>::default();
        let pos = LogRecord::<MonotonicU64>::TransactionCommitted(0, 3)
            .write(log_buffer_1.buffer_mut())
            .unwrap();
        log_buffer_1.set_buffer_position(pos);
        let mut log_buffer_2 = Arc::<FileLogBuffer>::default();
        let pos = LogRecord::<MonotonicU64>::TransactionCommitted(16, 3)
            .write(log_buffer_2.buffer_mut())
            .unwrap();
        log_buffer_2.set_buffer_position(pos);
        let mut log_buffer_3 = Arc::<FileLogBuffer>::default();
        let pos = LogRecord::<MonotonicU64>::TransactionCommitted(32, 3)
            .write(log_buffer_3.buffer_mut())
            .unwrap();
        log_buffer_3.set_buffer_position(pos);
        let mut log_buffer_4 = Arc::<FileLogBuffer>::default();
        let pos = LogRecord::<MonotonicU64>::TransactionCommitted(48, 3)
            .write(log_buffer_4.buffer_mut())
            .unwrap();
        log_buffer_4.set_buffer_position(pos);

        let (result_3, result_1, result_4, result_2) = futures::join!(
            file_io.flush(log_buffer_3, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            file_io.flush(log_buffer_1, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            file_io.flush(log_buffer_4, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            file_io.flush(log_buffer_2, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
        );

        assert!(result_1.is_ok());
        assert!(result_2.is_ok());
        assert!(result_3.is_ok());
        assert!(result_4.is_ok());

        drop(file_io);
        assert!(remove_dir_all(path).await.is_ok());
    }
}
