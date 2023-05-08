// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The module implements IO subsystem on top of the OS file system layer to function as the
//! persistence layer of a database system.
//!
//! The [`FileIO`] persistence layer only supports [`Sequencer`] types with the logical instant
//! type fixed to `u64`.

mod io_task_processor;
mod log_record;
mod random_access_file;
mod recovery;

pub use random_access_file::RandomAccessFile;

use crate::persistence_layer::{AwaitIO, AwaitRecovery, BufferredLogger, RecoveryResult};
use crate::transaction::ID as TransactionID;
use crate::{utils, Database, Error, PersistenceLayer, Sequencer};
use io_task_processor::{FlusherData, IOTask};
use log_record::LogRecord;
use recovery::RecoveryData;
use std::collections::BTreeMap;
use std::fs::{create_dir_all, OpenOptions};
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::thread::{self, JoinHandle};
use std::time::Instant;

/// [`FileIO`] abstracts the OS file system layer to implement [`PersistenceLayer`].
///
/// [`FileIO`] spawns two threads for file operations and synchronization with the device. Any
/// [`Sequencer`] implementations generating `u64` clock values can be used for [`FileIO`].
///
/// TODO: implement page cache.
/// TODO: implement checkpoint.
#[derive(Debug)]
pub struct FileIO<S: Sequencer<Instant = u64>> {
    /// The IO worker thread.
    io_worker: Option<JoinHandle<()>>,

    /// Shared data among the worker and database threads.
    file_io_data: Arc<FileIOData<S>>,

    /// The IO task sender.
    sender: SyncSender<IOTask>,

    /// This pacifies `Clippy` complaining the lack of usage of `S`.
    _phantom: PhantomData<S>,
}

/// [`FileLogBuffer`] implements [`BufferredLogger`].
#[derive(Debug, Default)]
pub struct FileLogBuffer {
    /// The associated log record.
    buffer: [u8; 32],

    /// The number of byes written to the buffer.
    bytes_written: u8,

    /// The offset in the log file of the log record.
    offset: u64,

    /// The address of the next [`FileLogBuffer`].
    next: usize,
}

/// [`FileIOData`] is shared among the worker and database threads.
#[derive(Debug)]
struct FileIOData<S: Sequencer<Instant = u64>> {
    /// The database to recover.
    recovery_data: Mutex<Option<Box<RecoveryData<S>>>>,

    /// The first log file.
    log0: RandomAccessFile,

    /// The second log file.
    log1: RandomAccessFile,

    /// The database file.
    db: RandomAccessFile,

    /// [`FileLogBuffer`] link.
    ///
    /// The whole link must be consumed at once otherwise it is susceptible to ABA problems.
    log_buffer_link: AtomicUsize,

    /// The first offset in the log file that has yet to be flushed.
    first_offset_to_flush: AtomicU64,

    /// Log offset values and [`Waker`] map.
    waker_map: Mutex<BTreeMap<u64, Waker>>,

    /// Flusher data.
    flusher_data: utils::BinarySemaphore<FlusherData>,
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

        let log0 = Self::open_file(&mut path_buffer, "0.log")?;
        let log1 = Self::open_file(&mut path_buffer, "1.log")?;
        let db = Self::open_file(&mut path_buffer, "db.dat")?;
        let file_io_data = Arc::new(FileIOData {
            recovery_data: Mutex::default(),
            log0,
            log1,
            db,
            log_buffer_link: AtomicUsize::new(0),
            first_offset_to_flush: AtomicU64::new(0),
            waker_map: Mutex::default(),
            flusher_data: utils::BinarySemaphore::default(),
        });
        let file_io_data_clone = file_io_data.clone();
        let (sender, mut receiver) = mpsc::sync_channel::<IOTask>(utils::advise_num_shards() * 4);
        Ok(FileIO {
            io_worker: Some(thread::spawn(move || {
                io_task_processor::process_sync(&mut receiver, &file_io_data_clone);
            })),
            file_io_data,
            sender,
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

    /// Pushes a [`FileLogBuffer`] into the log buffer linked list, and returns the end-of-buffer
    /// offset.
    fn push_log_buffer(log_buffer_link: &AtomicUsize, log_buffer_ptr: *mut FileLogBuffer) -> u64 {
        let mut head = log_buffer_link.load(Acquire);
        loop {
            let head_ptr = head as *const FileLogBuffer;

            // SAFETY: it assumes that the caller provided a valid pointer.
            let log_buffer = unsafe { &mut (*log_buffer_ptr) };
            debug_assert_ne!(log_buffer.bytes_written, 0);

            // SAFETY: the value of the pointer is properly checked.
            log_buffer.offset = if let Some(head) = unsafe { head_ptr.as_ref() } {
                // The offset of the last previously pushed log buffer is the starting offset
                // of the supplied log buffer.
                head.offset + u64::from(head.bytes_written)
            } else {
                // It is the first lob buffer.
                0
            };
            log_buffer.next = head;
            if let Err(actual) =
                log_buffer_link.compare_exchange(head, log_buffer_ptr as usize, AcqRel, Acquire)
            {
                head = actual;
            } else {
                return log_buffer.offset + u64::from(log_buffer.bytes_written);
            }
        }
    }
}

impl<S: Sequencer<Instant = u64>> Drop for FileIO<S> {
    #[inline]
    fn drop(&mut self) {
        loop {
            match self.sender.try_send(IOTask::Shutdown) {
                Ok(_) | Err(TrySendError::Disconnected(_)) => break,
                _ => (),
            }
        }
        if let Some(worker) = self.io_worker.take() {
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
        if self.sender.try_send(IOTask::Recover).is_err() {
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
    fn backup(
        &self,
        _database: &Database<S, Self>,
        _catalog_only: bool,
        _path: Option<&str>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO::with_eob_offset(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn checkpoint(
        &self,
        _database: &Database<S, Self>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO::with_eob_offset(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn participate(
        &self,
        _id: TransactionID,
        _xid: &[u8],
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO::with_eob_offset(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn rewind(
        &self,
        _id: TransactionID,
        _transaction_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        Ok(AwaitIO::with_eob_offset(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn prepare(
        &self,
        _id: TransactionID,
        _prepare_instant: u64,
        deadline: Option<Instant>,
    ) -> Result<AwaitIO<S, Self>, Error> {
        // TODO: implement it.
        Ok(AwaitIO::with_eob_offset(self, 0).set_deadline(deadline))
    }

    #[inline]
    fn commit(
        &self,
        mut log_buffer: Box<Self::LogBuffer>,
        id: TransactionID,
        commit_instant: u64,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, Self> {
        let Some(new_pos) = LogRecord::<S>::Committed(id, commit_instant).write(&mut log_buffer.buffer) else {
            unreachable!("logic error");
        };
        log_buffer.set_buffer_position(new_pos);
        log_buffer.flush(self, None, deadline)
    }

    #[inline]
    fn check_io_completion(&self, offset: u64, waker: &Waker) -> Option<Result<u64, Error>> {
        if self.file_io_data.first_offset_to_flush.load(Acquire) >= offset {
            Some(Ok(u64::default()))
        } else if let Ok(mut waker_map) = self.file_io_data.waker_map.try_lock() {
            // Push the `Waker` into the bag, and check the value again.
            waker_map.insert(offset, waker.clone());
            if self.file_io_data.first_offset_to_flush.load(Acquire) >= offset {
                waker_map.remove(&offset);
                Some(Ok(u64::default()))
            } else {
                None
            }
        } else {
            waker.wake_by_ref();
            None
        }
    }

    #[inline]
    fn check_recovery(&self, waker: &Waker) -> Result<RecoveryResult<S, Self>, Error> {
        if let Ok(mut guard) = self.file_io_data.recovery_data.try_lock() {
            if let Some(mut recovery_data) = guard.take() {
                if let Some(result) = recovery_data.get_result() {
                    // Recovery completed.
                    result?;
                    return Ok(RecoveryResult::Recovered(recovery_data.take()));
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
        if let Ok(mut guard) = self.file_io_data.recovery_data.try_lock() {
            guard.as_mut().unwrap().cancel();
        }
    }
}

impl FileLogBuffer {
    /// Returns the current buffer starting position.
    fn pos(&self) -> usize {
        self.bytes_written as usize
    }

    /// Sets the new buffer starting position.
    fn set_buffer_position(&mut self, pos: usize) {
        debug_assert!(pos <= self.buffer.len());
        {
            #![allow(clippy::cast_possible_truncation)]
            self.bytes_written = pos as u8;
        }
    }

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

impl<S: Sequencer<Instant = u64>> BufferredLogger<S, FileIO<S>> for FileLogBuffer {
    #[inline]
    fn flush(
        self: Box<Self>,
        persistence_layer: &FileIO<S>,
        _submit_instant: Option<NonZeroU32>,
        deadline: Option<Instant>,
    ) -> AwaitIO<S, FileIO<S>> {
        if self.bytes_written == 0 {
            // It is an empty log buffer.
            return AwaitIO::with_eob_offset(persistence_layer, 0);
        }
        let file_log_buffer_ptr = Box::into_raw(self);
        let eob_offset = FileIO::<S>::push_log_buffer(
            &persistence_layer.file_io_data.log_buffer_link,
            file_log_buffer_ptr,
        );
        drop(persistence_layer.sender.try_send(IOTask::Flush));
        AwaitIO::with_eob_offset(persistence_layer, eob_offset).set_deadline(deadline)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::MonotonicU64;
    use std::time::Duration;
    use tokio::fs::remove_dir_all;

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

        let mut log_buffer_1 = Box::<FileLogBuffer>::default();
        let pos = LogRecord::<MonotonicU64>::Committed(0, 3)
            .write(&mut log_buffer_1.buffer)
            .unwrap();
        log_buffer_1.set_buffer_position(pos);
        let mut log_buffer_2 = Box::<FileLogBuffer>::default();
        let pos = LogRecord::<MonotonicU64>::Committed(16, 3)
            .write(&mut log_buffer_2.buffer)
            .unwrap();
        log_buffer_2.set_buffer_position(pos);
        let mut log_buffer_3 = Box::<FileLogBuffer>::default();
        let pos = LogRecord::<MonotonicU64>::Committed(32, 3)
            .write(&mut log_buffer_3.buffer)
            .unwrap();
        log_buffer_3.set_buffer_position(pos);
        let mut log_buffer_4 = Box::<FileLogBuffer>::default();
        let pos = LogRecord::<MonotonicU64>::Committed(48, 3)
            .write(&mut log_buffer_4.buffer)
            .unwrap();
        log_buffer_4.set_buffer_position(pos);

        let (result_3, result_1, result_4, result_2) = futures::join!(
            log_buffer_3.flush(&file_io, None, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            log_buffer_1.flush(&file_io, None, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            log_buffer_4.flush(&file_io, None, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
            log_buffer_2.flush(&file_io, None, Some(Instant::now() + TIMEOUT_UNEXPECTED)),
        );

        assert!(result_1.is_ok());
        assert!(result_2.is_ok());
        assert!(result_3.is_ok());
        assert!(result_4.is_ok());

        drop(file_io);
        assert!(remove_dir_all(path).await.is_ok());
    }
}
