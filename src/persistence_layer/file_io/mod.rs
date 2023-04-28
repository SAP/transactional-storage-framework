// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The module implements IO subsystem on top of the OS file system layer to function as the
//! persistence layer of a database system.

mod random_access_file;
pub use random_access_file::RandomAccessFile;

mod io_task_processor;

use crate::persistence_layer::{AwaitIO, AwaitRecovery, BufferredLogger};
use crate::transaction::ID as TransactionID;
use crate::{utils, Database, Error, PersistenceLayer, Sequencer};
use io_task_processor::{FlusherData, IOTask};
use std::collections::BTreeMap;
use std::fs::{create_dir_all, OpenOptions};
use std::io;
use std::marker::PhantomData;
use std::mem::{size_of, MaybeUninit};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::ptr::addr_of;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Waker;
use std::thread::{self, JoinHandle};
use std::time::Instant;

/// [`FileIO`] abstracts the OS file system layer to implement [`PersistenceLayer`].
///
/// [`FileIO`] spawns two threads for file operations and synchronization with the device.
///
/// TODO: implement page cache.
/// TODO: implement checkpoint.
/// TODO: use IDL.
#[derive(Debug)]
pub struct FileIO<S: Sequencer> {
    /// The IO worker thread.
    io_worker: Option<JoinHandle<()>>,

    /// Shared data among the worker and database threads.
    ///
    /// TODO: need sharding of `FileIOSharedData`.
    file_io_data: Arc<FileIOData>,

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

/// [`FileIOData`] is shared among the worker and database threads.
#[derive(Debug)]
struct FileIOData {
    /// The first log file.
    log0: RandomAccessFile,

    /// The second log file.
    log1: RandomAccessFile,

    /// The first database file.
    data0: RandomAccessFile,

    /// The second database file.
    data1: RandomAccessFile,

    /// [`FileLogBuffer`] link.
    ///
    /// The whole link must be consumed at once otherwise it is susceptible to ABA problems.
    log_buffer_link: AtomicUsize,

    /// The log sequence number of the last flushed log buffer.
    last_flushed_lsn: AtomicU64,

    /// Log sequence number and [`Waker`] map.
    waker_map: Mutex<BTreeMap<u64, Waker>>,

    /// Flusher data.
    flusher_data: utils::BinarySemaphore<FlusherData>,
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

        let log0 = Self::open_file(&mut path_buffer, "0.log")?;
        let log1 = Self::open_file(&mut path_buffer, "1.log")?;
        let data0 = Self::open_file(&mut path_buffer, "0.dat")?;
        let data1 = Self::open_file(&mut path_buffer, "1.dat")?;
        let file_io_data = Arc::new(FileIOData {
            log0,
            log1,
            data0,
            data1,
            log_buffer_link: AtomicUsize::new(0),
            last_flushed_lsn: AtomicU64::new(0),
            waker_map: Mutex::default(),
            flusher_data: utils::BinarySemaphore::default(),
        });
        let file_io_data_clone = file_io_data.clone();
        let (sender, mut receiver) =
            mpsc::sync_channel::<IOTask<S>>(utils::advise_num_shards() * 4);
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
        let Ok(file) = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(file_path) else {
                return Err(Error::IO(io::ErrorKind::NotFound));
            };
        path_buffer.pop();
        Ok(RandomAccessFile::from_file(file))
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
        if let Some(worker) = self.io_worker.take() {
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
        if self.file_io_data.last_flushed_lsn.load(Acquire) >= lsn {
            Some(Ok(S::Instant::default()))
        } else if let Ok(mut waker_map) = self.file_io_data.waker_map.try_lock() {
            // Push the `Waker` into the bag, and check the value again.
            waker_map.insert(lsn, waker.clone());
            if self.file_io_data.last_flushed_lsn.load(Acquire) >= lsn {
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
