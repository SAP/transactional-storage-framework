// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! IO task processor.

use super::{AwaitIO, FileIO, FileIOData, FileLogBuffer, RandomAccessFile};
use crate::journal::ID as JournalID;
use crate::persistence_layer::BufferredLogger;
use crate::transaction::ID as TransactionID;
use crate::{Error, Sequencer};
use std::mem::{size_of, MaybeUninit};
use std::num::NonZeroU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::Arc;
use std::thread::{self, yield_now};
use std::time::Instant;
use std::{ptr::addr_of_mut, sync::mpsc::Receiver};

/// Types of IO related tasks.
#[derive(Debug)]
pub(super) enum IOTask<S: Sequencer> {
    /// The [`FileIO`] needs to flush log buffers.
    Flush,

    /// The [`FileIO`] needs to recover the database.
    Recover(Option<S::Instant>),

    /// The [`FileIO`] is shutting down.
    Shutdown,
}

/// Data needed by the flusher of an IO task processor.
#[derive(Debug, Default)]
pub(super) struct FlusherData {
    /// The flusher will shut down.
    shutdown: bool,

    /// Last written LSN.
    last_written_lsn: u64,
}

/// Processes IO tasks.
///
/// Synchronous calls are made in the function, therefore database workers must not invoke it.
pub(super) fn process_sync<S: Sequencer>(
    receiver: &mut Receiver<IOTask<S>>,
    file_io_data: &Arc<FileIOData>,
) {
    // `flusher` calls `sync_data` or `sync_all` in the background.
    let file_io_data_clone = file_io_data.clone();
    let flusher = thread::spawn(move || flush_sync(&file_io_data_clone));

    let _: &RandomAccessFile = &file_io_data.log1;
    let _: &RandomAccessFile = &file_io_data.data0;
    let _: &RandomAccessFile = &file_io_data.data1;

    let mut log0_offset = 0;
    let mut last_log0_lsn = 0;

    // Insert the log buffer head to force the log sequence number ever increasing.
    let mut log_buffer_head = FileLogBuffer::default();
    let log_buffer_head_addr = addr_of_mut!(log_buffer_head) as usize;
    let _: Result<usize, usize> =
        file_io_data
            .log_buffer_link
            .compare_exchange(0, log_buffer_head_addr, Release, Relaxed);

    while let Ok(task) = receiver.recv() {
        match task {
            IOTask::Flush => (),
            IOTask::Recover(_until) => {
                let mut read_offset = 0;
                let mut buffer: [u8; 32] = [0; 32];
                while file_io_data.log0.read(&mut buffer, read_offset).is_ok() {
                    read_offset += 32;
                }
            }
            IOTask::Shutdown => {
                break;
            }
        }
        if let Some(mut log_buffer) =
            take_log_buffer_link(&file_io_data.log_buffer_link, &mut log_buffer_head)
        {
            loop {
                let data = BufferredLogger::<S, FileIO<S>>::as_u8_slice(&log_buffer.content);
                if file_io_data.log0.write(data, log0_offset).is_err() {
                    // Retry after yielding.
                    yield_now();
                    continue;
                }
                log0_offset += data.len() as u64;
                debug_assert!(last_log0_lsn < log_buffer.lsn);
                last_log0_lsn = log_buffer.lsn;
                if let Some(next_log_buffer) = log_buffer.take_next_if_not(log_buffer_head_addr) {
                    log_buffer = next_log_buffer;
                } else {
                    drop(log_buffer);
                    break;
                }
            }

            // Let the flusher know that data was written to files.
            file_io_data
                .flusher_data
                .signal(|flusher_data| flusher_data.last_written_lsn = last_log0_lsn);
        }
    }

    // Let the flusher know that the IO task processor is shutting down.
    file_io_data
        .flusher_data
        .signal(|flusher_data| flusher_data.shutdown = true);

    drop(flusher.join());
}

/// Flushes dirty pages and updates metadata of the file.
///
/// Synchronous calls are made in the function, therefore database workers must not invoke it.
fn flush_sync(file_io_data: &Arc<FileIOData>) {
    while file_io_data
        .flusher_data
        .try_lock()
        .ok()
        .map_or(true, |flusher_data| !flusher_data.shutdown)
    {
        let last_flushed_lsn = file_io_data.last_flushed_lsn.load(Relaxed);
        let mut last_written_lsn = last_flushed_lsn;
        file_io_data.flusher_data.wait_while(|flusher_data| {
            last_written_lsn = flusher_data.last_written_lsn;
            !flusher_data.shutdown && last_written_lsn <= last_flushed_lsn
        });
        if last_written_lsn > last_flushed_lsn && file_io_data.log0.sync_all().is_ok() {
            file_io_data
                .last_flushed_lsn
                .store(last_written_lsn, Release);
            if let Ok(mut waker_map) = file_io_data.waker_map.lock() {
                for lsn in last_flushed_lsn + 1..=last_written_lsn {
                    if let Some(waker) = waker_map.remove(&lsn) {
                        waker.wake();
                    }
                }
            }
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
        if let Err(actual) =
            log_buffer_link.compare_exchange(current_head, log_buffer_head_addr, AcqRel, Acquire)
        {
            current_head = actual;
        } else {
            // Safety: the pointer was provided by `Box::into_raw`.
            let mut current_log_buffer = unsafe { Box::from_raw(current_head_ptr) };
            let mut next_log_buffer_opt = current_log_buffer.take_next_if_not(log_buffer_head_addr);
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
            &persistence_layer.file_io_data.log_buffer_link,
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
