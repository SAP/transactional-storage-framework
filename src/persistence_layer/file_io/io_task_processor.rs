// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! IO task processor.

use super::{FileIOData, FileLogBuffer, LogRecord, RandomAccessFile};
use crate::Sequencer;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::Arc;
use std::thread::{self, yield_now};
use std::{ptr::addr_of_mut, sync::mpsc::Receiver};

/// Types of IO related tasks.
#[derive(Debug)]
pub(super) enum IOTask<S: Sequencer> {
    /// The [`FileIO`](super::FileIO) needs to flush log buffers.
    Flush,

    /// The [`FileIO`](super::FileIO) needs to recover the database.
    Recover(Option<S::Instant>),

    /// The [`FileIO`](super::FileIO) is shutting down.
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
    let mut log_buffer_head =
        FileLogBuffer::<S>::from_log_record(LogRecord::Committed(0, S::Instant::default()));
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
                    let _log_record = LogRecord::<S>::from_raw_data(&buffer);
                }
            }
            IOTask::Shutdown => {
                break;
            }
        }
        if let Some(mut log_buffer) =
            take_log_buffer_link(&file_io_data.log_buffer_link, &mut log_buffer_head)
        {
            let mut buffer: [u8; 32] = [0; 32];
            let mut bytes_written = 0;
            loop {
                if let Some(n) = log_buffer.log_record.write(&mut buffer[bytes_written..]) {
                    bytes_written += n;
                } else {
                    if file_io_data
                        .log0
                        .write(&buffer[0..bytes_written], log0_offset)
                        .is_err()
                    {
                        // Retry after yielding.
                        yield_now();
                        continue;
                    }
                    log0_offset += bytes_written as u64;
                    bytes_written = 0;
                    continue;
                }
                if let Some(next_log_buffer) = log_buffer.take_next_if_not(log_buffer_head_addr) {
                    log_buffer = next_log_buffer;
                } else {
                    while file_io_data
                        .log0
                        .write(&buffer[0..bytes_written], log0_offset)
                        .is_err()
                    {
                        // Retry after yielding.
                        yield_now();
                    }
                    debug_assert!(last_log0_lsn < log_buffer.lsn);
                    log0_offset += bytes_written as u64;
                    last_log0_lsn = log_buffer.lsn;
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
fn take_log_buffer_link<S: Sequencer>(
    log_buffer_link: &AtomicUsize,
    log_buffer_head: &mut FileLogBuffer<S>,
) -> Option<Box<FileLogBuffer<S>>> {
    let mut current_head = log_buffer_link.load(Acquire);
    let log_buffer_head_addr = addr_of_mut!(*log_buffer_head) as usize;
    while current_head != 0 && current_head != log_buffer_head_addr {
        let current_head_ptr = current_head as *mut FileLogBuffer<S>;
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
