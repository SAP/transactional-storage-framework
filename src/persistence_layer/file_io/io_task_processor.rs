// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! IO task processor.

use super::recovery::recover_database;
use super::{FileIOData, FileLogBuffer, RandomAccessFile};
use crate::Sequencer;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::Arc;
use std::thread::{self, yield_now};
use std::{ptr::addr_of_mut, sync::mpsc::Receiver};

/// Types of IO related tasks.
#[derive(Debug)]
pub(super) enum IOTask {
    /// The [`FileIO`](super::FileIO) needs to flush log buffers.
    Flush,

    /// The [`FileIO`](super::FileIO) needs to recover the database.
    Recover,

    /// The [`FileIO`](super::FileIO) is shutting down.
    Shutdown,
}

/// Data needed by the flusher of an IO task processor.
#[derive(Debug, Default)]
pub(super) struct FlusherData {
    /// The flusher will shut down.
    shutdown: bool,

    /// The current offset in the log file to write.
    write_offset: u64,
}

/// Processes IO tasks.
///
/// Synchronous calls are made in the function, therefore database workers must not invoke it.
pub(super) fn process_sync<S: Sequencer>(
    receiver: &mut Receiver<IOTask>,
    file_io_data: &Arc<FileIOData<S>>,
) {
    // `flusher` calls `sync_data` or `sync_all` in the background.
    let file_io_data_clone = file_io_data.clone();
    let flusher = thread::spawn(move || flush_sync(&file_io_data_clone));

    let _: &RandomAccessFile = &file_io_data.log1;
    let _: &RandomAccessFile = &file_io_data.data0;
    let _: &RandomAccessFile = &file_io_data.data1;

    let mut log0_offset = 0;

    // Insert the log buffer head to share the current log offset value with database workers.
    let mut log_buffer_head = FileLogBuffer::default();
    let log_buffer_head_addr = addr_of_mut!(log_buffer_head) as usize;
    let _: Result<usize, usize> =
        file_io_data
            .log_buffer_link
            .compare_exchange(0, log_buffer_head_addr, Release, Relaxed);

    while let Ok(task) = receiver.recv() {
        match task {
            IOTask::Flush => (),
            IOTask::Recover => {
                recover_database(file_io_data);
            }
            IOTask::Shutdown => {
                break;
            }
        }
        if let Some(mut log_buffer) =
            take_log_buffer_link(&file_io_data.log_buffer_link, &mut log_buffer_head)
        {
            loop {
                if file_io_data
                    .log0
                    .write(&log_buffer.buffer[0..log_buffer.bytes_written], log0_offset)
                    .is_err()
                {
                    // Retry after yielding.
                    yield_now();
                    continue;
                }
                debug_assert_eq!(log0_offset, log_buffer.offset);
                log0_offset += log_buffer.bytes_written as u64;
                if let Some(next_log_buffer) = log_buffer.take_next_if_not(log_buffer_head_addr) {
                    log_buffer = next_log_buffer;
                } else {
                    break;
                }
            }

            // Let the flusher know that data was written to files.
            file_io_data
                .flusher_data
                .signal(|flusher_data| flusher_data.write_offset = log0_offset);
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
fn flush_sync<S: Sequencer>(file_io_data: &Arc<FileIOData<S>>) {
    while file_io_data
        .flusher_data
        .try_lock()
        .ok()
        .map_or(true, |flusher_data| !flusher_data.shutdown)
    {
        let first_offset_to_flush = file_io_data.first_offset_to_flush.load(Relaxed);
        let mut write_offset = 0;
        file_io_data.flusher_data.wait_while(|flusher_data| {
            write_offset = flusher_data.write_offset;
            !flusher_data.shutdown && write_offset <= first_offset_to_flush
        });
        if write_offset > first_offset_to_flush && file_io_data.log0.sync_all().is_ok() {
            file_io_data
                .first_offset_to_flush
                .store(write_offset, Release);
            if let Ok(mut waker_map) = file_io_data.waker_map.lock() {
                for offset in first_offset_to_flush + 1..=write_offset {
                    if let Some(waker) = waker_map.remove(&offset) {
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
        log_buffer_head.offset =
            // Safety: `current_head_ptr` not being zero was checked.
            unsafe { (*current_head_ptr).offset + (*current_head_ptr).bytes_written as u64 };
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
                debug_assert!(current_log_buffer.offset > next_log_buffer.offset);
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
