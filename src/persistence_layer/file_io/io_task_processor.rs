// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! IO task processor.

use super::recovery::recover_database;
use super::{FileIOData, FileLogBuffer, RandomAccessFile, Sequencer};
use std::mem::swap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::Arc;
use std::task::Waker;
use std::thread::yield_now;
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

/// Processes IO tasks.
///
/// Synchronous calls are made in the function, therefore database workers must not invoke it.
pub(super) fn process_sync<S: Sequencer<Instant = u64>>(
    receiver: &mut Receiver<IOTask>,
    file_io_data: &Arc<FileIOData<S>>,
) {
    let _: &RandomAccessFile = &file_io_data.log1;
    let _: &RandomAccessFile = &file_io_data.db;

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
                    .write(&log_buffer.buffer[0..log_buffer.pos()], log0_offset)
                    .is_err()
                {
                    yield_now();
                    continue;
                }

                debug_assert_eq!(log0_offset, log_buffer.offset);
                log0_offset += log_buffer.pos() as u64;

                if log_buffer.eoj_buffer_used {
                    while file_io_data
                        .log0
                        .write(&log_buffer.eoj_buffer, log0_offset)
                        .is_err()
                    {
                        yield_now();
                    }
                    log0_offset += log_buffer.eoj_buffer.len() as u64;
                }

                if let Some(next_log_buffer) = log_buffer.take_next_if_not(log_buffer_head_addr) {
                    log_buffer = next_log_buffer;
                } else {
                    break;
                }
            }

            while file_io_data.log0.sync_all().is_err() {
                yield_now();
            }
            file_io_data
                .next_offset_to_flush
                .store(log0_offset, Release);
            if let Ok(mut waker_map) = file_io_data.waker_map.lock() {
                let mut new_waker_map = waker_map.split_off(&(log0_offset + 1));
                swap(&mut *waker_map, &mut new_waker_map);
                new_waker_map.into_values().for_each(Waker::wake);
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
            unsafe { (*current_head_ptr).offset + (*current_head_ptr).len() as u64 };
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
