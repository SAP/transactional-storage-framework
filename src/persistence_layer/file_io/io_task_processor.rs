// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! IO task processor.

use super::db_header::{DatabaseHeader, LogFile};
use super::log_record::LogRecord;
use super::recovery::recover_database;
use super::{FileIOData, FileLogBuffer, RandomAccessFile, Sequencer};
use std::mem::swap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::thread::yield_now;

/// Types of IO related tasks.
#[derive(Debug)]
pub(super) enum IOTask {
    /// The [`FileIO`](super::FileIO) needs to flush log buffers.
    Flush,

    /// The [`FileIO`](super::FileIO) needs to recover the database.
    Recover,

    /// The [`FileIO`](super::FileIO) needs to switch log files.
    #[allow(dead_code)]
    Switch,

    /// The [`FileIO`](super::FileIO) needs to truncate the old log file.
    #[allow(dead_code)]
    Truncate,

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
    let header = DatabaseHeader::from_file(&file_io_data.db);
    assert_eq!(header.log_file, LogFile::Zero);

    let mut log0_offset = 0;

    while let Ok(task) = receiver.recv() {
        match task {
            IOTask::Flush => (),
            IOTask::Recover => {
                recover_database(file_io_data);
            }
            IOTask::Switch => todo!(),
            IOTask::Truncate => todo!(),
            IOTask::Shutdown => {
                break;
            }
        }
        if let Some(mut log_buffer) = take_log_buffer_link(&file_io_data.log_buffer_link) {
            loop {
                if file_io_data
                    .log0
                    .write(&log_buffer.buffer[0..log_buffer.pos()], log0_offset)
                    .is_err()
                {
                    yield_now();
                    continue;
                }
                log0_offset += log_buffer.pos() as u64;

                if log_buffer.eoj_logging {
                    let mut eoj_buffer = [0_u8; 8];
                    if log_buffer.submit_instant == 0 {
                        let discard_log_record = LogRecord::<S>::BufferDiscarded;
                        discard_log_record.write(&mut eoj_buffer);
                    } else {
                        let submit_log_record =
                            LogRecord::<S>::BufferSubmitted(log_buffer.submit_instant);
                        submit_log_record.write(&mut eoj_buffer);
                    }
                    while file_io_data.log0.write(&eoj_buffer, log0_offset).is_err() {
                        yield_now();
                    }
                    log0_offset += eoj_buffer.len() as u64;
                }

                if let Some(next_log_buffer) = log_buffer.take_next() {
                    log_buffer = next_log_buffer;
                } else {
                    break;
                }
            }

            while file_io_data.log0.sync_all().is_err() {
                yield_now();
            }
            let flush_count = file_io_data.flush_count.fetch_add(1, Release) + 1;
            if let Ok(mut waker_map) = file_io_data.waker_map.lock() {
                let mut new_waker_map = waker_map.split_off(&flush_count);
                swap(&mut *waker_map, &mut new_waker_map);
                for b in new_waker_map.into_values() {
                    while let Some(w) = b.pop() {
                        w.wake();
                    }
                }
            }
        }
    }
}

/// Takes the specified [`FileLogBuffer`] linked list.
fn take_log_buffer_link(log_buffer_link: &AtomicUsize) -> Option<Box<FileLogBuffer>> {
    let current_head = log_buffer_link.swap(0, Acquire);
    if current_head != 0 {
        let current_head_ptr = current_head as *mut FileLogBuffer;
        // Safety: the pointer was provided by `Box::into_raw`.
        let mut current_log_buffer = unsafe { Box::from_raw(current_head_ptr) };
        let mut next_log_buffer_opt = current_log_buffer.take_next();
        while let Some(mut next_log_buffer) = next_log_buffer_opt.take() {
            // Invert the direction of links to make it a FIFO queue.
            let next_after_next_log_buffer = next_log_buffer.take_next();
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
    None
}
