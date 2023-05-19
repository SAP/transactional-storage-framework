// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Log IO task processor.

use super::db_header::LogFile;
use super::log_record::LogRecord;
use super::recovery::recover_database;
use super::{FileIOData, FileLogBuffer, RandomAccessFile, Sequencer};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::thread::yield_now;

/// Types of IO related tasks.
#[derive(Debug)]
pub(super) enum LogIOTask {
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

/// Processes log IO tasks.
///
/// Synchronous calls are made in the function, therefore database workers must not invoke it.
pub(super) fn process_sync<S: Sequencer<Instant = u64>>(
    receiver: &mut Receiver<LogIOTask>,
    file_io_data: &Arc<FileIOData<S>>,
) {
    let _: &RandomAccessFile = &file_io_data.db;
    let mut log_file: &RandomAccessFile = if file_io_data.db_header.log_file == LogFile::Zero {
        &file_io_data.log0
    } else {
        &file_io_data.log1
    };
    let mut log_offset = log_file.len(Acquire);

    while let Ok(task) = receiver.recv() {
        match task {
            LogIOTask::Flush => (),
            LogIOTask::Recover => {
                recover_database(file_io_data);
                log_offset = log_file.len(Relaxed);
            }
            LogIOTask::Switch => {
                log_file = if file_io_data.db_header.log_file == LogFile::Zero {
                    &file_io_data.log0
                } else {
                    &file_io_data.log1
                };
                log_offset = log_file.len(Acquire);
            }
            LogIOTask::Truncate => loop {
                let result = if file_io_data.db_header.log_file == LogFile::Zero {
                    file_io_data.log1.set_len(0)
                } else {
                    file_io_data.log0.set_len(0)
                };
                if result.is_ok() {
                    break;
                }
                yield_now();
            },
            LogIOTask::Shutdown => {
                break;
            }
        }
        if let Some(mut log_buffer) = take_log_buffer_link(&file_io_data.log_buffer_link) {
            loop {
                if log_file
                    .write(&log_buffer.buffer[0..log_buffer.pos()], log_offset)
                    .is_err()
                {
                    yield_now();
                    continue;
                }
                log_offset += log_buffer.pos() as u64;

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
                    while log_file.write(&eoj_buffer, log_offset).is_err() {
                        yield_now();
                    }
                    log_offset += eoj_buffer.len() as u64;
                }

                if let Some(next_log_buffer) = log_buffer.take_next() {
                    log_buffer = next_log_buffer;
                } else {
                    break;
                }
            }

            while log_file.sync_all().is_err() {
                yield_now();
            }
            file_io_data.flush_count.fetch_add(1, Release);
            file_io_data.waker_bag.pop_all((), |_, w| w.wake());
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
