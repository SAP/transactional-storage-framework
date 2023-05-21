// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Log IO task processor.

use super::log_record::LogRecord;
use super::recovery::recover_database;
use super::Fingerprint;
use super::{FileIOData, FileLogBuffer, Sequencer};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::thread::yield_now;

/// Types of file IO related tasks.
#[derive(Debug)]
pub enum IOTask {
    /// The [`FileIO`](super::FileIO) needs to flush log buffers.
    Flush,

    /// The [`FileIO`](super::FileIO) needs to recover the database.
    Recover,

    /// The [`FileIO`](super::FileIO) is shutting down.
    Shutdown,
}

/// Processes file IO tasks.
///
/// Synchronous calls are made in the function, therefore database workers must not invoke it.
pub(super) fn process_sync<S: Sequencer<Instant = u64>>(
    receiver: &mut Receiver<IOTask>,
    file_io_data: &Arc<FileIOData<S>>,
) {
    let mut log_offset = file_io_data.log.len(Acquire);

    while let Ok(task) = receiver.recv() {
        match task {
            IOTask::Flush => {
                process_log_buffer_batch(file_io_data, &mut log_offset);
            }
            IOTask::Recover => {
                recover_database(file_io_data);
                log_offset = file_io_data.log.len(Relaxed);
            }
            IOTask::Shutdown => {
                process_log_buffer_batch(file_io_data, &mut log_offset);
                break;
            }
        }
    }
}

/// Processes a batch of log buffers.
fn process_log_buffer_batch<S: Sequencer<Instant = u64>>(
    file_io_data: &Arc<FileIOData<S>>,
    log_offset: &mut u64,
) {
    // TODO: `batch_sequence_number` is not optimal.
    let batch_sequence_number = file_io_data.batch_sequence_number.load(Relaxed);
    if let Some(mut log_buffer) =
        take_log_buffer_link(&file_io_data.log_buffer_link, batch_sequence_number + 1)
    {
        loop {
            if file_io_data
                .log
                .write(&log_buffer.buffer[0..log_buffer.pos()], *log_offset)
                .is_err()
            {
                yield_now();
                continue;
            }
            *log_offset += log_buffer.pos() as u64;

            if log_buffer.eoj_logging.load(Relaxed) {
                let mut eoj_buffer = [0_u8; 8];
                if log_buffer.submit_instant.load(Relaxed) == 0 {
                    let discard_log_record = LogRecord::<S>::BufferDiscarded;
                    discard_log_record.write(&mut eoj_buffer);
                } else {
                    let submit_log_record =
                        LogRecord::<S>::BufferSubmitted(log_buffer.submit_instant.load(Relaxed));
                    submit_log_record.write(&mut eoj_buffer);
                }
                while file_io_data.log.write(&eoj_buffer, *log_offset).is_err() {
                    yield_now();
                }
                *log_offset += eoj_buffer.len() as u64;
            }

            if let Some(next_log_buffer) = log_buffer.take_next() {
                log_buffer = next_log_buffer;
            } else {
                break;
            }
        }
        while file_io_data.log.sync_all().is_err() {
            yield_now();
        }
    }

    file_io_data
        .batch_sequence_number
        .swap(batch_sequence_number + 1, Release);
    file_io_data.waker_bag.pop_all((), |_, w| w.wake());
}

/// Takes the specified [`FileLogBuffer`] linked list.
fn take_log_buffer_link(
    log_buffer_link: &AtomicUsize,
    batch_sequence_number: u64,
) -> Option<Arc<FileLogBuffer>> {
    let current_head = log_buffer_link.swap(0, Acquire);
    if current_head != 0 {
        let current_head_ptr = current_head as *mut FileLogBuffer;
        // Safety: the pointer was provided by `Box::into_raw`.
        let mut current_log_buffer = unsafe { Arc::from_raw(current_head_ptr) };
        current_log_buffer.set_fingerprint(batch_sequence_number);
        let mut next_log_buffer_opt = current_log_buffer.take_next();
        while let Some(next_log_buffer) = next_log_buffer_opt.take() {
            // Invert the direction of links to make it a FIFO queue.
            let next_after_next_log_buffer = next_log_buffer.take_next();
            next_log_buffer
                .next
                .store(Arc::into_raw(current_log_buffer) as usize, Relaxed);
            if let Some(next_after_next_log_buffer) = next_after_next_log_buffer {
                current_log_buffer = next_log_buffer;
                next_log_buffer_opt.replace(next_after_next_log_buffer);
            } else {
                next_log_buffer.set_fingerprint(batch_sequence_number);
                return Some(next_log_buffer);
            }
        }
        return Some(current_log_buffer);
    }
    None
}
