// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Page IO task processor.

#![allow(dead_code)]

use super::{FileIOData, RandomAccessFile, Sequencer};
use std::sync::mpsc::Receiver;
use std::sync::Arc;

/// Types of IO related tasks.
#[derive(Debug)]
pub(super) enum PageIOTask {
    /// The [`FileIO`](super::FileIO) needs to flush dirty pages.
    Flush,

    /// The [`FileIO`](super::FileIO) is shutting down.
    Shutdown,
}

/// Processes IO tasks.
///
/// Synchronous calls are made in the function, therefore database workers must not invoke it.
pub(super) fn process_sync<S: Sequencer<Instant = u64>>(
    receiver: &mut Receiver<PageIOTask>,
    file_io_data: &Arc<FileIOData<S>>,
) {
    let _: &RandomAccessFile = &file_io_data.db;

    while let Ok(task) = receiver.recv() {
        match task {
            PageIOTask::Flush => (),
            PageIOTask::Shutdown => {
                break;
            }
        }
    }
}
