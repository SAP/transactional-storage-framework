// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use std::convert::Into;
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, available_parallelism, JoinHandle};

/// [`Overseer`] wakes up timed out tasks and deletes unreachable database objects.
#[derive(Debug)]
pub struct Overseer {
    /// The worker thread.
    worker: Option<JoinHandle<()>>,

    /// The task sender.
    sender: SyncSender<Task>,
}

/// [`Task`] is a means of communication between an [`Overseer`] and [`Database`](super::Database).
#[derive(Debug)]
pub enum Task {
    /// The [`Database`](super::Database) is shutting down.
    Shutdown,
}

impl Overseer {
    /// Spawns an [`Overseer`].
    #[inline]
    pub(crate) fn spawn() -> Overseer {
        let (sender, receiver) =
            mpsc::sync_channel::<Task>(available_parallelism().ok().map_or(1, Into::into));
        Overseer {
            worker: Some(thread::spawn(move || {
                if let Ok(Task::Shutdown) = receiver.recv() {
                    //
                }
            })),
            sender,
        }
    }

    /// Tries to posts a task.
    ///
    /// Returns `false` if it fails to post the task.
    #[inline]
    pub(crate) fn try_post(&self, task: Task) -> bool {
        self.sender.try_send(task).is_ok()
    }
}

impl Drop for Overseer {
    #[inline]
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            drop(worker.join());
        }
    }
}
