// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::database::Kernel;
use super::{PersistenceLayer, Sequencer};
use std::convert::Into;
use std::sync::mpsc::{self, SyncSender};
use std::sync::Arc;
use std::task::Waker;
use std::thread::{self, available_parallelism, JoinHandle};
use std::time::Instant;

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

    /// The [`Waker`] should be called at the specified deadline.
    #[allow(dead_code)]
    WakeUp(Instant, Waker),
}

impl Overseer {
    /// Spawns an [`Overseer`].
    #[inline]
    pub(super) fn spawn<S: Sequencer, P: PersistenceLayer<S>>(
        kernel: Arc<Kernel<S, P>>,
    ) -> Overseer {
        let (sender, receiver) =
            mpsc::sync_channel::<Task>(available_parallelism().ok().map_or(1, Into::into));
        Overseer {
            worker: Some(thread::spawn(move || {
                let _kernel = kernel;
                while let Ok(task) = receiver.recv() {
                    match task {
                        Task::Shutdown => break,
                        Task::WakeUp(_deadline, waker) => {
                            // TODO: wake up AFTER deadline.
                            waker.wake();
                        }
                    }
                }
            })),
            sender,
        }
    }

    /// Returns a reference to the [`SyncSender`].
    pub(super) fn sender(&self) -> &SyncSender<Task> {
        &self.sender
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
