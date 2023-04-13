// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::database::Kernel;
use super::{AccessController, PersistenceLayer, Sequencer};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Into;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::task::Waker;
use std::thread::{self, available_parallelism, JoinHandle};
use std::time::{Duration, Instant};

/// [`Overseer`] wakes up timed out tasks and deletes unreachable database objects.
///
/// [`Overseer`] is the only one that is allowed to execute blocking code as the code is run in a
/// separate background thread.
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

    /// The [`Waker`] should be called at around the specified deadline.
    ///
    /// The [`Waker`] may be called before the deadline is reached if memory allocation failed in
    /// the [`Overseer`].
    WakeUp(Instant, Waker),

    /// The [`Overseer`] should monitor the database object.
    Monitor(usize),

    /// The [`Overseer`] should scan the access controller entries corresponding to monitored
    /// database objects.
    ScanAccessController,
}

/// The default interval that an [`Overseer`] wakes up and checks the status of the database.
const DEFAULT_CHECK_INTERAL: Duration = Duration::from_secs(60);

impl Overseer {
    /// Spawns an [`Overseer`].
    #[inline]
    pub(super) fn spawn<S: Sequencer, P: PersistenceLayer<S>>(
        kernel: Arc<Kernel<S, P>>,
    ) -> Overseer {
        let (sender, receiver) =
            mpsc::sync_channel::<Task>(available_parallelism().ok().map_or(1, Into::into) * 4096);
        Overseer {
            worker: Some(thread::spawn(move || {
                Self::oversee(&receiver, &kernel);
            })),
            sender,
        }
    }

    /// Sends a [`Task`] to the [`Overseer`].
    ///
    /// Returns `false` if the [`Task`] could not be sent. It is usually not a problem since it
    /// means that the send buffer is full, and therefore the worker thread is guaranteed to run
    /// afterwards., However certain types of tasks that need very specific information, e.g.,
    /// [`Task::Monitor`] will need to be sent to the [`Overseer`] eventually.
    pub(super) fn send_task(&self, task: Task) -> bool {
        self.sender.try_send(task).is_ok()
    }

    /// Oversees the specified database kernel.
    fn oversee<S: Sequencer, P: PersistenceLayer<S>>(
        receiver: &Receiver<Task>,
        kernel: &Kernel<S, P>,
    ) {
        let mut waker_queue: BTreeMap<Instant, Waker> = BTreeMap::default();
        let mut monitored_database_object_ids: BTreeSet<usize> = BTreeSet::default();
        let mut wait_duration = DEFAULT_CHECK_INTERAL;
        loop {
            if let Ok(task) = receiver.recv_timeout(wait_duration) {
                match task {
                    Task::Shutdown => break,
                    Task::WakeUp(deadline, mut waker) => {
                        let now = Instant::now();
                        if deadline < now {
                            waker.wake();
                        } else {
                            // Try `16` times, and give up inserting the `Waker`.
                            for offset in 0..16 {
                                match waker_queue
                                    .insert(deadline + Duration::from_nanos(offset), waker)
                                {
                                    Some(other_waker) => waker = other_waker,
                                    None => break,
                                }
                            }
                        }
                    }
                    Task::Monitor(object_id) => {
                        monitored_database_object_ids.insert(object_id);
                    }
                    Task::ScanAccessController => {
                        // Do nothing.
                    }
                }
            }

            Self::scan_access_controller(
                kernel.access_controller(),
                &mut monitored_database_object_ids,
            );
            wait_duration = Self::wake_up(&mut waker_queue);
        }
    }

    /// Scans the access controller and cleans up access control information associated with
    /// monitored database objects.
    fn scan_access_controller<S: Sequencer>(
        access_controller: &AccessController<S>,
        monitored_object_ids: &mut BTreeSet<usize>,
    ) {
        monitored_object_ids.retain(|object_id| access_controller.transfer_ownership(*object_id));
    }

    /// Wakes up every expired [`Waker`], and returns the time remaining until the first [`Waker`]
    /// will be expired.
    fn wake_up(waker_queue: &mut BTreeMap<Instant, Waker>) -> Duration {
        let now = Instant::now();
        while let Some(entry) = waker_queue.first_entry() {
            if *entry.key() >= now {
                return *entry.key() - now;
            }
            entry.remove().wake();
        }
        DEFAULT_CHECK_INTERAL
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::Database;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Instant;

    struct AfterNSecs<'d>(Instant, u64, &'d Overseer);

    impl<'d> Future for AfterNSecs<'d> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.0 + Duration::from_secs(self.1) < Instant::now() {
                Poll::Ready(())
            } else {
                if !self.2.send_task(Task::WakeUp(
                    self.0 + Duration::from_secs(self.1),
                    cx.waker().clone(),
                )) {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
        }
    }

    #[tokio::test]
    async fn overseer() {
        let database = Database::default();
        let now = Instant::now();
        let after1sec = AfterNSecs(now, 1, database.overseer());
        let after2secs = AfterNSecs(now, 2, database.overseer());
        after1sec.await;
        assert!(now.elapsed() > Duration::from_millis(128));
        after2secs.await;
        assert!(now.elapsed() > Duration::from_millis(256));
    }
}
