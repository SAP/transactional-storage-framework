// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::database::Kernel;
use super::utils;
use super::{AccessController, PersistenceLayer, Sequencer};
use scc::ebr;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering::Acquire;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::task::Waker;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// [`TaskProcessor`] receives tasks from database system workers, and processes them in the
/// background.
///
/// [`TaskProcessor`] is the only one that is allowed to execute blocking code as the code is run
/// in a separate background thread.
#[derive(Debug)]
pub struct TaskProcessor {
    /// The task processor thread.
    processor: Option<JoinHandle<()>>,

    /// The task sender.
    sender: SyncSender<Task>,
}

/// [`Task`] is sent to a [`TaskProcessor`] by database workers, and the [`TaskProcessor`] makes
/// sure that all the received tasks are correctly processed eventually.
#[derive(Debug)]
pub enum Task {
    /// The [`Database`](super::Database) is shutting down.
    Shutdown,

    /// The [`Waker`] should be invoked at around the specified instant.
    ///
    /// [`TaskProcessor`] makes its best to invoke the [`Waker`] at the specified instant, however
    /// there are cases where [`TaskProcessor`] fails to fulfill the requirement.
    /// * The [`Waker`] may be called before the instant if memory allocation failed in the
    /// [`TaskProcessor`].
    /// * The [`Waker`] may be called after the instant if the [`TaskProcessor`] is overloaded.
    WakeUp(Instant, Waker),

    /// The [`TaskProcessor`] should monitor the database object.
    ///
    /// [`TaskProcessor`] periodically checks the associated [`AccessController`] if the
    /// corresponding access data can be cleaned up, or ownership of it can be transferred.
    Monitor(usize),

    /// The [`TaskProcessor`] should check the [`AccessController`] entries corresponding to
    /// monitored database objects.
    ///
    /// This task is sent to the [`TaskProcessor`] when a transaction is releasing some database
    /// resources.
    ScanAccessController,
}

/// The default interval that a [`TaskProcessor`] wakes up and checks the status of the database.
const DEFAULT_CHECK_INTERAL: Duration = Duration::from_secs(60);

impl TaskProcessor {
    /// Spawns an [`TaskProcessor`].
    #[inline]
    pub(super) fn spawn<S: Sequencer, P: PersistenceLayer<S>>(
        kernel: Arc<Kernel<S, P>>,
    ) -> TaskProcessor {
        let (sender, receiver) = mpsc::sync_channel::<Task>(utils::advise_num_shards() * 4);
        TaskProcessor {
            processor: Some(thread::spawn(move || {
                Self::process(&receiver, &kernel);
            })),
            sender,
        }
    }

    /// Tries to send a [`Task`] to the [`TaskProcessor`].
    ///
    /// Returns `false` if the [`Task`] could not be sent. It is usually not a problem since it
    /// means that the send buffer is full, and therefore the worker thread is guaranteed to run
    /// afterwards., However certain types of tasks that requires the [`TaskProcessor`] to take a
    /// very specific action, e.g., [`Task::Monitor`], will need to be sent to the
    /// [`TaskProcessor`] eventually by the caller.
    pub(super) fn send_task(&self, task: Task) -> bool {
        self.sender.try_send(task).is_ok()
    }

    /// Processes tasks.
    fn process<S: Sequencer, P: PersistenceLayer<S>>(
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
                            // Try `4` times, and give up inserting the `Waker`.
                            for offset in 0..4 {
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

            // The monitored database objects and deadlines have to be always checked on every
            // iteration as sending `ScanAccessController` tasks to `Overseer` may fail when the
            // send buffer is full.
            Self::scan_access_controller(
                kernel.access_controller(),
                &mut monitored_database_object_ids,
            );
            wait_duration = Self::wake_up(&mut waker_queue);

            // TODO: implement it correctly.
            if let Some(container) = kernel.container("TODO", &ebr::Barrier::new()) {
                let oldest = kernel.sequencer().min(Acquire);
                let versioned_record_iter = container.iter_versioned_records();
                for object_id in versioned_record_iter {
                    kernel.access_controller().try_remove_access_data_sync(
                        object_id,
                        &|i| *i <= oldest,
                        &mut |_| (),
                    );
                }
            }
        }
    }

    /// Scans the access controller and cleans up access control information associated with
    /// monitored database objects.
    fn scan_access_controller<S: Sequencer>(
        access_controller: &AccessController<S>,
        monitored_object_ids: &mut BTreeSet<usize>,
    ) {
        monitored_object_ids
            .retain(|object_id| access_controller.transfer_ownership_sync(*object_id));
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

impl Drop for TaskProcessor {
    #[inline]
    fn drop(&mut self) {
        if let Some(worker) = self.processor.take() {
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

    struct AfterNSecs<'d>(Instant, u64, &'d TaskProcessor);

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
    async fn wake_up() {
        let database = Database::default();
        let now = Instant::now();
        let after1sec = AfterNSecs(now, 1, database.task_processor());
        let after2secs = AfterNSecs(now, 2, database.task_processor());
        after1sec.await;
        assert!(now.elapsed() > Duration::from_millis(128));
        after2secs.await;
        assert!(now.elapsed() > Duration::from_millis(256));
    }
}
