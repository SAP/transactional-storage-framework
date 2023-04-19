// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::database::Kernel;
use super::utils;
use super::{PersistenceLayer, Sequencer};
use scc::ebr;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::take;
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

    /// The [`TaskProcessor`] should monitor the database container.
    ///
    /// TODO: implement it.
    #[allow(dead_code)]
    MonitorContainer(String),

    /// The [`TaskProcessor`] should monitor the database object.
    ///
    /// [`TaskProcessor`] periodically checks the associated [`AccessController`] if the
    /// corresponding access data can be cleaned up, or ownership of it can be transferred.
    MonitorObject(usize),

    /// The [`TaskProcessor`] should check the [`AccessController`] entries corresponding to
    /// monitored database objects.
    ///
    /// This task is sent to the [`TaskProcessor`] when a transaction is releasing some database
    /// resources.
    ScanAccessController,
}

/// The default interval that a [`TaskProcessor`] wakes up and checks the status of the database.
const DEFAULT_CHECK_INTERAL: Duration = Duration::from_secs(60);

/// [`TaskProcessor`] processes time critical tasks on every `CONTEXT_SWITCH_THRESHOLD` operations
/// in a long task.
const CONTEXT_SWITCH_THRESHOLD: usize = 256;

/// [`ThreadLocalData`] is privately used by [`TaskProcessor`].
#[derive(Debug)]
struct ThreadLocalData<S: Sequencer, P: PersistenceLayer<S>> {
    /// The [`Kernel`] of the database.
    kernel: Arc<Kernel<S, P>>,

    /// The [`Waker`] container.
    waker_queue: BTreeMap<Instant, Waker>,

    /// A set containing the names of monitored containers.
    monitored_containers: BTreeSet<String>,

    /// A set containing the object identifiers of monitored database objects.
    monitored_object_ids: BTreeSet<usize>,

    /// Wait duration to receive a new [`Task`].
    wait_duration: Duration,
}

impl TaskProcessor {
    /// Spawns an [`TaskProcessor`].
    #[inline]
    pub(super) fn spawn<S: Sequencer, P: PersistenceLayer<S>>(
        kernel: Arc<Kernel<S, P>>,
    ) -> TaskProcessor {
        let (sender, receiver) = mpsc::sync_channel::<Task>(utils::advise_num_shards() * 4);
        TaskProcessor {
            processor: Some(thread::spawn(move || {
                let mut thread_local_data = ThreadLocalData {
                    kernel,
                    waker_queue: BTreeMap::default(),
                    monitored_containers: BTreeSet::default(),
                    monitored_object_ids: BTreeSet::default(),
                    wait_duration: DEFAULT_CHECK_INTERAL,
                };
                Self::process(&receiver, &mut thread_local_data);
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
        thread_local_data: &mut ThreadLocalData<S, P>,
    ) {
        loop {
            if let Ok(task) = receiver.recv_timeout(thread_local_data.wait_duration) {
                match task {
                    Task::Shutdown => break,
                    Task::WakeUp(deadline, mut waker) => {
                        let now = Instant::now();
                        if deadline < now {
                            waker.wake();
                        } else {
                            // Try `4` times, and give up inserting the `Waker`.
                            for offset in 0..4 {
                                match thread_local_data
                                    .waker_queue
                                    .insert(deadline + Duration::from_nanos(offset), waker)
                                {
                                    Some(other_waker) => waker = other_waker,
                                    None => break,
                                }
                            }
                        }
                    }
                    Task::MonitorContainer(name) => {
                        thread_local_data.monitored_containers.insert(name);
                    }
                    Task::MonitorObject(object_id) => {
                        thread_local_data.monitored_object_ids.insert(object_id);
                    }
                    Task::ScanAccessController => {
                        // Do nothing.
                    }
                }
            }

            // The monitored database objects and deadlines have to be always checked on every
            // iteration as sending `ScanAccessController` tasks to `Overseer` may fail when the
            // send buffer is full.
            Self::process_time_critical_tasks(thread_local_data);

            // Perform MVCC garbage collection.
            let mut operation_count = 0;
            let mut monitored_containers = take(&mut thread_local_data.monitored_containers);
            monitored_containers.retain(|name| {
                if let Some(container) = thread_local_data
                    .kernel
                    .container(name.as_str(), &ebr::Barrier::new())
                {
                    let oldest = thread_local_data.kernel.sequencer().min(Acquire);
                    let versioned_record_iter = container.iter_versioned_records();
                    let mut num_versioned_records = 0;
                    for object_id in versioned_record_iter {
                        if !thread_local_data
                            .kernel
                            .access_controller()
                            .try_remove_access_data_sync(object_id, &|i| *i <= oldest, &mut |_| ())
                        {
                            num_versioned_records += 1;
                        }
                        if operation_count == CONTEXT_SWITCH_THRESHOLD {
                            // Process time critical tasks periodically.
                            Self::process_time_critical_tasks(thread_local_data);
                            operation_count = 0;
                        } else {
                            operation_count += 1;
                        }
                    }
                    return num_versioned_records != 0;
                }
                false
            });
            thread_local_data.monitored_containers = monitored_containers;
        }
    }

    /// Performs time critical tasks.
    ///
    /// It firstly scans the access controller and cleans up access control information associated
    /// with monitored database objects, then wakes up every expired [`Waker`] instances.
    fn process_time_critical_tasks<S: Sequencer, P: PersistenceLayer<S>>(
        thread_local_data: &mut ThreadLocalData<S, P>,
    ) {
        let access_controller = thread_local_data.kernel.access_controller();
        thread_local_data
            .monitored_object_ids
            .retain(|object_id| access_controller.transfer_ownership_sync(*object_id));

        let mut new_wait_duration = DEFAULT_CHECK_INTERAL;
        let now = Instant::now();
        while let Some(entry) = thread_local_data.waker_queue.first_entry() {
            if *entry.key() >= now {
                new_wait_duration = *entry.key() - now;
                break;
            }
            entry.remove().wake();
        }
        thread_local_data.wait_duration = new_wait_duration;
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
