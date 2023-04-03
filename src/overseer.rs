// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::database::Kernel;
use super::{PersistenceLayer, Sequencer};
use std::collections::BTreeMap;
use std::convert::Into;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::task::Waker;
use std::thread::{self, available_parallelism, JoinHandle};
use std::time::{Duration, Instant};

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

    /// The [`Waker`] should be called at around the specified deadline.
    ///
    /// The [`Waker`] may be called before the deadline is reached if memory allocation failed in
    /// the [`Overseer`].
    WakeUp(Instant, Waker),
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
            mpsc::sync_channel::<Task>(available_parallelism().ok().map_or(1, Into::into));
        Overseer {
            worker: Some(thread::spawn(move || {
                Self::oversee(&receiver, &kernel);
            })),
            sender,
        }
    }

    /// Returns a reference to the [`SyncSender`].
    pub(super) fn message_sender(&self) -> &SyncSender<Task> {
        &self.sender
    }

    /// Oversees the specified database kernel.
    fn oversee<S: Sequencer, P: PersistenceLayer<S>>(
        receiver: &Receiver<Task>,
        _kernel: &Kernel<S, P>,
    ) {
        let mut waker_queue: BTreeMap<Instant, Waker> = BTreeMap::default();
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
                }
            }

            wait_duration = Self::wake_up(&mut waker_queue);
        }
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
    use std::sync::mpsc::SyncSender;
    use std::task::Poll;
    use std::time::Instant;

    struct AfterNSec<'d>(Instant, Duration, &'d SyncSender<Task>);

    impl<'d> Future for AfterNSec<'d> {
        type Output = ();

        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Self::Output> {
            if self.0 + self.1 < Instant::now() {
                Poll::Ready(())
            } else {
                if self
                    .2
                    .try_send(Task::WakeUp(self.0 + self.1, cx.waker().clone()))
                    .is_err()
                {
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
        let after1sec = AfterNSec(now, Duration::from_secs(1), database.message_sender());
        let after2sec = AfterNSec(now, Duration::from_secs(2), database.message_sender());
        after1sec.await;
        assert!(now.elapsed() > Duration::from_millis(128));
        after2sec.await;
        assert!(now.elapsed() > Duration::from_millis(256));
    }
}
