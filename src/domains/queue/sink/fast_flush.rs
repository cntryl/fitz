//! Bounded background execution for Queue's best-effort fast flushes.

use crate::domains::queue::actor::recovery_store::QueueStore;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use std::panic::AssertUnwindSafe;
use std::thread::JoinHandle;

const FAST_FLUSH_QUEUE_CAPACITY: usize = 1;

pub(super) type FlushResult = Result<bool, String>;

struct FlushRequest {
    family_id: u32,
    reply: Sender<FlushResult>,
}

enum WorkerMessage {
    Flush(FlushRequest),
    Barrier(Sender<()>),
}

#[derive(Clone)]
pub(super) struct FastFlushClient {
    sender: Sender<WorkerMessage>,
}

pub(super) struct FastFlushWorker {
    sender: Option<Sender<WorkerMessage>>,
    thread: Option<JoinHandle<()>>,
}

impl FastFlushWorker {
    pub(super) fn spawn(store: QueueStore) -> std::io::Result<(Self, FastFlushClient)> {
        Self::spawn_with(move |family_id| {
            store
                .flush_family(family_id)
                .map_err(|error| error.to_string())
        })
    }

    pub(super) fn spawn_with(
        flush: impl Fn(u32) -> FlushResult + Send + 'static,
    ) -> std::io::Result<(Self, FastFlushClient)> {
        let (sender, receiver) = crossbeam_channel::bounded(FAST_FLUSH_QUEUE_CAPACITY);
        let thread = std::thread::Builder::new()
            .name("fitz-queue-fast-flush".to_string())
            .spawn(move || Self::run(&receiver, flush))?;
        Ok((
            Self {
                sender: Some(sender.clone()),
                thread: Some(thread),
            },
            FastFlushClient { sender },
        ))
    }

    fn run(receiver: &Receiver<WorkerMessage>, flush: impl Fn(u32) -> FlushResult) {
        while let Ok(message) = receiver.recv() {
            match message {
                WorkerMessage::Flush(request) => {
                    let result =
                        std::panic::catch_unwind(AssertUnwindSafe(|| flush(request.family_id)))
                            .unwrap_or_else(
                                |_| Err("Queue fast flush worker panicked".to_string()),
                            );
                    let _ = request.reply.send(result);
                }
                WorkerMessage::Barrier(reply) => {
                    let _ = reply.send(());
                }
            }
        }
    }

    pub(super) fn wait_until_idle(&self) -> Result<(), &'static str> {
        let Some(sender) = self.sender.as_ref() else {
            return Err("Queue fast flush worker is already stopping");
        };
        let (reply, receiver) = crossbeam_channel::bounded(1);
        sender
            .send(WorkerMessage::Barrier(reply))
            .map_err(|_| "Queue fast flush worker stopped before drain")?;
        receiver
            .recv()
            .map_err(|_| "Queue fast flush worker dropped drain completion")
    }
}

impl Drop for FastFlushWorker {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl FastFlushClient {
    pub(super) fn try_flush(&self, family_id: u32) -> Result<Receiver<FlushResult>, bool> {
        let (reply, receiver) = crossbeam_channel::bounded(1);
        match self
            .sender
            .try_send(WorkerMessage::Flush(FlushRequest { family_id, reply }))
        {
            Ok(()) => Ok(receiver),
            Err(TrySendError::Full(_)) => Err(true),
            Err(TrySendError::Disconnected(_)) => Err(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    #[test]
    fn should_enqueue_flush_without_waiting_for_worker_completion() {
        // Arrange
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let first_flush = AtomicBool::new(true);
        let (worker, client) = FastFlushWorker::spawn_with(move |_| {
            if first_flush.swap(false, Ordering::AcqRel) {
                started_tx.send(()).expect("notify blocked flush start");
                release_rx.recv().expect("release blocked flush");
            }
            Ok(true)
        })
        .expect("spawn test flush worker");

        // Act
        let reply = client.try_flush(1).expect("enqueue first flush");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker starts first flush");
        let started = Instant::now();
        let second = client.try_flush(2);
        let third = client.try_flush(3);
        let enqueue_elapsed = started.elapsed();
        release_tx.send(()).expect("release first flush");
        let first_result = reply.recv_timeout(Duration::from_secs(1));
        let second_result = second.and_then(|receiver| {
            receiver
                .recv_timeout(Duration::from_secs(1))
                .map_err(|_| false)
        });

        // Assert
        assert!(enqueue_elapsed < Duration::from_millis(100));
        assert!(matches!(third, Err(true)), "bounded queue reports full");
        assert_eq!(first_result.expect("first flush result"), Ok(true));
        assert_eq!(second_result.expect("second flush result"), Ok(true));
        drop(client);
        drop(worker);
    }

    #[test]
    fn should_report_flush_failure_without_stopping_worker() {
        // Arrange
        let (worker, client) = FastFlushWorker::spawn_with(|family_id| {
            if family_id == 1 {
                Err("injected failure".to_string())
            } else {
                Ok(true)
            }
        })
        .expect("spawn test flush worker");

        // Act
        let failed = client
            .try_flush(1)
            .expect("enqueue failing flush")
            .recv_timeout(Duration::from_secs(1))
            .expect("receive failed flush result");
        let succeeded = client
            .try_flush(2)
            .expect("enqueue following flush")
            .recv_timeout(Duration::from_secs(1))
            .expect("receive following flush result");

        // Assert
        assert_eq!(failed, Err("injected failure".to_string()));
        assert_eq!(succeeded, Ok(true));
        drop(client);
        drop(worker);
    }

    #[test]
    fn should_wait_for_submitted_flush_before_idle_barrier_completes() {
        // Arrange
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::channel();
        let (worker, client) = FastFlushWorker::spawn_with(move |_| {
            started_tx.send(()).expect("notify blocked flush start");
            release_rx.recv().expect("release blocked flush");
            Ok(true)
        })
        .expect("spawn test flush worker");
        let reply = client.try_flush(1).expect("enqueue blocking flush");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker starts blocking flush");
        let worker = std::sync::Arc::new(worker);
        let worker_for_drain = worker.clone();
        let (drain_started_tx, drain_started_rx) = mpsc::channel();
        let (drain_result_tx, drain_result_rx) = mpsc::channel();
        let drain = std::thread::spawn(move || {
            drain_started_tx.send(()).expect("notify drain start");
            drain_result_tx
                .send(worker_for_drain.wait_until_idle())
                .expect("send drain result");
        });
        drain_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("drain thread started");

        // Act
        let result_while_flush_is_blocked = drain_result_rx.try_recv();
        release_tx.send(()).expect("release blocking flush");
        let completed_flush = reply.recv_timeout(Duration::from_secs(1));
        let completed_drain = drain_result_rx.recv_timeout(Duration::from_secs(1));

        // Assert
        assert!(matches!(
            result_while_flush_is_blocked,
            Err(mpsc::TryRecvError::Empty)
        ));
        assert_eq!(completed_flush.expect("flush result"), Ok(true));
        assert_eq!(completed_drain.expect("idle barrier result"), Ok(()));
        drain.join().expect("join drain thread");
        drop(client);
        drop(worker);
    }
}
