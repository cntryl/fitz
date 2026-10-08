//! One sleeping worker and at most one outstanding consumer pacing request.

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

pub(super) const IMPLEMENTATION: &str = "dedicated_sleep_worker_capacity_one_monotonic_deadline";

struct Request {
    deadline: Instant,
    permit: oneshot::Sender<()>,
}

pub(super) struct Pacer {
    requests: Option<Sender<Request>>,
    pending: Option<oneshot::Receiver<()>>,
    worker: Option<tokio::task::JoinHandle<()>>,
}

impl Pacer {
    pub(super) fn new() -> Self {
        let (requests, receiver) = bounded(1);
        Self {
            requests: Some(requests),
            pending: None,
            worker: Some(tokio::task::spawn_blocking(move || work(&receiver))),
        }
    }

    pub(super) async fn wait_after(
        &mut self,
        acknowledged: Instant,
        pause: Duration,
    ) -> Result<Instant, String> {
        if self.pending.is_some() {
            return Err("pacing request still outstanding after cancellation".into());
        }
        let (permit, receiver) = oneshot::channel();
        self.requests
            .as_ref()
            .ok_or("pacing worker already shut down")?
            .try_send(Request {
                deadline: acknowledged
                    .checked_add(pause)
                    .ok_or("pacing deadline overflow")?,
                permit,
            })
            .map_err(|error| error.to_string())?;
        // Retain the outstanding receiver if this future is cancelled. A later
        // wait cannot enqueue another request before cleanup.
        self.pending = Some(receiver);
        self.pending
            .as_mut()
            .ok_or("missing pacing receiver")?
            .await
            .map_err(|error| error.to_string())?;
        self.pending = None;
        Ok(Instant::now())
    }

    pub(super) async fn shutdown(&mut self) -> Result<(), String> {
        self.requests = None;
        self.pending = None;
        if let Some(worker) = self.worker.take() {
            worker.await.map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

impl Drop for Pacer {
    fn drop(&mut self) {
        // Disconnect wakes a sleeping or idle worker even if an enclosing
        // timeout cancels shutdown. Joining never blocks a broker runtime thread.
        self.requests = None;
    }
}

fn work(requests: &Receiver<Request>) {
    while let Ok(request) = requests.recv() {
        loop {
            let remaining = request.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let _ = request.permit.send(());
                break;
            }
            // A timed receive sleeps without busy waiting. Disconnection is
            // cancellation; no sender is retained by the worker itself.
            match requests.recv_timeout(remaining) {
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) | Ok(_) => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn should_release_the_worker_when_an_enclosing_future_drops_the_pacer() {
        // Arrange
        let mut pacer = super::Pacer::new();
        let worker = pacer.worker.take().unwrap();
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(5),
            pacer.wait_after(
                std::time::Instant::now(),
                std::time::Duration::from_secs(60),
            ),
        )
        .await;
        // Act
        drop(pacer);
        let stopped = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
        // Assert
        stopped.unwrap().unwrap();
    }
}
