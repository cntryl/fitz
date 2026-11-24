//! One-shot gate for a real Stream COMMIT on a transport session.

use super::TestServer;
use crate::domains::stream::protocol::StreamMessage;
use crate::domains::stream::{StreamClientFrame, StreamClientRequest};
use crate::runtime::{DeliveryError, Envelope, MailboxSink};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

const COMMIT_GATE_TIMEOUT: Duration = Duration::from_secs(30);

struct BlockingStreamCommitSink {
    inner: Arc<dyn MailboxSink>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: crossbeam_channel::Receiver<()>,
}

impl MailboxSink for BlockingStreamCommitSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let is_commit = envelope
            .payload::<StreamClientRequest>()
            .is_some_and(|request| {
                request.meta.message_type == 602
                    && matches!(
                        &request.frame,
                        Ok(StreamClientFrame::Op(StreamMessage::Commit { .. }))
                    )
            });
        let entered = if is_commit {
            self.entered.lock().take()
        } else {
            None
        };
        if let Some(entered) = entered {
            let _ = entered.send(());
            self.release
                .recv_timeout(COMMIT_GATE_TIMEOUT)
                .map_err(|_| DeliveryError::Timeout)?;
        }
        self.inner.deliver(envelope)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.inner.deliver_high_priority(envelope)
    }
}

/// Releases a blocked COMMIT when explicitly released or dropped during unwinding.
#[doc(hidden)]
#[must_use = "dropping this handle releases the Stream COMMIT gate"]
pub struct BlockedStreamCommit {
    entered: Option<oneshot::Receiver<()>>,
    release: crossbeam_channel::Sender<()>,
}

impl BlockedStreamCommit {
    /// Wait until the next real Stream COMMIT reaches the forwarding gate.
    ///
    /// # Errors
    /// Returns an error if this wait was already consumed, the gate disappears,
    /// or no COMMIT arrives within the supplied timeout.
    pub async fn wait_until_blocked(&mut self, timeout: Duration) -> Result<(), String> {
        let entered = self
            .entered
            .take()
            .ok_or_else(|| "Stream COMMIT gate wait already consumed".to_string())?;
        tokio::time::timeout(timeout, entered)
            .await
            .map_err(|_| "Timed out waiting for Stream COMMIT gate".to_string())?
            .map_err(|_| "Stream COMMIT gate disappeared before entry".to_string())
    }

    /// Allow the intercepted COMMIT to continue into the original Stream sink.
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for BlockedStreamCommit {
    fn drop(&mut self) {
        // The channel holds one release, so teardown never waits for the sink.
        let _ = self.release.try_send(());
    }
}

impl TestServer {
    /// Gate one COMMIT while forwarding every request to the real Stream domain.
    ///
    /// # Errors
    /// Returns an error when this server has no registered Stream sink.
    #[doc(hidden)]
    pub fn block_next_stream_commit(&self) -> Result<BlockedStreamCommit, String> {
        let router = self.runtime.router();
        let inner = router
            .resolve_domain_sink_for_tests("stream")
            .ok_or_else(|| "Stream sink is not registered".to_string())?;
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        router.register_domain_pattern(
            "stream",
            Arc::new(BlockingStreamCommitSink {
                inner,
                entered: Mutex::new(Some(entered_tx)),
                release: release_rx,
            }),
        );
        Ok(BlockedStreamCommit {
            entered: Some(entered_rx),
            release: release_tx,
        })
    }
}
