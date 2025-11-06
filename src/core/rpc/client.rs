use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

use crate::core::engine::EngineHandle;
use tokio_stream::wrappers::ReceiverStream;
type InboxMsg = (
    String,
    Option<String>,
    Vec<u8>,
    Option<String>,
    Option<u32>,
    bool,
);

/// A minimal in-process RPC client helper using reply-queue pattern.
/// It manages a cryptographically secure inbox subscription (inbox://{uuid})
/// and offers a streaming call API where server responses must include TAG_SEQ for ordering.
#[derive(Clone)]
pub struct RpcClient {
    engine: EngineHandle,
    pub reply_route: String,
    sub_id: Arc<Mutex<Option<u64>>>,
    inbox_rx: Arc<Mutex<mpsc::Receiver<InboxMsg>>>,
}

impl RpcClient {
    /// Create a new RPC client with a cryptographically secure inbox route.
    /// The inbox route uses the format: inbox://{uuid-v4}
    pub async fn new(
        engine: EngineHandle,
        channel_id: u32,
    ) -> Result<Self, String> {
        let reply_route = format!("inbox://{}", Uuid::new_v4());
        let (tx, rx) = mpsc::channel::<InboxMsg>(128);
        let sub_id = engine
            .subscribe(reply_route.clone(), tx, channel_id)
            .await?;
        Ok(Self {
            engine,
            reply_route,
            sub_id: Arc::new(Mutex::new(Some(sub_id))),
            inbox_rx: Arc::new(Mutex::new(rx)),
        })
    }

    /// Unsubscribe the reply route; optional cleanup
    pub async fn close(&self) {
        if let Some(id) = self.sub_id.lock().await.take() {
            let _ = self.engine.unsubscribe(id).await;
        }
    }

    /// Publish an RPC request with TAG_ROUTE_REPLY and return a receiver stream for ordered responses
    /// with matching TAG_ID, ordered by TAG_SEQ ascending. Caller should consume until their own
    /// completion condition is met.
    pub async fn call_stream(
        &self,
        route: &str,
        cid: &str,
        body: &[u8],
    ) -> Result<ReceiverStream<Vec<u8>>, String> {
        // Publish request with TAG_ROUTE_REPLY; note engine.publish handles store+notify, but the
        // transport-side TLV build is client responsibility outside engine. For the in-process helper
        // we just call engine.publish with reply_to metadata so subscribers get it.
        self.engine
            .publish(
                route.to_string(),
                cid.to_string(),
                body.to_vec(),
                Some(self.reply_route.clone()),
                None,
                false,
                None,
            )
            .await?;

        // Create a filtered stream that yields bodies for our cid ordered by seq
        let rx = self.inbox_rx.clone();
        let cid_owned = cid.to_string();
        let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>(64);
        tokio::spawn(async move {
            // Pre-allocate for typical streaming scenarios (usually <16 out-of-order chunks)
            let mut pending: HashMap<u32, Vec<u8>> = HashMap::with_capacity(8);
            let mut next_seq: u32 = 1;
            let mut rx_guard = rx.lock().await;
            while let Some((_route, maybe_id, body, _reply_to, seq_opt, end)) =
                rx_guard.recv().await
            {
                // Fast path: skip non-matching correlation IDs
                if maybe_id.as_deref() != Some(&cid_owned) {
                    continue;
                }
                let seq = seq_opt.unwrap_or(0);
                pending.insert(seq, body);
                // Flush in order
                while let Some(b) = pending.remove(&next_seq) {
                    if out_tx.send(b).await.is_err() {
                        return;
                    }
                    next_seq = next_seq.saturating_add(1);
                }
                if end {
                    break;
                }
            }
        });
        Ok(ReceiverStream::new(out_rx))
    }

    /// Unary helper: publishes and awaits the first response body matching the cid on the reply route.
    /// Does not enforce end-of-stream; it's suitable for single-response RPCs.
    pub async fn call_unary(&self, route: &str, cid: &str, body: &[u8]) -> Result<Vec<u8>, String> {
        let mut stream = self.call_stream(route, cid, body).await?;
        use tokio_stream::StreamExt;
        match stream.next().await {
            Some(chunk) => Ok(chunk),
            None => Err("no response".to_string()),
        }
    }
}

/// Worker helpers for publishing replies back to a client reply route while preserving cid and optional seq.
pub struct RpcWorker {
    engine: EngineHandle,
}

impl RpcWorker {
    pub fn new(engine: EngineHandle) -> Self {
        Self { engine }
    }

    pub async fn publish_reply(
        &self,
        reply_route: String,
        cid: String,
        body: Vec<u8>,
    ) -> Result<(), String> {
        // For replies we do not set seq or end; caller may use publish_reply_seq or publish_reply_end when streaming
        self.engine
            .publish(reply_route, cid, body, None, None, false, None)
            .await
    }

    pub async fn publish_reply_seq(
        &self,
        reply_route: String,
        cid: String,
        seq: u32,
        body: Vec<u8>,
        end: bool,
    ) -> Result<(), String> {
        self.engine
            .publish(reply_route, cid, body, None, Some(seq), end, None)
            .await
    }
}
