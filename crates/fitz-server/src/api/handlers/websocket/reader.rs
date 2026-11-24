//! Keep WebSocket control traffic live while ordered domain dispatch waits.

use super::{is_normal_websocket_disconnect, monotonic_elapsed_millis};
use crate::api::buffer_budget::BufferBudget;
use crate::api::ingress::IngressConfig;
use bytes::Bytes;
use futures_util::StreamExt;
use hyper_tungstenite::tungstenite::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

pub(super) struct ReceivedFrame {
    pub(super) frame: Bytes,
    pub(super) received_at: Instant,
}

pub(super) struct ReaderContext<'a> {
    pub(super) config: &'a IngressConfig,
    pub(super) budget: BufferBudget,
    pub(super) frames: mpsc::Sender<ReceivedFrame>,
    pub(super) control: &'a mpsc::Sender<Message>,
    pub(super) last_pong: &'a Arc<AtomicU64>,
    pub(super) monotonic_origin: &'a Arc<std::time::Instant>,
    pub(super) session_id: u64,
    pub(super) sink: &'a crate::api::outbound::SessionOutboundSink,
}

pub(super) async fn read_frames<S>(
    receiver: &mut futures_util::stream::SplitStream<hyper_tungstenite::WebSocketStream<S>>,
    context: ReaderContext<'_>,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    while let Some(message) = receiver.next().await {
        match message {
            Ok(Message::Binary(frame)) => {
                let received_at = Instant::now();
                if frame.len() > context.config.max_frame_size {
                    return Err("WebSocket frame exceeds configured message limit".to_owned());
                }
                let Ok(frame) = context.budget.wrap(frame.clone()) else {
                    context
                        .sink
                        .reject_unqueued_frame(context.session_id, &frame)?;
                    continue;
                };
                // Never park the control reader behind a full data queue.
                match context
                    .frames
                    .try_send(ReceivedFrame { frame, received_at })
                {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(frame)) => context
                        .sink
                        .reject_unqueued_frame(context.session_id, &frame.frame)?,
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err("WebSocket inbound frame queue closed".to_owned())
                    }
                }
            }
            Ok(Message::Ping(payload)) => context
                .control
                .try_send(Message::Pong(payload))
                .map_err(|_| "WebSocket control queue full or closed".to_owned())?,
            Ok(Message::Pong(_)) => context.last_pong.store(
                monotonic_elapsed_millis(**context.monotonic_origin),
                Ordering::Release,
            ),
            Ok(Message::Close(_)) => return Ok(()),
            Ok(_) => {}
            Err(error) if is_normal_websocket_disconnect(&error) => return Ok(()),
            Err(error) => return Err(format!("WebSocket error: {error}")),
        }
    }
    Ok(())
}
