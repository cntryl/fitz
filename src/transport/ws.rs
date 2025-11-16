//! WebSocket transport: async at the edge, sync engine inside.
//!
//! - Tokio handles sockets + WebSocket framing.
//! - Engine stays 100% synchronous.
//! - Per-connection outbound queue is a Tokio MPSC sender the engine can `try_send` into.

use crate::core::engine::EngineHandle;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

type ConnectionId = u64;

#[derive(Clone)]
pub struct WsTransport {
    pub addr: SocketAddr,
    pub engine: EngineHandle,
}

impl WsTransport {
    pub fn new(addr: SocketAddr, engine: EngineHandle) -> Self {
        Self { addr, engine }
    }

    pub async fn run(self) -> std::io::Result<()> {
        crate::transport::mark_started();
        let listener = TcpListener::bind(self.addr).await?;
        crate::transport::mark_live();
        crate::transport::mark_ready();

        tracing::info!("ws listening on {}", self.addr);

        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("ws accept error: {e}");
                    continue;
                }
            };

            let engine = self.engine.clone();
            let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);

            tokio::spawn(async move {
                if let Err(e) = handle_connection(conn_id, stream, peer, engine).await {
                    tracing::error!("ws connection {conn_id} error: {e}");
                }
            });
        }
    }
}

async fn handle_connection(
    conn_id: ConnectionId,
    stream: TcpStream,
    peer: SocketAddr,
    engine: EngineHandle,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    crate::transport::inc_active_connections();
    tracing::debug!("ws connection {conn_id} accepted from {peer}");

    // If you need TLS, wrap `stream` here before `accept_async`.
    let ws_stream = accept_async(stream).await?;
    let (mut ws_sink, mut ws_stream) = ws_stream.split();

    // Outbound queue for this connection.
    // Engine will hold this and call `try_send` from its sync thread.
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    // Register with engine (engine stores outbound_tx for this conn_id).
    engine.register_connection(conn_id, outbound_tx);

    // Single task handles both reading from WS and writing outbound frames.
    // Engine is sync; this task is the async boundary.
    loop {
        tokio::select! {
            // Inbound frames: WS → engine.on_frame(conn_id, bytes)
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Binary(bytes))) => {
                        // Synchronous handoff into engine; engine will enqueue internally.
                        engine.on_frame(conn_id, bytes);
                    }
                    Some(Ok(Message::Close(_))) => {
                        break;
                    }
                    Some(Ok(_other)) => {
                        // Ignore non-binary frames (ping/pong/text) or handle as needed.
                        continue;
                    }
                    Some(Err(e)) => {
                        tracing::debug!("ws {conn_id} read error: {e}");
                        break;
                    }
                    None => {
                        // Remote closed.
                        break;
                    }
                }
            }

            // Outbound frames: engine → WS
            Some(frame) = outbound_rx.recv() => {
                // Best-effort write; if it fails, tear down the connection.
                if let Err(e) = ws_sink.send(Message::Binary(frame)).await {
                    tracing::debug!("ws {conn_id} write error: {e}");
                    break;
                }
            }
        }
    }

    tracing::debug!("ws connection {conn_id} closed");
    engine.on_disconnect(conn_id);
    crate::transport::dec_active_connections();
    Ok(())
}
