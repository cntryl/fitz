//! WebSocket transport: async at the edge, sync engine inside.
//!
//! - Tokio handles sockets + WebSocket framing.
//! - Engine stays 100% synchronous.
//! - Per-connection outbound queue is a Tokio MPSC sender the engine can `try_send` into.

use crate::core::engine::EnginePool;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

type ConnectionId = u64;

#[derive(Clone)]
pub struct WsTransport {
    pub addr: SocketAddr,
    pub engine: EnginePool,
}

impl WsTransport {
    pub fn new(addr: SocketAddr, engine: EnginePool) -> Self {
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
    engine_pool: EnginePool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    crate::transport::inc_active_connections();
    tracing::debug!("ws connection {conn_id} accepted from {peer}");

    // For plain WS without auth, use default route_family
    // In production, JWT should be in first frame or upgrade headers
    let route_family = "default";
    let engine = engine_pool.get_handle(route_family);

    // If you need TLS, wrap `stream` here before `accept_async`.
    let ws_stream = accept_async(stream).await?;
    let (mut ws_sink, mut ws_stream) = ws_stream.split();

    // Outbound queue for this connection (bounded for backpressure).
    // Engine will hold this and call `try_send` from its sync thread.
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Arc<Vec<u8>>>(256);

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
                // Best-effort write; clone underlying buffer for tungstenite send
                if let Err(e) = ws_sink.send(Message::Binary((*frame).clone())).await {
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

pub async fn handle_upgraded_connection(
    ws_stream: WebSocketStream<hyper::upgrade::Upgraded>,
    engine_pool: EnginePool,
    session_auth: crate::authz::SessionAuth,
    route_family: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    crate::transport::inc_active_connections();
    let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        "ws upgraded connection {conn_id} accepted for subject: {} route_family: {}",
        session_auth.subject,
        route_family
    );

    // Select engine shard based on route_family (tenant)
    let engine = engine_pool.get_handle(&route_family);

    let (mut ws_sink, mut ws_stream) = ws_stream.split();

    // Outbound queue for this connection (bounded for backpressure).
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Arc<Vec<u8>>>(256);

    // Register session and connection with engine
    engine.register_session(conn_id, session_auth);
    engine.register_connection(conn_id, outbound_tx);

    // Single task handles both reading from WS and writing outbound frames.
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
                        continue;
                    }
                    Some(Err(e)) => {
                        tracing::debug!("ws {conn_id} read error: {e}");
                        break;
                    }
                    None => {
                        break;
                    }
                }
            }

            // Outbound frames: engine → WS
            Some(frame) = outbound_rx.recv() => {
                if let Err(e) = ws_sink.send(Message::Binary((*frame).clone())).await {
                    tracing::debug!("ws {conn_id} write error: {e}");
                    break;
                }
            }
        }
    }

    tracing::debug!("ws upgraded connection {conn_id} closed");
    engine.on_disconnect(conn_id);
    crate::transport::dec_active_connections();
    Ok(())
}
