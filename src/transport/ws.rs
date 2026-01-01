//! WebSocket transport glue layer

use crate::runtime::ingress::Ingress;
use crate::session::{next_session_id, CloseReason, Session, SessionError, SessionMetadata, SessionPermissions, TransportKind};
use crate::transport::config::TransportConfig;
use bytes::Bytes;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{protocol::Message, Error as WsError};
use tokio_tungstenite::WebSocketStream;
use tokio::time::sleep;

/// Errors produced by transport handlers
#[derive(Debug)]
pub enum TransportError {
    WebSocket(WsError),
    Session(SessionError),
    Ingress(String),
    FrameTooLarge(usize),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WebSocket(err) => write!(f, "websocket error: {}", err),
            Self::Session(err) => write!(f, "session error: {}", err),
            Self::Ingress(err) => write!(f, "ingress error: {}", err),
            Self::FrameTooLarge(size) => write!(f, "frame too large: {} bytes", size),
        }
    }
}

impl std::error::Error for TransportError {}

impl From<WsError> for TransportError {
    fn from(err: WsError) -> Self {
        Self::WebSocket(err)
    }
}

impl From<SessionError> for TransportError {
    fn from(err: SessionError) -> Self {
        Self::Session(err)
    }
}

/// Run a WebSocket connection through the ingress boundary
pub async fn handle_websocket_connection(
    mut stream: WebSocketStream<TcpStream>,
    ingress: Arc<dyn Ingress>,
    config: TransportConfig,
    permissions: SessionPermissions,
) -> Result<(), TransportError> {
    let session_id = next_session_id();
    let peer_addr = stream.get_ref().peer_addr().ok();

    let mut session = Session::new(
        session_id,
        TransportKind::WebSocket,
        peer_addr,
        permissions,
        SessionMetadata::new(),
        config.channel_capacity,
        None,
    );

    ingress
        .on_open(session.info())
        .await
        .map_err(TransportError::Ingress)?;

    while let Some(msg) = stream.next().await {
        match msg? {
            Message::Binary(payload) => {
                if payload.len() > config.max_frame_size {
                    let reason = format!("frame size {} exceeds max {}", payload.len(), config.max_frame_size);
                    ingress
                        .on_close(session_id, CloseReason::Error(reason.clone()))
                        .await;
                    return Err(TransportError::FrameTooLarge(payload.len()));
                }
                let frame = Bytes::from(payload);
                loop {
                    match session.on_frame(frame.clone(), ingress.as_ref()).await {
                        Ok(()) => break,
                        Err(SessionError::Backpressure(_channel)) => {
                            sleep(config.backpressure_timeout).await;
                            continue;
                        }
                        Err(err) => {
                            let reason = close_reason_from_session(&err);
                            ingress.on_close(session_id, reason).await;
                            return Err(TransportError::Session(err));
                        }
                    }
                }
            }
            Message::Close(frame) => {
                let _reason = frame
                    .map(|f| f.reason.to_string())
                    .unwrap_or_else(|| "client_close".to_string());
                ingress
                    .on_close(session_id, CloseReason::ClientClose)
                    .await;
                return Ok(());
            }
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Text(_) => continue,
            _ => continue,
        }
    }

    ingress
        .on_close(session_id, CloseReason::ClientClose)
        .await;
    Ok(())
}

fn close_reason_from_session(err: &SessionError) -> CloseReason {
    match err {
        SessionError::IngressClose(reason) => CloseReason::Error(reason.clone()),
        _ => CloseReason::Error(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    // TODO: add WebSocket transport tests when sockets are simulated
}
