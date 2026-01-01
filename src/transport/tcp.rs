//! TCP transport glue layer

use crate::runtime::ingress::Ingress;
use crate::session::{next_session_id, CloseReason, Session, SessionError, SessionMetadata, SessionPermissions, TransportKind};
use crate::transport::config::TransportConfig;
use bytes::{Bytes, BytesMut, Buf};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::sleep;

/// Errors produced by the TCP transport
#[derive(Debug)]
pub enum TransportError {
    Io(std::io::Error),
    Session(SessionError),
    Ingress(String),
    FrameTooLarge(usize),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "tcp io error: {}", err),
            Self::Session(err) => write!(f, "session error: {}", err),
            Self::Ingress(err) => write!(f, "ingress error: {}", err),
            Self::FrameTooLarge(size) => write!(f, "frame too large: {} bytes", size),
        }
    }
}

impl std::error::Error for TransportError {}

impl From<std::io::Error> for TransportError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<SessionError> for TransportError {
    fn from(err: SessionError) -> Self {
        Self::Session(err)
    }
}

/// Run a TCP connection through the ingress boundary
pub async fn handle_tcp_connection(
    mut stream: TcpStream,
    ingress: Arc<dyn Ingress>,
    config: TransportConfig,
    permissions: SessionPermissions,
) -> Result<(), TransportError> {
    let session_id = next_session_id();
    let peer_addr = stream.peer_addr().ok();

    let mut session = Session::new(
        session_id,
        TransportKind::Tcp,
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

    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        let n = stream.read_buf(&mut buffer).await?;
        if n == 0 {
            break;
        }

        while buffer.len() >= 4 {
            let len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
            if len > config.max_frame_size {
                let reason = format!("frame size {} exceeds max {}", len, config.max_frame_size);
                ingress.on_close(session_id, CloseReason::Error(reason.clone())).await;
                return Err(TransportError::FrameTooLarge(len));
            }

            if buffer.len() < 4 + len {
                break;
            }

            let payload = buffer[4..4 + len].to_vec();
            buffer.advance(4 + len);
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
    // TODO: add TCP transport tests once the framing layer is mockable
}
