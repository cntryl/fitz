//! Connection management

use crate::error::{FitzError, Result};
use crate::transport::{AnyTransport, Transport};

pub struct FitzConnection {
    transport: AnyTransport,
}

impl FitzConnection {
    /// Connect via TCP
    pub fn connect_tcp(host: &str, port: u16) -> Result<Self> {
        let transport = AnyTransport::Tcp(
            crate::transport::tcp::TcpTransport::connect(host, port)
                .map_err(|e| FitzError::Connection(e.to_string()))?,
        );
        Ok(Self { transport })
    }

    /// Connect via WebSocket
    pub fn connect_ws(url: &str) -> Result<Self> {
        let transport = AnyTransport::WebSocket(
            crate::transport::websocket::WebSocketTransport::connect(url)
                .map_err(|e| FitzError::Connection(e.to_string()))?,
        );
        Ok(Self { transport })
    }

    pub fn send_frame(&mut self, frame: &[u8]) -> Result<()> {
        self.transport
            .send_frame(frame)
            .map_err(|e| FitzError::Transport(e.to_string()))
    }

    pub fn recv_frame(&mut self) -> Result<Vec<u8>> {
        self.transport
            .recv_frame()
            .map_err(|e| FitzError::Transport(e.to_string()))
    }

    pub fn close(&mut self) -> Result<()> {
        self.transport
            .close()
            .map_err(|e| FitzError::Transport(e.to_string()))
    }
}
