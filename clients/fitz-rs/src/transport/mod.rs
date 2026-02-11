//! Abstract transport interface supporting TCP and WebSocket

pub mod tcp;
pub mod websocket;

use std::io;

/// Abstract transport trait - implemented by TCP and WebSocket
pub trait Transport: Send + Sync {
    /// Send a single frame (TLV payload without session framing)
    fn send_frame(&mut self, frame: &[u8]) -> io::Result<()>;

    /// Receive a single frame
    fn recv_frame(&mut self) -> io::Result<Vec<u8>>;

    /// Close the connection gracefully
    fn close(&mut self) -> io::Result<()>;
}

/// Runtime-selected transport
pub enum AnyTransport {
    Tcp(tcp::TcpTransport),
    WebSocket(Box<websocket::WebSocketTransport>),
}

impl Transport for AnyTransport {
    fn send_frame(&mut self, frame: &[u8]) -> io::Result<()> {
        match self {
            AnyTransport::Tcp(t) => t.send_frame(frame),
            AnyTransport::WebSocket(t) => t.send_frame(frame),
        }
    }

    fn recv_frame(&mut self) -> io::Result<Vec<u8>> {
        match self {
            AnyTransport::Tcp(t) => t.recv_frame(),
            AnyTransport::WebSocket(t) => t.recv_frame(),
        }
    }

    fn close(&mut self) -> io::Result<()> {
        match self {
            AnyTransport::Tcp(t) => t.close(),
            AnyTransport::WebSocket(t) => t.close(),
        }
    }
}
