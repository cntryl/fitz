//! WebSocket transport with binary message frames

use super::Transport;
use futures_util::stream::StreamExt;
use futures_util::sink::SinkExt;
use std::io;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::MaybeTlsStream;

pub struct WebSocketTransport {
    ws: tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    rt: tokio::runtime::Runtime,
}

impl WebSocketTransport {
    /// Connect to a WebSocket server
    pub fn connect(url: &str) -> io::Result<Self> {
        let rt = tokio::runtime::Runtime::new()?;

        let ws = rt.block_on(async {
            match tokio_tungstenite::connect_async(url).await {
                Ok((ws, _)) => Ok(ws),
                Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
            }
        })?;

        Ok(Self { ws, rt })
    }
}

impl Transport for WebSocketTransport {
    fn send_frame(&mut self, frame: &[u8]) -> io::Result<()> {
        self.rt.block_on(async {
            // WebSocket: send as binary message (no length prefix)
            self.ws
                .send(Message::Binary(frame.to_vec()))
                .await
                .map_err(io::Error::other)
        })
    }

    fn recv_frame(&mut self) -> io::Result<Vec<u8>> {
        self.rt.block_on(async {
            loop {
                match self.ws.next().await {
                    Some(Ok(Message::Binary(data))) => return Ok(data),
                    Some(Ok(Message::Text(_))) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Text frames not supported",
                        ))
                    }
                    Some(Ok(Message::Close(_))) => {
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            "Server closed connection",
                        ))
                    }
                    Some(Ok(_)) => continue, // Ignore ping/pong
                    Some(Err(e)) => return Err(io::Error::other(e)),
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "Connection closed",
                        ))
                    }
                }
            }
        })
    }

    fn close(&mut self) -> io::Result<()> {
        self.rt.block_on(async {
            self.ws
                .close(None)
                .await
                .map_err(io::Error::other)
        })
    }
}
