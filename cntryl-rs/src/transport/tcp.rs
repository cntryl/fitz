//! TCP transport with length-prefixed frames

use super::Transport;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub struct TcpTransport {
    stream: TcpStream,
    read_buf: Vec<u8>,
}

impl TcpTransport {
    /// Connect to a TCP server
    pub fn connect(host: &str, port: u16) -> std::io::Result<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr)?;
        
        // Set timeouts
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        
        Ok(Self {
            stream,
            read_buf: vec![0u8; 65536],
        })
    }
}

impl Transport for TcpTransport {
    fn send_frame(&mut self, frame: &[u8]) -> std::io::Result<()> {
        // TCP frame format: [u32 BE length][payload]
        let len = frame.len() as u32;
        self.stream.write_all(&len.to_be_bytes())?;
        self.stream.write_all(frame)?;
        self.stream.flush()?;
        Ok(())
    }

    fn recv_frame(&mut self) -> std::io::Result<Vec<u8>> {
        // Read length field (u32 BE)
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;

        if len > self.read_buf.len() {
            self.read_buf.resize(len, 0);
        }

        // Read payload exactly
        self.stream.read_exact(&mut self.read_buf[..len])?;
        Ok(self.read_buf[..len].to_vec())
    }

    fn close(&mut self) -> std::io::Result<()> {
        self.stream.shutdown(std::net::Shutdown::Both)?;
        Ok(())
    }
}
