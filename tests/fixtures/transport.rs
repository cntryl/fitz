//! Transport helpers for end-to-end integration tests
//!
//! Provides connector traits and frame builders for each domain.
//! Tests use generic async functions parameterized by connector type.

#![allow(dead_code)]

// Re-export testkit types for test files
use bytes::BufMut;
pub use fitz::testkit::{
    TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder, TlvFrameParser,
};

#[inline]
fn u32_len(value: usize) -> u32 {
    u32::try_from(value)
        .unwrap_or_else(|_| panic!("transport frame length exceeds u32::MAX: {value}"))
}

#[inline]
fn u64_from_i32(value: i32) -> u64 {
    u64::try_from(value).unwrap_or_else(|_| panic!("negative TTL seconds are invalid: {value}"))
}

#[inline]
fn msg_type_to_u8(msg_type: u16) -> u8 {
    msg_type.to_le_bytes()[0]
}

#[inline]
fn u32_to_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or_else(|_| panic!("u32 value does not fit usize: {value}"))
}

// ============================================================================
// GENERIC CONNECTOR TRAITS
// ============================================================================

/// Generic test client trait for sending/receiving frames
#[async_trait::async_trait]
pub trait TestConnectorClient: Send {
    async fn request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), String>;
}

#[async_trait::async_trait]
pub trait FrameReceivingConnector: TestConnectorClient {
    async fn recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String>;
}

// ============================================================================
// TCP AND WEBSOCKET CONNECTOR WRAPPER STRUCTS
// ============================================================================

pub struct TcpClient(TestClient);
pub struct WsClient(TestWebSocketClient);

#[async_trait::async_trait]
trait FixtureTransportClient: Send {
    async fn fixture_request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
    async fn fixture_send_frame(&mut self, frame: &[u8]) -> Result<(), String>;
    async fn fixture_recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String>;
}

trait HasFixtureClient {
    type Client: FixtureTransportClient;

    fn client_mut(&mut self) -> &mut Self::Client;
}

async fn connect_tcp_raw(server: &TestServer) -> Result<TestClient, String> {
    TestClient::new(server.tcp_addr)
        .await
        .map_err(|e| e.to_string())
}

async fn connect_ws_raw(server: &TestServer) -> Result<TestWebSocketClient, String> {
    let url = format!("ws://{}", server.ws_addr);
    TestWebSocketClient::connect(&url)
        .await
        .map_err(|e| e.to_string())
}

#[async_trait::async_trait]
impl FixtureTransportClient for TestClient {
    async fn fixture_request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn fixture_send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.send_frame(frame).await.map_err(|e| e.to_string())
    }

    async fn fixture_recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.recv_frame(timeout_ms).await.map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl FixtureTransportClient for TestWebSocketClient {
    async fn fixture_request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn fixture_send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.send_frame(frame).await.map_err(|e| e.to_string())
    }

    async fn fixture_recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.recv_frame(timeout_ms).await.map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl TestConnectorClient for TcpClient {
    async fn request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.0.send_frame(frame).await.map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl FrameReceivingConnector for TcpClient {
    async fn recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .recv_frame(timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl<T> TestConnectorClient for T
where
    T: HasFixtureClient + Send,
{
    async fn request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.client_mut().fixture_request(frame, timeout_ms).await
    }

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.client_mut().fixture_send_frame(frame).await
    }
}

#[async_trait::async_trait]
impl TestConnectorClient for WsClient {
    async fn request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.0.send_frame(frame).await.map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl FrameReceivingConnector for WsClient {
    async fn recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .recv_frame(timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl<T> FrameReceivingConnector for T
where
    T: HasFixtureClient + Send,
{
    async fn recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.client_mut().fixture_recv_frame(timeout_ms).await
    }
}

mod kv;
mod lease;
mod notice;
mod queue;
mod rpc;
mod schedule;
mod stream;

pub use kv::*;
pub use lease::*;
pub use notice::*;
pub use queue::*;
pub use rpc::*;
pub use schedule::*;
pub use stream::*;
