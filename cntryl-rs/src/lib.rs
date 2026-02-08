//! Fitz Rust Client Library
//!
//! A synchronous client library for the Fitz event streaming and orchestration platform.
//! Supports both TCP and WebSocket transports.
//!
//! # Example
//!
//! ```ignore
//! use cntryl::FitzClient;
//!
//! let client = FitzClient::connect_tcp("127.0.0.1", 4091, "my-realm", "secret")?;
//!
//! let mut tx = client.kv().begin("app", "users", TransactionMode::ReadWrite)?;
//! tx.put(b"user:1", b"alice")?;
//! let value = tx.get(b"user:1")?;
//! tx.commit()?;
//! ```

pub mod auth;
pub mod codec;
pub mod connection;
pub mod error;
pub mod protocol;
pub mod transport;
pub mod domains;

pub use auth::TestTokenGenerator;
pub use error::{FitzError, Result};
pub use protocol::{Route, TransactionMode};

use connection::FitzConnection;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Builder for creating Fitz clients with flexible configuration
pub struct FitzClientBuilder {
    realm: String,
    secret: String,
    timeout: Duration,
}

impl FitzClientBuilder {
    /// Create a new client builder
    pub fn new(realm: &str, secret: &str) -> Self {
        Self {
            realm: realm.to_string(),
            secret: secret.to_string(),
            timeout: Duration::from_secs(30),
        }
    }

    /// Set connection timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Connect via TCP
    pub fn connect_tcp(self, host: &str, port: u16) -> Result<FitzClient> {
        let mut conn = FitzConnection::connect_tcp(host, port)?;
        
        // Generate JWT
        let gen = TestTokenGenerator::new(&self.secret);
        let token = gen.generate(&self.realm, "fitz-client")?;

        // Send CONNECT frame with JWT
        let connect_frame = codec::encode_message_frame(protocol::message_type::CONNECT, token.as_bytes());
        conn.send_frame(&connect_frame)?;

        // Expect empty response
        let resp = conn.recv_frame()?;
        if !resp.is_empty() {
            return Err(FitzError::Protocol("Expected empty CONNECT response".into()));
        }

        Ok(FitzClient {
            connection: Arc::new(Mutex::new(conn)),
            realm: self.realm.clone(),
            route_family: 1,
        })
    }

    /// Connect via WebSocket
    pub fn connect_ws(self, url: &str) -> Result<FitzClient> {
        let mut conn = FitzConnection::connect_ws(url)?;
        
        // Generate JWT
        let gen = TestTokenGenerator::new(&self.secret);
        let token = gen.generate(&self.realm, "fitz-client")?;

        // Send CONNECT frame with JWT
        let connect_frame = codec::encode_message_frame(protocol::message_type::CONNECT, token.as_bytes());
        conn.send_frame(&connect_frame)?;

        // Expect empty response
        let resp = conn.recv_frame()?;
        if !resp.is_empty() {
            return Err(FitzError::Protocol("Expected empty CONNECT response".into()));
        }

        Ok(FitzClient {
            connection: Arc::new(Mutex::new(conn)),
            realm: self.realm.clone(),
            route_family: 1,
        })
    }
}

/// Main Fitz client
pub struct FitzClient {
    connection: Arc<Mutex<FitzConnection>>,
    realm: String,
    route_family: u64,
}

impl FitzClient {
    /// Create a builder
    pub fn builder(realm: &str, secret: &str) -> FitzClientBuilder {
        FitzClientBuilder::new(realm, secret)
    }

    /// Convenient helper: connect via TCP
    pub fn connect_tcp(host: &str, port: u16, realm: &str, secret: &str) -> Result<Self> {
        FitzClient::builder(realm, secret).connect_tcp(host, port)
    }

    /// Convenient helper: connect via WebSocket
    pub fn connect_ws(url: &str, realm: &str, secret: &str) -> Result<Self> {
        FitzClient::builder(realm, secret).connect_ws(url)
    }

    /// Get a KV client
    pub fn kv(&self) -> domains::kv::KvClient {
        domains::kv::KvClient::new(
            self.connection.clone(),
            self.realm.clone(),
            self.route_family,
        )
    }

    /// Get a Queue client
    pub fn queue(&self) -> domains::queue::QueueClient {
        domains::queue::QueueClient::new()
    }

    /// Get a Notice (pub/sub) client
    pub fn notice(&self) -> domains::notice::NoticeClient {
        domains::notice::NoticeClient::new()
    }

    /// Get an RPC client
    pub fn rpc(&self) -> domains::rpc::RpcClient {
        domains::rpc::RpcClient::new()
    }

    /// Get a Lease client
    pub fn lease(&self) -> domains::lease::LeaseClient {
        domains::lease::LeaseClient::new()
    }

    /// Get a Stream client
    pub fn stream(&self) -> domains::stream::StreamClient {
        domains::stream::StreamClient::new()
    }

    /// Get a Schedule client
    pub fn schedule(&self) -> domains::schedule::ScheduleClient {
        domains::schedule::ScheduleClient::new()
    }

    /// Close the connection
    pub fn close(&self) -> Result<()> {
        let mut conn = self.connection.lock()
            .map_err(|_| FitzError::Connection("Poisoned mutex".into()))?;
        conn.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_client_builder() {
        let builder = FitzClient::builder("test-realm", "secret");
        assert!(builder.realm == "test-realm");
    }
}
