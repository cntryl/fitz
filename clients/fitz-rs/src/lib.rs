//! Fitz Rust Client Library
//!
//! A synchronous client library for the Fitz event streaming and orchestration platform.
//! Supports both TCP and WebSocket transports.
//!
//! # Quick Start
//!
//! ```ignore
//! use cntryl::FitzClient;
//!
//! // Connect — realm is in the JWT, not on the client struct.
//! let client = FitzClient::connect_tcp("127.0.0.1", 4091, "my-realm", "secret")?;
//!
//! // Routes are opaque strings — the client never parses them.
//! let tx = client.kv().begin("kv://my-realm/app/users", TransactionMode::ReadWrite)?;
//! tx.put(b"user:1", b"alice")?;
//! tx.commit()?;
//!
//! // Leases
//! let grant = client.lease().acquire("lease://my-realm/locks/leader", "node-1", 30)?;
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
pub use protocol::TransactionMode;

use connection::{FitzConnection, SharedConnection};
use std::time::Duration;

/// Builder for creating Fitz clients with flexible configuration.
///
/// `realm` is embedded in the JWT during CONNECT — it is not stored
/// on the client or passed to domain methods.
pub struct FitzClientBuilder {
    realm: String,
    secret: String,
    timeout: Duration,
}

impl FitzClientBuilder {
    pub fn new(realm: &str, secret: &str) -> Self {
        Self {
            realm: realm.to_string(),
            secret: secret.to_string(),
            timeout: Duration::from_secs(30),
        }
    }

    /// Set connection timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Connect via TCP.
    pub fn connect_tcp(self, host: &str, port: u16) -> Result<FitzClient> {
        let conn = FitzConnection::connect_tcp(host, port)?;
        self.finish(conn)
    }

    /// Connect via WebSocket.
    pub fn connect_ws(self, url: &str) -> Result<FitzClient> {
        let conn = FitzConnection::connect_ws(url)?;
        self.finish(conn)
    }

    fn finish(self, conn: FitzConnection) -> Result<FitzClient> {
        let shared = SharedConnection::new(conn);

        // Generate JWT and send CONNECT frame.
        // Per wire protocol: silence means success, server closes on invalid CONNECT.
        let gen = TestTokenGenerator::new(&self.secret);
        let token = gen.generate(&self.realm, "fitz-client")?;
        shared.send_only(protocol::message_type::CONNECT, token.as_bytes())?;

        Ok(FitzClient { connection: shared })
    }
}

/// Main Fitz client — the single entry point for all domains.
///
/// Create one per connection. Call `.kv()`, `.lease()`, etc. to get
/// domain-specific handles (they share the underlying connection).
///
/// The client is intentionally realm-agnostic — realm context lives in
/// the JWT sent during CONNECT, and route strings carry the addressing.
pub struct FitzClient {
    connection: SharedConnection,
}

impl FitzClient {
    /// Create a builder.
    pub fn builder(realm: &str, secret: &str) -> FitzClientBuilder {
        FitzClientBuilder::new(realm, secret)
    }

    /// Convenient helper: connect via TCP.
    pub fn connect_tcp(host: &str, port: u16, realm: &str, secret: &str) -> Result<Self> {
        FitzClient::builder(realm, secret).connect_tcp(host, port)
    }

    /// Convenient helper: connect via WebSocket.
    pub fn connect_ws(url: &str, realm: &str, secret: &str) -> Result<Self> {
        FitzClient::builder(realm, secret).connect_ws(url)
    }

    /// Get a KV domain client.
    pub fn kv(&self) -> domains::kv::KvClient {
        domains::kv::KvClient::new(self.connection.clone())
    }

    /// Get a Lease domain client.
    pub fn lease(&self) -> domains::lease::LeaseClient {
        domains::lease::LeaseClient::new(self.connection.clone())
    }

    /// Close the connection.
    pub fn close(&self) -> Result<()> {
        self.connection.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_client_builder() {
        let builder = FitzClient::builder("test-realm", "secret");
        assert_eq!(builder.realm, "test-realm");
    }
}
