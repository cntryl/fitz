//! Session-level actor for connection lifecycle and authorization
//!
//! SessionActor is responsible for:
//! 1. Enforcing all authentication and authorization for a connection
//! 2. Managing domain-specific session state (subscriptions, transactions, etc.)
//! 3. Delegating to domain actors (NoticeRouteActor, RpcActor, etc.) after validation
//! 4. Cleaning up session state on disconnect
//!
//! # Authorization Model
//!
//! SessionActor trusts the transport layer for identity binding (JWT validation, mTLS, etc.)
//! and enforces domain-specific permission checks:
//!
//! - **Notification**: Prefix-based route patterns (exact, area wildcard, realm wildcard, global)
//! - **RPC**: Method-level permissions (future)
//! - **Queue**: Queue-level permissions (future)
//! - **Stream**: Stream-level permissions (future)
//! - **Lease**: Lease scope permissions (future)
//! - **KV**: Key prefix permissions (future)
//!
//! # Session Lifecycle
//!
//! 1. Transport accepts connection, validates identity → creates SessionActor
//! 2. SessionActor enforces permissions for all domain operations
//! 3. On disconnect, SessionActor triggers cleanup (unsubscribe all, release leases, etc.)

use crate::runtime::actor::{Actor, Context};

/// Session unique identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

/// Session identity and claims (from JWT or mTLS)
///
/// Passed to SessionActor on creation by the transport layer.
#[derive(Debug, Clone)]
pub struct SessionClaims {
    /// Unique session identifier
    pub session_id: SessionId,
    /// Realm ID (from "realm" claim)
    pub realm: String,
    /// Subject (from "sub" claim, typically user/service identity)
    pub subject: String,
    /// Raw domain-specific claims (passed to domain handlers)
    pub domain_claims: std::collections::HashMap<String, String>,
}

/// SessionActor base type
///
/// This is extended by domain-specific session implementations.
/// For notification domain, see `domains::notification::session::NotificationSessionActor`.
pub struct SessionActor {
    /// Session claims from authentication layer
    pub claims: SessionClaims,
    /// Session state (domain-specific, stored as opaque bytes for now)
    pub state: std::collections::HashMap<String, Vec<u8>>,
}

impl SessionActor {
    /// Create a new SessionActor with validated claims
    pub fn new(claims: SessionClaims) -> Self {
        Self {
            claims,
            state: std::collections::HashMap::new(),
        }
    }

    /// Get the session ID
    pub fn session_id(&self) -> SessionId {
        self.claims.session_id
    }

    /// Get the realm
    pub fn realm(&self) -> &str {
        &self.claims.realm
    }

    /// Get the subject (identity)
    pub fn subject(&self) -> &str {
        &self.claims.subject
    }
}

impl Actor for SessionActor {
    // Base SessionActor is message-agnostic; domain-specific actors will define their message types
    type Message = ();

    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {
        // Base SessionActor does nothing; domain handlers extend this
    }

    fn stopped(&mut self) {
        // Cleanup on disconnect (domain-specific cleanup happens in domain implementations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_claims() -> SessionClaims {
        let mut domain_claims = std::collections::HashMap::new();
        domain_claims.insert("permissions".to_string(), "notice:*".to_string());

        SessionClaims {
            session_id: SessionId(1),
            realm: "acme".to_string(),
            subject: "user@example.com".to_string(),
            domain_claims,
        }
    }

    #[test]
    fn should_create_session_from_claims() {
        // Arrange & Act
        let claims = test_claims();
        let session = SessionActor::new(claims.clone());

        // Assert
        assert_eq!(session.session_id(), SessionId(1));
        assert_eq!(session.realm(), "acme");
        assert_eq!(session.subject(), "user@example.com");
    }

    #[test]
    fn should_store_domain_state() {
        // Arrange
        let claims = test_claims();
        let mut session = SessionActor::new(claims);

        // Act
        session.state.insert("subscription_count".to_string(), vec![1, 0]);

        // Assert
        assert_eq!(session.state.get("subscription_count"), Some(&vec![1, 0]));
    }
}
