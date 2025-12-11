//! Network transport layer.
//!
//! Transport is fully decoupled from durability and realms.
//! It owns:
//! - TCP and WebSocket connections
//! - Frame multiplexing
//! - Session bootstrapping
//! - Mapping connections → SessionActors

pub mod tcp;
pub mod websocket;
pub mod multiplexer;
pub mod protocol;
