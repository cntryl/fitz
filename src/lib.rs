//! # Fitz: Actor-Based Unified Messaging Runtime
//!
//! Fitz is a unified messaging platform built on an actor model, powered by Midge for durable storage.
//! Everything fast is done in-memory. Everything durable is delegated. Everything complex is decomposed.
//!
//! Fitz delivers streams, queues, KV, routing, RPC, notices, and real-time coordination through a single,
//! coherent architecture.
//!
//! ## Quick Start
//!
//! ```ignore
//! use fitz::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let fitz = FitzSystemBuilder::new().build().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Module Overview
//!
//! - **actor**: Core actor runtime (mailbox, scheduler, supervision)
//! - **messages**: Message types for actor coordination
//! - **personas**: High-level actors (session, realm, router, stream, queue, rpc, lease, metrics)
//! - **storage**: Midge integration layer for durable persistence
//! - **transport**: Network layer (TCP, WebSocket, multiplexing)
//! - **routing**: Route parsing and matching DSL
//! - **kv**: KV storage wrapper
//! - **metrics**: Internal observability system
//! - **api**: Public SDK-facing APIs
//! - **config**: Configuration and tuning
//! - **util**: Small utilities (IDs, time, buffers)
//! - **bootstrap**: System initialization

pub mod actor;
pub mod messages;
pub mod domains;
pub mod storage;
pub mod transport;
pub mod routing;
pub mod metrics;
pub mod api;
pub mod config;
pub mod util;
pub mod bootstrap;
pub mod prelude;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
