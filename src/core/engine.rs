//! Core message dispatcher (synchronous).
//!
//! The engine is a single-threaded (or sharded) synchronous message pump.
//! - Deterministic ordering
//! - No futures, no tokio, no awaits
//! - No task spawning
//! - All domain handlers are synchronous
//! - WS layer delivers inbound frames via crossbeam/SPSC
//! - Engine pushes outbound frames via per-connection SPSC
//!
//! This is the “Fitz max performance” architecture:
//!   async WS edges → sync engine → async WS edges.

use std::collections::HashMap;
use std::sync::Arc;

use crate::core::domain::{DomainContext, DomainResponse};
use crate::core::registry::DomainRegistry;
use crate::protocol::route::parse_route;
use crate::routing::RouteFamilyId;

use crossbeam_channel::{Receiver, Sender};

pub type ConnectionId = u64;

pub enum EngineEvent {
    Frame {
        conn_id: ConnectionId,
        bytes: Vec<u8>,
    },
    Disconnect {
        conn_id: ConnectionId,
    },
}

/// A handle held by async WS tasks.
/// They push inbound frames into `inbox`.
/// They hold their per-connection outbound `Sender<Vec<u8>>`.
#[derive(Clone)]
pub struct EngineHandle {
    inbox: Sender<EngineEvent>,
}

impl EngineHandle {
    pub fn new(inbox: Sender<EngineEvent>) -> Self {
        Self { inbox }
    }

    /// Called by WS reader task when a frame arrives.
    pub fn on_frame(&self, conn_id: ConnectionId, bytes: Vec<u8>) {
        let _ = self.inbox.send(EngineEvent::Frame { conn_id, bytes });
    }

    /// Called by WS task at disconnect.
    pub fn on_disconnect(&self, conn_id: ConnectionId) {
        let _ = self.inbox.send(EngineEvent::Disconnect { conn_id });
    }

    /// Called when the connection is established to register its outbound queue.
    pub fn register_connection(
        &self,
        registry: &EngineConnectionRegistry,
        conn_id: ConnectionId,
        outbound: Sender<Vec<u8>>,
    ) {
        registry.register(conn_id, outbound);
    }
}

/// Stores per-connection outbound queues.
/// Engine pushes outbound frames via these.
pub struct EngineConnectionRegistry {
    conns: parking_lot::RwLock<HashMap<ConnectionId, Sender<Vec<u8>>>>,
}

impl EngineConnectionRegistry {
    pub fn new() -> Self {
        Self {
            conns: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, conn_id: ConnectionId, tx: Sender<Vec<u8>>) {
        self.conns.write().insert(conn_id, tx);
    }

    pub fn remove(&self, conn_id: ConnectionId) {
        self.conns.write().remove(&conn_id);
    }

    pub fn send(&self, conn_id: ConnectionId, bytes: Vec<u8>) {
        if let Some(tx) = self.conns.read().get(&conn_id) {
            let _ = tx.send(bytes);
        }
    }
}

/// Engine: a synchronous event loop.
pub struct Engine {
    inbox: Receiver<EngineEvent>,
    registry: EngineConnectionRegistry,
    domains: Arc<DomainRegistry>,
}

impl Engine {
    pub fn new(
        inbox: Receiver<EngineEvent>,
        registry: EngineConnectionRegistry,
        domains: Arc<DomainRegistry>,
    ) -> Self {
        Self {
            inbox,
            registry,
            domains,
        }
    }

    /// Main loop — runs on a dedicated thread.
    pub fn run(&self) {
        while let Ok(event) = self.inbox.recv() {
            match event {
                EngineEvent::Frame { conn_id, bytes } => {
                    self.handle_frame(conn_id, bytes);
                }
                EngineEvent::Disconnect { conn_id } => {
                    self.handle_disconnect(conn_id);
                }
            }
        }
    }

    fn handle_frame(&self, conn_id: ConnectionId, bytes: Vec<u8>) {
        // Parse TLV or your protocol envelope.
        // Assume you already extracted route, payload, route_family, channel_id, etc.
        let (route, payload, channel_id, route_family) =
            match crate::protocol::frame::decode(bytes) {
                Ok(v) => v,
                Err(e) => {
                    self.send_error(conn_id, channel_id, &e);
                    return;
                }
            };

        // Parse route
        let parsed = match parse_route(&route) {
            Ok(r) => r,
            Err(e) => {
                self.send_error(conn_id, channel_id, &format!("invalid route: {}", e));
                return;
            }
        };

        // Build domain request context
        let ctx = DomainContext {
            route: parsed.clone(),
            route_str: route,
            payload,
            channel_id,
            route_family,
            sender: None,
        };

        // Domain dispatch (synchronous)
        let response = self
            .domains
            .dispatch(parsed.scheme.as_str(), ctx);

        // Write back
        match response {
            Ok(DomainResponse::Frame(frame)) => {
                self.registry.send(conn_id, frame.into_vec());
            }
            Ok(DomainResponse::Ok) => {
                // empty response allowed
                self.registry.send(
                    conn_id,
                    crate::protocol::frame::PooledFrame::empty().into_vec(),
                );
            }
            Ok(DomainResponse::Error(err)) => {
                self.send_error(conn_id, channel_id, &err);
            }
            Err(e) => {
                self.send_error(conn_id, channel_id, &e);
            }
        }
    }

    fn handle_disconnect(&self, conn_id: ConnectionId) {
        self.registry.remove(conn_id);
        self.domains.cleanup_channel(conn_id);
    }

    fn send_error(&self, conn_id: ConnectionId, channel_id: u32, err: &str) {
        let frame = crate::protocol::frame::make_error(channel_id, err);
        self.registry.send(conn_id, frame.into_vec());
    }
}
