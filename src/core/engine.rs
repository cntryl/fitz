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
#[derive(Clone, Debug)]
pub struct EngineHandle {
    inbox: Sender<EngineEvent>,
    domains: Arc<DomainRegistry>,
    registry: Arc<EngineConnectionRegistry>,
}

impl EngineHandle {
    pub fn new(
        inbox: Sender<EngineEvent>,
        domains: Arc<DomainRegistry>,
        registry: Arc<EngineConnectionRegistry>,
    ) -> Self {
        Self {
            inbox,
            domains,
            registry,
        }
    }

    /// Called by WS reader task when a frame arrives.
    pub fn on_frame(&self, conn_id: ConnectionId, bytes: Vec<u8>) {
        let _ = self.inbox.send(EngineEvent::Frame { conn_id, bytes });
    }

    /// Called by WS task at disconnect.
    pub fn on_disconnect(&self, conn_id: ConnectionId) {
        let _ = self.inbox.send(EngineEvent::Disconnect { conn_id });
    }

    /// Synchronous dispatch for transport layer.
    /// Builds a frame, calls domain dispatch, returns response bytes.
    pub fn dispatch(
        &self,
        route: String,
        payload: Vec<u8>,
        channel_id: u32,
        route_family: crate::routing::RouteFamilyId,
    ) -> Result<Vec<u8>, String> {
        // Parse route
        let parsed = match parse_route(&route) {
            Ok(r) => r,
            Err(e) => return Err(format!("invalid route: {}", e)),
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
        let response = self.domains.dispatch(parsed.scheme.as_str(), ctx)?;

        // Convert to bytes
        match response {
            DomainResponse::Frame(frame) => Ok(frame.into_vec()),
            DomainResponse::Ok => Ok(vec![]),
            DomainResponse::Error(err) => Err(err),
        }
    }

    /// Called when the connection is established to register its outbound queue.
    pub fn register_connection(
        &self,
        conn_id: ConnectionId,
        outbound: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    ) {
        self.registry.register(conn_id, outbound);
    }

    /// Called when a channel is being cleaned up (connection disconnect, etc.)
    pub fn cleanup_channel(&self, channel_id: u32, route_family: crate::routing::RouteFamilyId) {
        self.domains.cleanup_channel(route_family, channel_id);
    }
}

/// Stores per-connection outbound queues.
/// Engine pushes outbound frames via these.
#[derive(Debug)]
pub struct EngineConnectionRegistry {
    conns: parking_lot::RwLock<HashMap<ConnectionId, tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>,
}

impl Default for EngineConnectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineConnectionRegistry {
    pub fn new() -> Self {
        Self {
            conns: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, conn_id: ConnectionId, tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
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
    registry: Arc<EngineConnectionRegistry>,
    domains: Arc<DomainRegistry>,
}

impl Engine {
    pub fn new(
        inbox: Receiver<EngineEvent>,
        registry: Arc<EngineConnectionRegistry>,
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
        // Parse frame header first to get channel_id for error reporting
        let parsed_frame = match crate::protocol::frame::parse_frame(&bytes) {
            Ok(f) => f,
            Err(e) => {
                // Can't get channel_id, use 0
                self.send_error(conn_id, 0, &format!("frame parse error: {:?}", e));
                return;
            }
        };
        let channel_id = parsed_frame.header.channel_id;

        // Decode TLVs
        let (route, payload, route_family) = match crate::protocol::frame::decode(bytes) {
            Ok((r, p, rf)) => (r, p, rf),
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
        let response = self.domains.dispatch(parsed.scheme.as_str(), ctx);

        // Write back
        match response {
            Ok(DomainResponse::Frame(frame)) => {
                self.registry.send(conn_id, frame.into_vec());
            }
            Ok(DomainResponse::Ok) => {
                // empty response allowed
                self.registry.send(
                    conn_id,
                    crate::protocol::frame::PooledFrame::from_vec(vec![]).into_vec(),
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
        // TODO: cleanup channels - need to track which channels belong to which connection
    }

    fn send_error(&self, conn_id: ConnectionId, channel_id: u32, err: &str) {
        let frame = crate::protocol::frame::make_error(channel_id, err);
        self.registry.send(conn_id, frame.to_vec());
    }
}
