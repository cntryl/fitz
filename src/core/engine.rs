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

use crate::authz::SessionAuth;
use crate::core::domain::{DomainContext, DomainResponse};
use crate::core::registry::DomainRegistry;
use crate::protocol::route::parse_route;

use crossbeam_channel::{Receiver, Sender, TrySendError};

pub type ConnectionId = u64;
pub type ChannelId = u32;

/// Number of engine shards (power of 2 for efficient hashing)
pub const NUM_SHARDS: usize = 8;

/// Engine inbox capacity (bounded to prevent memory exhaustion)
pub const ENGINE_INBOX_CAPACITY: usize = 1024;

/// Per-connection outbound queue capacity (used by transports)
pub const OUTBOUND_QUEUE_CAPACITY: usize = 256;

/// Choose which engine shard should handle a given route_family (tenant).
/// This ensures all connections for a tenant go to the same shard for consistency.
pub fn choose_shard(route_family: &str) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    route_family.hash(&mut hasher);
    let hash = hasher.finish();
    (hash as usize) % NUM_SHARDS
}

/// Pool of engine handles for sharded architecture.
/// Transports use this to route connections to the appropriate shard.
#[derive(Clone, Debug)]
pub struct EnginePool {
    shards: Arc<[EngineHandle; NUM_SHARDS]>,
}

impl EnginePool {
    /// Create a new engine pool from an array of handles
    pub fn new(shards: [EngineHandle; NUM_SHARDS]) -> Self {
        Self {
            shards: Arc::new(shards),
        }
    }
    
    /// Get the handle for a specific route_family (tenant)
    pub fn get_handle(&self, route_family: &str) -> &EngineHandle {
        let shard_id = choose_shard(route_family);
        &self.shards[shard_id]
    }
    
    /// Get handle by explicit shard index (for testing/admin)
    pub fn get_handle_by_index(&self, shard_id: usize) -> Option<&EngineHandle> {
        self.shards.get(shard_id)
    }
    
    /// Get all handles (for broadcast/cleanup operations)
    pub fn all_handles(&self) -> &Arc<[EngineHandle; NUM_SHARDS]> {
        &self.shards
    }
}

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
    /// Returns false if inbox is full (backpressure - caller should close connection).
    pub fn on_frame(&self, conn_id: ConnectionId, bytes: Vec<u8>) -> bool {
        match self.inbox.try_send(EngineEvent::Frame { conn_id, bytes }) {
            Ok(_) => true,
            Err(TrySendError::Full(_)) => {
                tracing::warn!("engine inbox full for conn {}, dropping frame", conn_id);
                false
            }
            Err(TrySendError::Disconnected(_)) => {
                tracing::error!("engine inbox disconnected for conn {}", conn_id);
                false
            }
        }
    }

    /// Called by WS task at disconnect.
    pub fn on_disconnect(&self, conn_id: ConnectionId) {
        // Disconnect events have priority, but still use try_send to avoid blocking
        let _ = self.inbox.try_send(EngineEvent::Disconnect { conn_id });
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
        };

        // Domain dispatch (synchronous)
        let response = self.domains.dispatch(parsed.scheme.as_str(), ctx)?;

        // Convert to bytes
        match response {
            DomainResponse::Frame(frame) => Ok(frame.into_vec()),
            DomainResponse::Ok => Ok(vec![]),
            DomainResponse::Error(err) => Err(err),
            DomainResponse::RpcDelivery {
                target_channel_id,
                message,
                ack_frame,
            } => {
                // TODO: Implement RPC message delivery via transport layer
                // For now, just return the acknowledgment
                // Transport needs to:
                // 1. Look up channel_id -> connection mapping
                // 2. Serialize message as frame
                // 3. Send via connection's outbound channel with backpressure handling
                let _ = (target_channel_id, message); // Suppress unused warning
                Ok(ack_frame.into_vec())
            }
            DomainResponse::NoticeDelivery {
                subscribers,
                notification_frame,
                ack_frame,
            } => {
                // TODO: Cross-domain coordination for control->notice fanout
                // Engine should query notice service for matching subscribers when
                // subscribers list is empty (e.g., from control domain).
                // This requires engine to have access to notice service reference.
                //
                // For now, NoticeDelivery from control will have empty subscribers
                // and no fanout will occur (same as before, but with cleaner separation).
                //
                // Future implementation:
                // 1. Extract route from notification_frame (TAG_ROUTE)
                // 2. Call notice_service.publish() to get matching subscribers
                // 3. For each (channel_id, _sub_id) in subscribers
                // 4. Look up channel_id -> connection mapping
                // 5. Send notification_frame via connection's outbound channel
                // 6. Handle backpressure per connection

                let _ = (subscribers, notification_frame); // Suppress unused warning
                Ok(match ack_frame {
                    Some(f) => f.into_vec(),
                    None => vec![],
                })
            }
        }
    }

    /// Register session authentication state for a connection.
    /// Called once during WebSocket setup after JWT verification.
    pub fn register_session(&self, conn_id: ConnectionId, session: SessionAuth) {
        self.registry.register_session(conn_id, session);
    }

    /// Called when the connection is established to register its outbound queue.
    pub fn register_connection(
        &self,
        conn_id: ConnectionId,
        outbound: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) {
        self.registry.register(conn_id, outbound);
    }

    /// Called when a channel is being cleaned up (connection disconnect, etc.)
    pub fn cleanup_channel(&self, channel_id: u32, route_family: crate::routing::RouteFamilyId) {
        self.domains.cleanup_channel(route_family, channel_id);
    }
}

/// Stores per-connection outbound queues, sessions, and channel routing.
/// Engine pushes outbound frames via these.
#[derive(Debug)]
pub struct EngineConnectionRegistry {
    /// conn_id → outbound SPSC producer (bounded for backpressure)
    conns: parking_lot::RwLock<HashMap<ConnectionId, tokio::sync::mpsc::Sender<Vec<u8>>>>,
    /// conn_id → session authentication/authorization state
    sessions: parking_lot::RwLock<HashMap<ConnectionId, SessionAuth>>,
    /// channel_id → conn_id (for routing replies/notifications)
    channel_to_conn: parking_lot::RwLock<HashMap<ChannelId, ConnectionId>>,
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
            sessions: parking_lot::RwLock::new(HashMap::new()),
            channel_to_conn: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn register_session(&self, conn_id: ConnectionId, session: SessionAuth) {
        self.sessions.write().insert(conn_id, session);
    }

    pub fn get_session(&self, conn_id: ConnectionId) -> Option<SessionAuth> {
        self.sessions.read().get(&conn_id).cloned()
    }

    pub fn register_channel(&self, channel_id: ChannelId, conn_id: ConnectionId) {
        self.channel_to_conn.write().entry(channel_id).or_insert(conn_id);
    }

    pub fn get_conn_for_channel(&self, channel_id: ChannelId) -> Option<ConnectionId> {
        self.channel_to_conn.read().get(&channel_id).copied()
    }

    pub fn register(&self, conn_id: ConnectionId, tx: tokio::sync::mpsc::Sender<Vec<u8>>) {
        self.conns.write().insert(conn_id, tx);
    }

    pub fn remove(&self, conn_id: ConnectionId) -> (Option<String>, Vec<ChannelId>) {
        self.conns.write().remove(&conn_id);
        
        // Get route_family from session before removing it
        let route_family = self.sessions.write()
            .remove(&conn_id)
            .map(|session| session.route_family);
        
        // Collect orphaned channels
        let orphaned: Vec<ChannelId> = self.channel_to_conn
            .read()
            .iter()
            .filter(|(_, &cid)| cid == conn_id)
            .map(|(&ch, _)| ch)
            .collect();
        
        // Remove channel mappings
        let mut channel_map = self.channel_to_conn.write();
        for ch in &orphaned {
            channel_map.remove(ch);
        }
        
        (route_family, orphaned)
    }

    pub fn send(&self, conn_id: ConnectionId, bytes: Vec<u8>) {
        if let Some(tx) = self.conns.read().get(&conn_id) {
            // Use try_send for non-blocking operation (engine is sync)
            match tx.try_send(bytes) {
                Ok(_) => {},
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!("outbound queue full for conn {}, dropping frame", conn_id);
                    // TODO: Consider marking connection for closure
                },
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("outbound queue closed for conn {}", conn_id);
                }
            }
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

        // Register channel \u2192 connection mapping (if first frame from this channel)
        self.registry.register_channel(channel_id, conn_id);

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

        // Look up session for authorization
        let session = match self.registry.get_session(conn_id) {
            Some(s) => s,
            None => {
                self.send_error(conn_id, channel_id, "no session found for connection");
                return;
            }
        };

        // Authorization check (before domain dispatch)
        if !session.grants.allows(&parsed) {
            self.send_error(conn_id, channel_id, "authorization denied");
            return;
        }

        // Build domain request context
        let ctx = DomainContext {
            route: parsed.clone(),
            route_str: route,
            payload,
            channel_id,
            route_family,
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
            Ok(DomainResponse::RpcDelivery {
                target_channel_id,
                message,
                ack_frame,
            }) => {
                // 1. Send RPC message to target inbox owner
                //    Lookup: channel_id \u2192 conn_id \u2192 outbound queue
                if let Some(target_conn_id) = self.registry.get_conn_for_channel(target_channel_id) {
                    // Serialize RpcMessage to frame bytes
                    let message_bytes = self.serialize_rpc_message(target_channel_id, message);
                    self.registry.send(target_conn_id, message_bytes);
                }
                
                // 2. Send ack back to requester (on their connection)
                self.registry.send(conn_id, ack_frame.into_vec());
            }
            Ok(DomainResponse::NoticeDelivery {
                subscribers,
                notification_frame,
                ack_frame,
            }) => {
                let bytes = notification_frame.into_vec();
                
                // Fanout to all subscribers
                // Each subscriber is identified by their channel_id
                for (sub_channel_id, _sub_id) in &subscribers {
                    // Lookup: channel_id \u2192 conn_id \u2192 outbound queue
                    if let Some(sub_conn_id) = self.registry.get_conn_for_channel(*sub_channel_id) {
                        self.registry.send(sub_conn_id, bytes.clone());
                    }
                }
                
                // Send ACK back to publisher if present
                if let Some(f) = ack_frame {
                    self.registry.send(conn_id, f.into_vec());
                }
            }
            Err(e) => {
                self.send_error(conn_id, channel_id, &e);
            }
        }
    }

    fn handle_disconnect(&self, conn_id: ConnectionId) {
        // Remove connection and get route_family + orphaned channels
        let (route_family_opt, orphaned_channels) = self.registry.remove(conn_id);
        
        // Convert route_family to RouteFamilyId
        // Use hash of the string to get the ID (matching what RouteTable does)
        let route_family_id = if let Some(rf) = route_family_opt {
            // Simple hash to RouteFamilyId (matching routing module)
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            rf.hash(&mut hasher);
            hasher.finish() as crate::routing::RouteFamilyId
        } else {
            // Fallback to default if no session found
            crate::routing::RouteFamilyId::default()
        };
        
        // Cleanup each orphaned channel in all domains
        for channel_id in orphaned_channels {
            self.domains.cleanup_channel(route_family_id, channel_id);
        }
    }

    fn send_error(&self, conn_id: ConnectionId, channel_id: u32, err: &str) {
        // Try to map error string to error code
        use crate::protocol::tags::*;
        let err_code = if err.contains("authorization") || err.contains("authz") {
            ERR_AUTHZ_DENIED
        } else if err.contains("session") || err.contains("unknown") {
            ERR_ENGINE_UNKNOWN_SESSION
        } else if err.contains("invalid route") || err.contains("route:") {
            ERR_ROUTE_INVALID
        } else if err.contains("parse") || err.contains("TLV") || err.contains("frame") {
            ERR_FRAME_INVALID
        } else if err.contains("scheme") || err.contains("unsupported") {
            ERR_SCHEME_UNSUPPORTED
        } else {
            ERR_ENGINE_INTERNAL
        };
        
        let frame = crate::protocol::frame::make_error_with_code(channel_id, err_code, err);
        self.registry.send(conn_id, frame);
    }

    fn serialize_rpc_message(
        &self,
        channel_id: u32,
        message: crate::core::rpc::RpcMessage,
    ) -> Vec<u8> {
        use crate::protocol::frame::build_tlv;
        use crate::protocol::tags::{TAG_BODY, TAG_ID, TAG_ROUTE, TAG_ROUTE_REPLY, TAG_SEQ, TAG_STREAM_END, FRAME_DAT};
        
        // Build TLVs for the message
        let mut payload = Vec::new();
        build_tlv(TAG_ROUTE, message.route.as_bytes(), &mut payload);
        
        if let Some(corr_id) = message.correlation_id {
            build_tlv(TAG_ID, corr_id.as_bytes(), &mut payload);
        }
        
        if !message.body.is_empty() {
            build_tlv(TAG_BODY, &message.body, &mut payload);
        }
        
        if let Some(reply_route) = message.reply_route {
            build_tlv(TAG_ROUTE_REPLY, reply_route.as_bytes(), &mut payload);
        }
        
        if let Some(seq) = message.seq {
            build_tlv(TAG_SEQ, &seq.to_be_bytes(), &mut payload);
        }
        
        if message.is_stream_end {
            build_tlv(TAG_STREAM_END, &[], &mut payload);
        }
        
        // Build frame with header (frame_type=FRAME_DAT, flags=0)
        crate::protocol::frame::build_frame(FRAME_DAT, 0, channel_id, &payload)
    }
}

/// Start NUM_SHARDS engine threads and return an EnginePool.
/// Each engine runs in its own OS thread for deterministic, non-blocking processing.
pub fn start_engine_pool() -> EnginePool {
    use std::array;
    
    tracing::info!("starting {} engine shards", NUM_SHARDS);
    
    // Create domain registry (shared across all shards for now)
    let domains = Arc::new(DomainRegistry::new());
    
    // Create shards
    let handles: [EngineHandle; NUM_SHARDS] = array::from_fn(|shard_id| {
        // Create bounded channel for this shard
        let (tx, rx) = crossbeam_channel::bounded(ENGINE_INBOX_CAPACITY);
        
        // Create per-shard registry
        let registry = Arc::new(EngineConnectionRegistry::new());
        
        // Create engine
        let engine = Engine::new(rx, Arc::clone(&registry), Arc::clone(&domains));
        
        // Spawn engine thread
        let thread_name = format!("engine-shard-{}", shard_id);
        std::thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                tracing::info!("{} started", thread_name);
                engine.run();
                tracing::info!("{} stopped", thread_name);
            })
            .expect("failed to spawn engine thread");
        
        // Return handle for this shard
        EngineHandle::new(tx, Arc::clone(&domains), registry)
    });
    
    tracing::info!("all {} engine shards started", NUM_SHARDS);
    
    EnginePool::new(handles)
}
