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

use crossbeam_channel::{Receiver, Sender};

pub type ConnectionId = u64;
pub type ChannelId = u32;

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
        outbound: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
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
    /// conn_id → outbound SPSC producer
    conns: parking_lot::RwLock<HashMap<ConnectionId, tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>,
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

    pub fn register(&self, conn_id: ConnectionId, tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
        self.conns.write().insert(conn_id, tx);
    }

    pub fn remove(&self, conn_id: ConnectionId) -> Vec<ChannelId> {
        self.conns.write().remove(&conn_id);
        self.sessions.write().remove(&conn_id);
        
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
        
        orphaned
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
        // Remove connection and get orphaned channels
        let orphaned_channels = self.registry.remove(conn_id);
        
        // Cleanup each orphaned channel in all domains
        for channel_id in orphaned_channels {
            // We need route_family for cleanup, but we don't have it here
            // For now, cleanup will be called from domains without route_family check
            // TODO: Store route_family with session and pass it here
            // Workaround: Use a sentinel route_family or update cleanup_channel signature
            self.domains.cleanup_channel(crate::routing::RouteFamilyId::default(), channel_id);
        }
    }

    fn send_error(&self, conn_id: ConnectionId, channel_id: u32, err: &str) {
        let frame = crate::protocol::frame::make_error(channel_id, err);
        self.registry.send(conn_id, frame.to_vec());
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
