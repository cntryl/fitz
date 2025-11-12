//! Core message dispatcher that routes requests to domain handlers.
//!
//! The engine is a high-performance message pump optimized for concurrency:
//! - Non-blocking dispatch loop (spawns domain work to avoid blocking on slow domains)
//! - Unbounded channel for backpressure tolerance
//! - Concurrent domain handling via tokio task spawning
//! - Receives: Dispatch command with (route, payload, channel_id, route_family)
//! - Parses: Route to determine which domain handles it
//! - Spawns: Domain handler work as separate tokio tasks
//! - Returns: Response correlated by channel_id via oneshot
//! - Cleanup: When channel closes, notify all domains
//!
//! Key design: Engine loop stays fast and responsive. All domain work runs concurrently.
//! Domains handle their own concerns (pub/sub, lifecycle, cleanup, storage, etc.)

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::core::domain::{DomainContext, DomainResponse};
use crate::core::registry::DomainRegistry;
use crate::protocol::route::parse_route;
use crate::storage::RouteFamilyId;

/// Engine commands
#[derive(Debug)]
pub enum EngineCommand {
    /// Route a message to the appropriate domain handler
    Dispatch {
        route: String,
        payload: Vec<u8>,
        channel_id: u32,
        route_family: RouteFamilyId,
        resp: oneshot::Sender<Result<crate::protocol::frame::PooledFrame, String>>,
    },

    /// Notify all domains that a channel is closing (connection dropped, client disconnected, etc.)
    /// Domains use this to cleanup subscriptions, sessions, resources, etc.
    CleanupChannel {
        channel_id: u32,
        route_family: RouteFamilyId,
    },
}

#[derive(Clone, Debug)]
pub struct EngineHandle {
    tx: mpsc::UnboundedSender<EngineCommand>,
}

impl EngineHandle {
    pub fn new(tx: mpsc::UnboundedSender<EngineCommand>) -> Self {
        Self { tx }
    }

    /// Dispatch a message to the appropriate domain handler
    /// Returns TLV-encoded response correlated by channel_id
    pub async fn dispatch(
        &self,
        route: String,
        payload: Vec<u8>,
        channel_id: u32,
        route_family: RouteFamilyId,
    ) -> Result<Vec<u8>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::Dispatch {
            route,
            payload,
            channel_id,
            route_family,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .map_err(|_| "engine stopped".to_string())?;

        let pooled = rx.await.map_err(|_| "no response".to_string())?;
        pooled.map(|pf| pf.into_vec()).map_err(|e| e)
    }

    /// Notify domains that a channel is closing (connection dropped, client disconnected, etc.)
    /// Domains use this to cleanup subscriptions, inboxes, resources, etc.
    pub async fn cleanup_channel(
        &self,
        channel_id: u32,
        route_family: RouteFamilyId,
    ) -> Result<(), String> {
        let cmd = EngineCommand::CleanupChannel {
            channel_id,
            route_family,
        };
        self.tx
            .send(cmd)
            .map_err(|_| "engine stopped".to_string())?;
        Ok(())
    }
}

/// Start the engine
pub fn start_engine() -> EngineHandle {
    let (_jh, handle) = start_engine_with_join();
    handle
}

/// Start the engine and return handle + join handle
pub fn start_engine_with_join() -> (JoinHandle<()>, EngineHandle) {
    // Use unbounded channel to prevent backpressure on high-throughput scenarios
    // Engine spawns work concurrently, so channel never blocks
    let (tx, mut rx) = mpsc::unbounded_channel::<EngineCommand>();
    let handle = EngineHandle::new(tx.clone());

    // Create domain registry - all domains initialized here
    let registry = Arc::new(DomainRegistry::new());

    let jh = tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                EngineCommand::Dispatch {
                    route,
                    payload,
                    channel_id,
                    route_family,
                    resp,
                } => {
                    // Clone registry Arc for spawned task
                    let registry_clone = Arc::clone(&registry);
                    
                    // Spawn domain work as separate task to keep engine loop fast and responsive.
                    // Move values directly into task closure - no per-request allocation overhead.
                    tokio::spawn(async move {
                        // Parse route to determine domain
                        let parsed = match parse_route(&route) {
                            Ok(r) => r,
                            Err(e) => {
                                let _ = resp.send(Err(format!("invalid route: {}", e)));
                                return;
                            }
                        };

                        // Create domain context - move route and payload, don't clone
                        let request = DomainContext {
                            route: parsed.clone(),
                            route_str: route,
                            payload,
                            channel_id,
                            route_family,
                        };

                        // Dispatch to domain 
                        let response =
                            match registry_clone.dispatch(parsed.scheme.as_str(), request).await {
                                Ok(resp) => resp,
                                Err(e) => {
                                    let _ = resp.send(Err(e));
                                    return;
                                }
                            };

                        // Convert domain response to PooledFrame and send
                        let result = match response {
                            DomainResponse::Ok => {
                                Ok(crate::protocol::frame::PooledFrame::from_vec(Vec::new()))
                            }
                            DomainResponse::Frame(data) => Ok(data),
                            DomainResponse::Error(e) => Err(e),
                        };
                        let _ = resp.send(result);
                    });
                }

                EngineCommand::CleanupChannel {
                    channel_id,
                    route_family,
                } => {
                    // Clone registry Arc for spawned task
                    let registry_clone = Arc::clone(&registry);
                    
                    // Cleanup spawned as separate task (fire-and-forget)
                    tokio::spawn(async move {
                        registry_clone.cleanup_channel(route_family, channel_id).await;
                    });
                }
            }
        }
    });

    (jh, handle)
}
