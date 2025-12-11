//! Build and start the complete Fitz actor system.
//!
//! Responsible for:
//! - Spawning all system actors (Router, Metrics, Midge)
//! - Creating realm actors per configured realm
//! - Starting the scheduler and transport layers
//! - Binding listeners and accepting connections

use crate::actor::{ActorRef, ActorSystem, Scheduler};
use crate::messages::{MidgeMsg, RouterMsg, MetricsMsg};
use crate::storage::MidgeActor;
use std::sync::Arc;

/// Builder for the complete Fitz system.
pub struct FitzSystemBuilder {
    system_name: String,
    worker_threads: usize,
    tcp_bind: Option<String>,
    ws_bind: Option<String>,
}

impl FitzSystemBuilder {
    /// Create a new system builder.
    pub fn new() -> Self {
        Self {
            system_name: "fitz".to_string(),
            worker_threads: 4,
            tcp_bind: None,
            ws_bind: None,
        }
    }

    /// Set the system name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.system_name = name.into();
        self
    }

    /// Set the number of worker threads.
    pub fn with_workers(mut self, count: usize) -> Self {
        self.worker_threads = count;
        self
    }

    /// Bind TCP transport.
    pub fn with_tcp(mut self, bind_addr: impl Into<String>) -> Self {
        self.tcp_bind = Some(bind_addr.into());
        self
    }

    /// Bind WebSocket transport.
    pub fn with_websocket(mut self, bind_addr: impl Into<String>) -> Self {
        self.ws_bind = Some(bind_addr.into());
        self
    }

    /// Build and start the system.
    pub fn build(self) -> Result<FitzSystem, String> {
        // Create the actor system
        let actor_system = ActorSystem::new(&self.system_name);
        let scheduler = actor_system.scheduler(self.worker_threads);

        // Spawn global actors
        let midge_actor = scheduler.spawn(MidgeActor::new(), "midge");
        let router_actor = scheduler.spawn(RouterActor::new(), "router");
        let metrics_actor = scheduler.spawn(MetricsActor::new(), "metrics");

        // Create global registry
        let global_actors = GlobalActors {
            midge: midge_actor.clone(),
            router: router_actor.clone(),
            metrics: metrics_actor.clone(),
        };

        Ok(FitzSystem {
            actor_system,
            scheduler,
            global_actors: Arc::new(global_actors),
            tcp_bind: self.tcp_bind,
            ws_bind: self.ws_bind,
        })
    }
}

impl Default for FitzSystemBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The running Fitz system.
pub struct FitzSystem {
    actor_system: ActorSystem,
    scheduler: Scheduler,
    global_actors: Arc<GlobalActors>,
    tcp_bind: Option<String>,
    ws_bind: Option<String>,
}

impl FitzSystem {
    /// Start the system (blocks until shutdown).
    pub fn start(self) -> Result<(), String> {
        println!("🚀 Fitz v2 starting...");
        println!("   System: {}", self.actor_system.name());
        println!("   Global actors:");
        println!("     - Midge:   {}", self.global_actors.midge.name());
        println!("     - Router:  {}", self.global_actors.router.name());
        println!("     - Metrics: {}", self.global_actors.metrics.name());

        if let Some(tcp) = &self.tcp_bind {
            println!("   TCP transport: {}", tcp);
            // TODO: Start TCP transport
        }

        if let Some(ws) = &self.ws_bind {
            println!("   WS transport: {}", ws);
            // TODO: Start WebSocket transport
        }

        println!("✅ Fitz v2 ready");

        // TODO: Start scheduler (blocks)
        self.scheduler.start().map_err(|e| format!("{:?}", e))?;

        Ok(())
    }

    /// Shutdown the system gracefully.
    pub fn shutdown(self) {
        println!("🛑 Fitz v2 shutting down...");
        self.actor_system.shutdown();
    }

    /// Get reference to global actors.
    pub fn global_actors(&self) -> Arc<GlobalActors> {
        self.global_actors.clone()
    }
}

/// References to global singleton actors.
pub struct GlobalActors {
    pub midge: ActorRef<MidgeMsg>,
    pub router: ActorRef<RouterMsg>,
    pub metrics: ActorRef<MetricsMsg>,
}

// Placeholder structs for personas (to be filled in)
struct RouterActor {}
impl RouterActor {
    fn new() -> Self {
        Self {}
    }
}
impl crate::actor::Actor for RouterActor {
    type Message = RouterMsg;
    fn on_message(&mut self, _msg: Self::Message, _ctx: &mut crate::actor::ActorContext<Self::Message>) {
        // TODO: implement router logic
    }
}

struct MetricsActor {}
impl MetricsActor {
    fn new() -> Self {
        Self {}
    }
}
impl crate::actor::Actor for MetricsActor {
    type Message = MetricsMsg;
    fn on_message(&mut self, _msg: Self::Message, _ctx: &mut crate::actor::ActorContext<Self::Message>) {
        // TODO: implement metrics logic
    }
}
