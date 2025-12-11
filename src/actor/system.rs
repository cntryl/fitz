//! System-level actor and supervision.

use super::scheduler::Scheduler;
use std::sync::Arc;

/// Global actor system.
#[derive(Clone)]
pub struct ActorSystem {
    name: Arc<String>,
}

impl ActorSystem {
    /// Create a new actor system.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Arc::new(name.into()),
        }
    }

    /// Get the system name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Create a scheduler for this system.
    pub fn scheduler(&self, worker_count: usize) -> Scheduler {
        Scheduler::new(format!("{}-scheduler", self.name), worker_count)
    }

    /// Shutdown the actor system.
    pub fn shutdown(&self) {
        // TODO: coordinate graceful shutdown of all actors
    }
}

