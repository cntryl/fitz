use crate::core::engine::EngineHandle;
use crate::storage::mem::{QueueConfig, QueueScope};

/// Queue API: durable-ish queue semantics over the engine + store.
#[derive(Clone, Debug)]
pub struct Queue {
    engine: EngineHandle,
}

impl Queue {
    pub fn new(engine: EngineHandle) -> Self {
        Self { engine }
    }

    /// Produce (append) a message to a queue route. Provide an id for dedupe/idempotency if desired.
    pub async fn publish(&self, route: String, id: String, body: Vec<u8>) -> Result<(), String> {
        // No reply_to, no seq/end, TTL not used here (store may apply TTL via config)
        self.engine
            .publish(route, id, body, None, None, false, None)
            .await
    }

    pub async fn reserve(
        &self,
        route: String,
        lease_secs: u32,
    ) -> Result<(String, Vec<u8>, String), String> {
        self.engine.reserve(route, lease_secs).await
    }

    pub async fn extend_lease(
        &self,
        route: String,
        id: String,
        token: String,
        add_secs: u32,
    ) -> Result<u32, String> {
        self.engine.extend_lease(route, id, token, add_secs).await
    }

    pub async fn consume(&self, route: String, id: String, token: String) -> Result<(), String> {
        self.engine.consume(route, id, token).await
    }

    pub async fn peek(&self, route: String) -> Result<Option<(String, Vec<u8>)>, String> {
        self.engine.peek(route).await
    }

    /// Configure queues via hierarchical scopes.
    pub async fn set_config(&self, scope: QueueScope, cfg: QueueConfig) -> Result<(), String> {
        self.engine.set_queue_config(scope, cfg).await
    }
}
