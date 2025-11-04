use crate::core::engine::{EngineHandle, SubSender};

/// Notice API: thin wrapper around EngineHandle for publish/subscribe semantics.
#[derive(Clone, Debug)]
pub struct Notice {
    engine: EngineHandle,
}

impl Notice {
    pub fn new(engine: EngineHandle) -> Self {
        Self { engine }
    }

    /// Publish a notice. For notice semantics, id can be any identifier (e.g., uuid), and end=false.
    pub async fn publish(&self, route: String, id: String, body: Vec<u8>) -> Result<(), String> {
        // No reply_to, no seq, no end
        self.engine
            .publish(route, id, body, None, None, false, None)
            .await
    }

    /// Subscribe to a notice route pattern. Returns the subscription id.
    pub async fn subscribe(
        &self,
        route: String,
        sender: SubSender,
        channel_id: u32,
    ) -> Result<u64, String> {
        self.engine.subscribe(route, sender, channel_id).await
    }

    pub async fn unsubscribe(&self, id: u64) -> Result<(), String> {
        self.engine.unsubscribe(id).await
    }

    pub async fn cleanup_channel(&self, channel_id: u32) -> Result<(), String> {
        self.engine.cleanup_channel(channel_id).await
    }
}
