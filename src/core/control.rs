use crate::core::engine::EngineHandle;

/// Control API: admin and introspection helpers over the engine.
#[derive(Clone, Debug)]
pub struct Control {
    engine: EngineHandle,
}

impl Control {
    pub fn new(engine: EngineHandle) -> Self {
        Self { engine }
    }

    pub async fn list_resources(&self, route_prefix: String) -> Result<Vec<String>, String> {
        self.engine.list_resources(route_prefix).await
    }

    pub async fn list_areas(&self) -> Result<Vec<String>, String> {
        self.engine.list_areas().await
    }

    pub async fn fetch_status(&self) -> Result<String, String> {
        self.engine.fetch_status().await
    }

    pub async fn fetch_resource_status(&self, resource: String) -> Result<String, String> {
        self.engine.fetch_resource_status(resource).await
    }
}
