use super::McpResourceDetailRequest;
use serde::{Deserialize, Serialize};

/// Resource arguments with an explicit routing/isolation scope independent of realm.
/// The original request type remains available to existing Rust callers.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct McpScopedResourceRequest {
    #[serde(flatten)]
    pub resource: McpResourceDetailRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_family: Option<u64>,
}

impl McpScopedResourceRequest {
    pub(super) fn effective_family(&self) -> Option<u64> {
        self.route_family.or_else(|| {
            (self.resource.scheme == "queue")
                .then_some(self.resource.queue_family)
                .flatten()
        })
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        if self.resource.scheme == "queue"
            && self
                .route_family
                .zip(self.resource.queue_family)
                .is_some_and(|(family, legacy)| family != legacy)
        {
            return Err("route_family and legacy queue_family disagree".to_string());
        }
        Ok(())
    }
}
