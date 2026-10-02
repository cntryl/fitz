//! Typed catalog for the implemented registry surface.
//!
//! Metadata describes current enforcement; candidate runtime/item budgets are
//! not advertised as hard execution limits before the transport enforces them.

use super::{McpScopedResourceRequest, McpToolDescriptor, McpToolRegistry};
use crate::api::admin::troubleshooting::{GlobalTroubleshootingDiagnostics, ResourceTimeline};
use crate::api::admin::{
    KvResourceDetail, LeaseResourceDetail, NoticeResourceDetail, OperationCollection,
    QueueResourceDetail, ScheduleResourceDetail, StreamResourceDetail,
};
use rmcp::model::{ProtocolVersion, Tool, ToolAnnotations};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Primary protocol revision; requests use the SDK's stateless lifecycle.
#[must_use]
pub const fn primary_revision() -> ProtocolVersion {
    ProtocolVersion::V_2026_07_28
}

/// Explicitly supported compatibility revision with initialization.
#[must_use]
pub const fn compatibility_revision() -> ProtocolVersion {
    ProtocolVersion::V_2025_11_25
}

/// Whether the revision is supported by the Fitz endpoint contract.
#[must_use]
pub fn supports_revision(revision: &ProtocolVersion) -> bool {
    *revision == primary_revision() || *revision == compatibility_revision()
}

/// Resource-detail responses reuse the REST types for all seven domains.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum ResourceDetailOutput {
    Kv(KvResourceDetail),
    Queue(QueueResourceDetail),
    Stream(StreamResourceDetail),
    Lease(LeaseResourceDetail),
    Schedule(ScheduleResourceDetail),
    Notice(NoticeResourceDetail),
    Rpc(OperationCollection),
}

#[derive(schemars::JsonSchema)]
struct EmptyArguments {}

impl McpToolRegistry {
    /// Returns a stable MCP catalog with schemas derived from REST read models.
    ///
    /// Listing a descriptor grants no authority to invoke its tool. The registry
    /// still authenticates and authorizes every execution before collecting facts.
    #[must_use]
    pub fn protocol_tools(&self) -> Vec<Tool> {
        let mut descriptors = self.tool_descriptors();
        descriptors.sort_by(|left, right| left.name.cmp(&right.name));
        descriptors.into_iter().map(protocol_tool).collect()
    }
}

fn protocol_tool(descriptor: McpToolDescriptor) -> Tool {
    let resource = descriptor.name.starts_with("inspect_resource_");
    let scope = if resource {
        "resource READ; explicit route_family authority; omitted family requires wildcard authority; queue_family is a legacy Queue-only argument"
    } else {
        "wildcard route READ and wildcard route-family authority"
    };
    let metadata = json!({
        "fitz.catalogVersion": 1,
        "fitz.capability": descriptor.capability,
        "fitz.source": descriptor.rest_path,
        "fitz.authority": scope,
        "fitz.freshness": "current broker read-model snapshot; no durable history implied",
        "fitz.pagination": "single bounded response; no cursor",
        "fitz.budget": descriptor.budget,
        "fitz.enforcement": {
            "resultBytes": true,
            "hardRuntimeDeadline": false,
            "collectionItemLimit": false,
            "timelineResultItems": descriptor.name == "inspect_resource_timeline"
        }
    })
    .as_object()
    .expect("catalog metadata is an object")
    .clone();
    let tool = Tool::new(
        descriptor.name.clone(),
        descriptor.summary,
        serde_json::Map::new(),
    )
    .with_annotations(ToolAnnotations::new().read_only(true).open_world(false))
    .with_meta(rmcp::model::MetaObject(metadata));
    let tool = if resource {
        tool.with_input_schema::<McpScopedResourceRequest>()
    } else {
        tool.with_input_schema::<EmptyArguments>()
    };
    match descriptor.name.as_str() {
        "get_global_stats" => tool.with_output_schema::<crate::api::admin::GlobalStats>(),
        "inspect_resource_detail" => tool.with_output_schema::<ResourceDetailOutput>(),
        "inspect_resource_timeline" => tool.with_output_schema::<ResourceTimeline>(),
        _ => tool.with_output_schema::<GlobalTroubleshootingDiagnostics>(),
    }
}

#[cfg(test)]
mod tests;
