//! Bounded cursor retention for paginated metadata. Cursors contain no readable resource names.
use super::{
    McpCapabilityClass, McpCostBudget, McpExecutionContext, McpInvocation, McpToolDefinition,
    McpToolDescriptor, McpToolError, McpToolRegistry, McpToolResult,
};
use crate::auth::Access;
use crate::boot::Runtime;
use crate::control::admin::read_model::{InventoryEntry, InventoryScope};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct InventoryRequest {
    pub route_family: Option<u64>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub realm: Option<String>,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub scheme: Option<String>,
}

struct Cursor {
    owner: String,
    request: InventoryRequest,
    after: InventoryEntry,
    created: Instant,
}
static CURSORS: OnceLock<Mutex<HashMap<String, Cursor>>> = OnceLock::new();
const MAX_CURSORS: usize = 1024;
const CURSOR_TTL: Duration = Duration::from_secs(300);

impl McpToolRegistry {
    pub(super) fn inventory_tool() -> McpToolDefinition {
        McpToolDefinition::new(McpToolDescriptor {
            name: "list_resource_inventory".into(), capability: McpCapabilityClass::Inspect,
            summary: "Page authorized realm/area/resource names from shared projections; does not read payloads".into(),
            rest_path: "/api/v1/:route_family/:scheme/realms".into(), budget: McpCostBudget::collection(),
        }, inventory)
    }
}

fn error(reason: &str) -> McpToolError {
    McpToolError::InvalidArguments {
        tool_name: "list_resource_inventory".into(),
        reason: reason.into(),
    }
}

fn inventory(
    runtime: &Runtime,
    invocation: &McpInvocation,
    context: &McpExecutionContext,
) -> McpToolResult<Value> {
    let McpInvocation::Inventory(request) = invocation else {
        return Err(error("missing inventory arguments"));
    };
    let mut bound_request = request.clone();
    bound_request.cursor = None;
    let owner = format!(
        "{}:{:?}:{}",
        context.principal_name().unwrap_or_default(),
        context
            .principal
            .as_ref()
            .map(|principal| &principal.route_family_access),
        context.permissions.authority_fingerprint()
    );
    let after = if let Some(token) = &request.cursor {
        let cache = CURSORS.get_or_init(Default::default).lock();
        let cursor = cache
            .get(token)
            .ok_or_else(|| error("unknown or expired inventory cursor"))?;
        if cursor.created.elapsed() > CURSOR_TTL
            || cursor.owner != owner
            || cursor.request != bound_request
        {
            return Err(error(
                "inventory cursor does not match this authority and query",
            ));
        }
        Some(cursor.after.clone())
    } else {
        None
    };
    let limit = request.limit.unwrap_or(100).clamp(1, 256);
    let mut stored_estimates = Vec::new();
    let persisted_kv = request.scheme.as_deref() == Some("kv")
        && request.route_family.is_some()
        && request.realm.is_some();
    let (scanned, has_more) = if persisted_kv {
        let (entries, has_more) = if let Some(resource) = request.resource.as_deref() {
            let entry = runtime
                .kv_inventory_metadata_resource(
                    request.route_family.expect("checked family"),
                    request.realm.as_deref().expect("checked realm"),
                    request.area.as_deref().expect("validated area"),
                    resource,
                )
                .map_err(|_| error("persisted KV metadata is unavailable"))?;
            (entry.into_iter().collect(), false)
        } else {
            runtime
                .kv_inventory_metadata_page(
                    request.route_family.expect("checked family"),
                    request.realm.as_deref().expect("checked realm"),
                    request.area.as_deref(),
                    after
                        .as_ref()
                        .map(|entry| (entry.area.as_str(), entry.resource.as_str())),
                    limit,
                )
                .map_err(|_| error("persisted KV metadata is unavailable"))?
        };
        let names = entries
            .iter()
            .map(|entry| InventoryEntry {
                route_family: entry.route_family,
                realm: entry.realm.clone(),
                area: entry.area.clone(),
                resource: entry.resource.clone(),
                scheme: "kv".into(),
            })
            .collect();
        stored_estimates = entries;
        (names, has_more)
    } else {
        runtime.admin_read_model().inventory_page(
            InventoryScope {
                family: request.route_family,
                realm: request.realm.as_deref(),
                area: request.area.as_deref(),
                resource: request.resource.as_deref(),
                scheme: request.scheme.as_deref(),
            },
            after.as_ref(),
            limit,
        )
    };
    let last = scanned.last().cloned();
    let items = scanned
        .into_iter()
        .filter(|entry| {
            let route = format!(
                "{}://{}/{}/{}",
                entry.scheme, entry.realm, entry.area, entry.resource
            );
            let permission_route = if entry.scheme == "rpc" {
                format!("{route}/*")
            } else {
                route
            };
            context.principal.as_ref().is_some_and(|principal| {
                principal
                    .route_family_access
                    .allows(&entry.route_family.to_string())
            }) && context
                .permissions
                .allows_route(&permission_route, Access::Read)
                && request
                    .realm
                    .as_ref()
                    .is_none_or(|realm| *realm == entry.realm)
                && request.area.as_ref().is_none_or(|area| *area == entry.area)
                && request
                    .resource
                    .as_ref()
                    .is_none_or(|resource| *resource == entry.resource)
                && request
                    .scheme
                    .as_ref()
                    .is_none_or(|scheme| *scheme == entry.scheme)
        })
        .map(|entry| {
            let mut value = serde_json::to_value(&entry).expect("inventory name serializes");
            if let Some(estimate) = stored_estimates
                .iter()
                .find(|estimate| estimate.area == entry.area && estimate.resource == entry.resource)
            {
                value["estimated_record_count"] =
                    serde_json::json!(estimate.estimated_record_count);
                value["estimated_storage_bytes"] =
                    serde_json::json!(estimate.estimated_storage_bytes);
                value["estimate_complete"] = serde_json::json!(estimate.estimate_complete);
            }
            value
        })
        .collect::<Vec<_>>();
    let next_cursor = if has_more {
        if let Some(after) = last {
            let mut random = [0u8; 16];
            getrandom::fill(&mut random).map_err(|_| error("cursor generation unavailable"))?;
            let token = hex::encode(random);
            let mut cache = CURSORS.get_or_init(Default::default).lock();
            cache.retain(|_, cursor| cursor.created.elapsed() <= CURSOR_TTL);
            if cache.len() >= MAX_CURSORS {
                return Err(error("inventory cursor capacity exhausted"));
            }
            cache.insert(
                token.clone(),
                Cursor {
                    owner,
                    request: bound_request,
                    after,
                    created: Instant::now(),
                },
            );
            Some(token)
        } else {
            None
        }
    } else {
        None
    };
    Ok(
        serde_json::json!({ "route_family": request.route_family, "items": items, "next_cursor": next_cursor, "has_more": has_more, "limit": limit,
            "_meta": { "source": if persisted_kv { "persisted KV inventory estimates; metadata-only read" } else { "shared admin projections" }, "cached_projection": !persisted_kv, "partial": has_more, "unavailable": if persisted_kv { vec!["live transaction counters and latency histories use unknown/default placeholders; payloads are not returned"] } else { vec!["inactive KV resources outside current projections; select scheme kv and a known realm for persisted metadata", "payloads and durable event history"] }, "cursor_ttl_seconds": 300, "cursor_capacity": MAX_CURSORS, "scan_limit_per_projection": limit + 2 }
        }),
    )
}

#[cfg(test)]
mod tests;
