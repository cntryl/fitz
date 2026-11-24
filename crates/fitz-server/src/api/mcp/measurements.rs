//! Explicit acceptance measurements; elapsed time is evidence, not a preemption guarantee.
use super::*;
use crate::api::admin::auth::AdminRouteFamilyAccess;
use crate::auth::default_anonymous_permissions;
use crate::control::admin::{read_model::AdminReadModel, RpcWorker};
use std::sync::Arc;
use std::time::Instant;

#[test]
#[ignore = "explicit local budget measurement"]
fn should_measure_bounded_operational_reads_without_promising_hard_preemption() {
    // Arrange
    let model = AdminReadModel::new();
    model.replace_rpc_workers(
        (0..500)
            .map(|id| {
                RpcWorker::snapshot(
                    1,
                    id,
                    "acme",
                    &format!("rpc://acme/jobs/job-{id}/run"),
                    "2026-10-04T00:00:00Z",
                    0,
                    0.0,
                )
            })
            .collect(),
    );
    let runtime = Runtime::with_admin_read_model(Arc::new(crate::runtime::Router::new()), model);
    let registry = McpToolRegistry::read_only();
    let context = McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "measurement".into(),
            route_family_access: AdminRouteFamilyAccess::wildcard(),
        },
        default_anonymous_permissions(),
    );
    let resource = serde_json::json!({"route_family":1,"scheme":"rpc","realm":"acme","area":"jobs","resource":"job-0"});
    let inventory = serde_json::json!({"route_family":1,"scheme":"rpc","realm":"acme","area":"jobs","limit":100});
    let family = serde_json::json!({"route_family":1});
    let mut measurements = Vec::new();

    // Act
    for descriptor in registry.tool_descriptors() {
        let arguments = match descriptor.name.as_str() {
            "inspect_resource_detail" | "inspect_resource_timeline" => Some(&resource),
            "list_resource_inventory" => Some(&inventory),
            "get_sessions" | "get_topology" | "get_structured_metrics" => Some(&family),
            _ => None,
        };
        let mut samples = Vec::new();
        let mut bytes = 0;
        let mut rejected = 0;
        for _ in 0..20 {
            let start = Instant::now();
            let result = registry.execute(
                &descriptor.name,
                &runtime,
                &context,
                &McpCapabilityPolicy::read_only(),
                arguments,
            );
            samples.push(start.elapsed().as_micros());
            match result {
                Ok(value) => {
                    let size = serde_json::to_vec(&value).unwrap().len();
                    assert!(size <= descriptor.budget.max_result_bytes);
                    bytes = bytes.max(size);
                }
                Err(
                    McpToolError::BudgetExceeded { .. } | McpToolError::ResultItemsExceeded { .. },
                ) => rejected += 1,
                Err(error) => panic!("unexpected measurement failure: {error:?}"),
            }
        }
        samples.sort_unstable();
        measurements.push((
            descriptor.name,
            samples[9],
            samples[18],
            samples[19],
            bytes,
            rejected,
        ));
    }

    // Assert
    assert_eq!(measurements.len(), 10);
    for (tool, p50, p95, maximum, bytes, rejected) in measurements {
        println!("mcp_measurement tool={tool} runs=20 p50_us={p50} p95_us={p95} max_us={maximum} max_result_bytes={bytes} budget_rejections={rejected}");
    }
}
