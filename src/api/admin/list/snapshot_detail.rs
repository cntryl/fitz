//! REST detail DTOs built from an already selected administrative projection.
use super::{
    detail_views, resource_inventory, troubleshooting, KvResourceDetail, LeaseResourceDetail,
    NoticeResourceDetail, ResourcePath, Runtime, ScheduleResourceDetail, StreamResourceDetail,
};
use crate::control::admin::read_model::AdminSnapshot;
use crate::runtime::DomainKind;
use serde_json::Value;

pub(crate) fn snapshot_detail(
    runtime: &Runtime,
    snapshot: &AdminSnapshot,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    domain: DomainKind,
) -> Result<Value, String> {
    match domain {
        DomainKind::Kv => kv_snapshot_detail(runtime, snapshot, path, family),
        DomainKind::Queue => encode(detail_views::queue_detail_from_rows(
            path,
            family,
            snapshot
                .queues
                .iter()
                .filter(|item| path.matches(&item.realm, &item.area, &item.resource))
                .cloned()
                .collect(),
        )),
        DomainKind::Stream => encode(
            snapshot
                .streams
                .iter()
                .find(|item| path.matches(&item.realm, &item.area, &item.resource))
                .cloned()
                .map_or_else(
                    || StreamResourceDetail::empty(path),
                    StreamResourceDetail::from_stream,
                ),
        ),
        DomainKind::Lease => {
            let (count, oldest, renewals) = snapshot
                .leases
                .iter()
                .filter(|item| path.matches(&item.realm, &item.area, &item.resource))
                .fold((0, 0, 0usize), |(count, oldest, renewals), item| {
                    (
                        count + 1,
                        oldest.max(
                            troubleshooting::age_seconds_since(&item.acquired_at)
                                .unwrap_or_default(),
                        ),
                        renewals.saturating_add(item.renewals),
                    )
                });
            encode(LeaseResourceDetail::from_count(
                path, count, oldest, renewals,
            ))
        }
        DomainKind::Schedule => {
            let schedules = snapshot
                .schedules
                .iter()
                .filter(|item| path.matches(&item.realm, &item.area, &item.resource))
                .cloned()
                .collect::<Vec<_>>();
            encode(if schedules.is_empty() {
                ScheduleResourceDetail::empty(path)
            } else {
                ScheduleResourceDetail::aggregate(path, &schedules)
            })
        }
        DomainKind::Notice => encode(NoticeResourceDetail::from_count(
            path,
            snapshot
                .notice_subscriptions
                .iter()
                .filter(|item| detail_views::matches_resource_route(&item.pattern, path))
                .count(),
        )),
        DomainKind::Rpc => encode(resource_inventory::rpc_operations_from_rows(
            path,
            &snapshot.rpc_workers,
            &snapshot.rpc_pending,
        )),
    }
}

fn kv_snapshot_detail(
    runtime: &Runtime,
    snapshot: &AdminSnapshot,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> Result<Value, String> {
    let transactions = snapshot
        .kv_transactions
        .iter()
        .filter(|item| path.matches(&item.realm, &item.area, &item.resource))
        .count();
    let (families, truncated) = family.map_or_else(
        || {
            runtime
                .admin_auth()
                .bounded_provisioned_route_families(64, None)
        },
        |family| (vec![family], false),
    );
    if truncated {
        return Err("KV detail exceeds the family collection budget; select one family".into());
    }
    let mut entries = Vec::new();
    let mut unavailable = families.is_empty();
    for family in families {
        match runtime.kv_inventory_metadata_resource(family, path.realm, path.area, path.resource) {
            Ok(Some(entry)) => entries.push(entry),
            Ok(None) | Err(_) => unavailable = true,
        }
    }
    // Exact resource reads reuse the persisted metadata contract; no key/value inventory is returned.
    let inventory = entries.into_iter().reduce(|mut total, entry| {
        total.route_family = 0;
        total.estimated_record_count = total
            .estimated_record_count
            .saturating_add(entry.estimated_record_count);
        total.estimated_storage_bytes = total
            .estimated_storage_bytes
            .saturating_add(entry.estimated_storage_bytes);
        total.estimate_complete &= entry.estimate_complete;
        total.read_latency_avg_ms = total.read_latency_avg_ms.max(entry.read_latency_avg_ms);
        total.read_latency_p95_ms = total.read_latency_p95_ms.max(entry.read_latency_p95_ms);
        total.write_latency_avg_ms = total.write_latency_avg_ms.max(entry.write_latency_avg_ms);
        total.write_latency_p95_ms = total.write_latency_p95_ms.max(entry.write_latency_p95_ms);
        total
    });
    let mut value = encode(KvResourceDetail::from_inventory(
        path,
        inventory,
        transactions,
    ))?;
    value["_meta"] = serde_json::json!({ "source": "persisted KV inventory estimates and shared transaction projection", "partial": unavailable, "unavailable": if unavailable { vec!["stored resource estimates absent or unavailable; zero fields do not prove an empty resource", "KV latency histories"] } else { vec!["KV latency histories"] } });
    Ok(value)
}

fn encode(value: impl serde::Serialize) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|error| error.to_string())
}
