use super::{
    collect_distinct_entries, collect_resource_refs, matches_family, parse_flexible_route,
    parse_rpc_operation, AreaCollection, AreaEntry, Infallible, IntoResourceRef, KvByteValue,
    KvCommittedPair, KvCommittedValueResponse, KvPrefixScanResponse, KvRowsResponse,
    KvTransactionsList, LeaseResourceCollection, LeaseResourceEntry, NoticeResourceCollection,
    NoticeResourceEntry, OperationCollection, OperationEntry, RealmCollection, RealmEntry,
    ResourceCollection, ResourceEntry, ResourcePath, ResourceRef, Response, RpcResourceCollection,
    RpcResourceEntry, Runtime, ScheduleResourceCollection, ScheduleResourceEntry, SessionsList,
    StreamResourceCollection, StreamResourceEntry,
};
use crate::api::admin::troubleshooting;
use crate::domains::kv::sink::AdminKvRowsRequest;
use crate::runtime::routing::RouteFamily;
use base64::Engine;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn kv_storage_error_response(error: &str) -> Response {
    let status = if error.to_ascii_lowercase().contains("routefamily")
        || error.to_ascii_lowercase().contains("route family")
    {
        hyper::StatusCode::BAD_REQUEST
    } else {
        hyper::StatusCode::SERVICE_UNAVAILABLE
    };
    crate::api::admin::error_response(status, error)
}

pub(crate) fn kv_byte_value(bytes: &[u8]) -> KvByteValue {
    KvByteValue {
        base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        utf8: std::str::from_utf8(bytes).ok().map(ToString::to_string),
        len_bytes: bytes.len(),
    }
}

#[must_use]
pub fn collect_realms(resources: &[ResourceRef]) -> RealmCollection {
    RealmCollection {
        realms: collect_distinct_entries(
            resources.iter().map(|item| item.realm.clone()),
            |realm| RealmEntry { realm },
        ),
    }
}

#[must_use]
pub fn collect_areas(resources: &[ResourceRef], realm: &str) -> AreaCollection {
    AreaCollection {
        realm: realm.to_string(),
        areas: collect_distinct_entries(
            resources
                .iter()
                .filter(|item| item.realm == realm)
                .map(|item| item.area.clone()),
            |area| AreaEntry { area },
        ),
    }
}

pub fn collect_resources(resources: &[ResourceRef], realm: &str, area: &str) -> ResourceCollection {
    ResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources: collect_distinct_entries(
            resources
                .iter()
                .filter(|item| item.realm == realm && item.area == area)
                .map(|item| item.resource.clone()),
            ResourceEntry::named,
        ),
    }
}

#[must_use]
pub fn collect_stream_resources(
    runtime: &Runtime,
    realm: &str,
    area: &str,
    family: Option<u64>,
) -> StreamResourceCollection {
    let mut resources = BTreeMap::<String, StreamResourceEntry>::new();
    for stream in runtime
        .stream_list_streams(Some(realm))
        .into_iter()
        .filter(|stream| matches_family(family, stream.route_family) && stream.area == area)
    {
        let entry =
            resources
                .entry(stream.resource.clone())
                .or_insert_with(|| StreamResourceEntry {
                    resource: stream.resource,
                    committed_event_count: 0,
                    size_bytes: 0,
                    sessions_active: 0,
                });
        entry.committed_event_count = entry
            .committed_event_count
            .saturating_add(stream.offset.saturating_add(1));
        entry.size_bytes = entry.size_bytes.saturating_add(stream.size_bytes);
        entry.sessions_active = entry.sessions_active.saturating_add(stream.sessions_active);
    }
    StreamResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources: resources.into_values().collect(),
    }
}

#[must_use]
pub fn collect_lease_resources(
    runtime: &Runtime,
    realm: &str,
    area: &str,
    family: Option<u64>,
) -> LeaseResourceCollection {
    let mut resources = BTreeMap::<String, LeaseResourceEntry>::new();
    for lease in runtime
        .lease_list_leases(Some(realm))
        .into_iter()
        .filter(|lease| matches_family(family, lease.route_family) && lease.area == area)
    {
        let entry = resources
            .entry(lease.resource.clone())
            .or_insert_with(|| LeaseResourceEntry {
                resource: lease.resource,
                active_leases: 0,
                waiters: 0,
                oldest_lease_age_seconds: 0,
            });
        entry.active_leases = entry.active_leases.saturating_add(1);
        entry.oldest_lease_age_seconds = entry
            .oldest_lease_age_seconds
            .max(troubleshooting::age_seconds_since(&lease.acquired_at).unwrap_or(0));
    }
    for waiter in runtime.lease_list_waiters().into_iter().filter(|waiter| {
        matches_family(family, waiter.route_family) && waiter.realm == realm && waiter.area == area
    }) {
        let entry =
            resources
                .entry(waiter.resource.clone())
                .or_insert_with(|| LeaseResourceEntry {
                    resource: waiter.resource,
                    active_leases: 0,
                    waiters: 0,
                    oldest_lease_age_seconds: 0,
                });
        entry.waiters = entry.waiters.saturating_add(1);
    }
    LeaseResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources: resources.into_values().collect(),
    }
}

#[must_use]
pub fn collect_notice_resources(
    runtime: &Runtime,
    realm: &str,
    area: &str,
    family: Option<u64>,
) -> NoticeResourceCollection {
    let mut resources = BTreeMap::<String, NoticeResourceEntry>::new();
    for subscription in runtime
        .notice_list_subscriptions(Some(realm), None)
        .into_iter()
        .filter(|subscription| matches_family(family, subscription.route_family))
    {
        let Some(path) = parse_flexible_route(&subscription.pattern) else {
            continue;
        };
        if path.realm != realm || path.area != area {
            continue;
        }
        let entry = resources
            .entry(path.resource.clone())
            .or_insert_with(|| NoticeResourceEntry {
                resource: path.resource,
                subscriptions_active: 0,
                notifications_received: 0,
                publishes_per_minute: 0.0,
            });
        entry.subscriptions_active = entry.subscriptions_active.saturating_add(1);
        entry.notifications_received = entry
            .notifications_received
            .saturating_add(subscription.notifications_received);
    }
    for route in runtime
        .notice_list_routes(Some(realm))
        .into_iter()
        .filter(|route| matches_family(family, route.route_family))
    {
        let Some(path) = parse_flexible_route(&route.route) else {
            continue;
        };
        if path.realm != realm || path.area != area {
            continue;
        }
        let entry = resources
            .entry(path.resource.clone())
            .or_insert_with(|| NoticeResourceEntry {
                resource: path.resource,
                subscriptions_active: 0,
                notifications_received: 0,
                publishes_per_minute: 0.0,
            });
        entry.publishes_per_minute += route.publishes_per_minute;
    }
    NoticeResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources: resources.into_values().collect(),
    }
}

#[must_use]
pub fn collect_rpc_resources(
    runtime: &Runtime,
    realm: &str,
    area: &str,
    family: Option<u64>,
) -> RpcResourceCollection {
    let mut resources = BTreeMap::<String, RpcResourceEntry>::new();
    for worker in runtime
        .rpc_list_workers(Some(realm))
        .into_iter()
        .filter(|worker| matches_family(family, worker.route_family))
    {
        let Some(operation) = parse_rpc_operation(&worker.route) else {
            continue;
        };
        if operation.realm != realm || operation.area != area {
            continue;
        }
        let entry = resources
            .entry(operation.resource.clone())
            .or_insert_with(|| RpcResourceEntry {
                resource: operation.resource,
                workers_registered: 0,
                requests_pending: 0,
                slowest_worker_average_latency_ms: None,
            });
        entry.workers_registered = entry.workers_registered.saturating_add(1);
        if worker.requests_handled > 0 {
            entry.slowest_worker_average_latency_ms = Some(
                entry
                    .slowest_worker_average_latency_ms
                    .map_or(worker.average_latency_ms, |latency| {
                        latency.max(worker.average_latency_ms)
                    }),
            );
        }
    }
    for pending in runtime
        .rpc_list_pending(Some(realm))
        .into_iter()
        .filter(|pending| matches_family(family, pending.route_family))
    {
        let Some(operation) = parse_rpc_operation(&pending.route) else {
            continue;
        };
        if operation.realm != realm || operation.area != area {
            continue;
        }
        let entry = resources
            .entry(operation.resource.clone())
            .or_insert_with(|| RpcResourceEntry {
                resource: operation.resource,
                workers_registered: 0,
                requests_pending: 0,
                slowest_worker_average_latency_ms: None,
            });
        entry.requests_pending = entry.requests_pending.saturating_add(1);
    }
    RpcResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources: resources.into_values().collect(),
    }
}

fn schedule_families(
    runtime: &Runtime,
    family: Option<u64>,
    schedule_rows: &[crate::control::admin::ScheduleInfo],
) -> Vec<u64> {
    family.map_or_else(
        || {
            let mut families = runtime
                .admin_auth()
                .provisioned_route_families()
                .into_iter()
                .filter_map(|value| value.parse().ok())
                .collect::<BTreeSet<_>>();
            families.extend(schedule_rows.iter().map(|schedule| schedule.route_family));
            families.into_iter().collect()
        },
        |family| vec![family],
    )
}

#[must_use]
pub fn collect_schedule_resources(
    runtime: &Runtime,
    realm: &str,
    area: &str,
    family: Option<u64>,
) -> ScheduleResourceCollection {
    let mut resources = BTreeMap::<String, ScheduleResourceEntry>::new();
    let schedules = runtime
        .schedule_list_schedules(Some(realm))
        .into_iter()
        .filter(|schedule| matches_family(family, schedule.route_family) && schedule.area == area)
        .collect::<Vec<_>>();
    for schedule in &schedules {
        let entry = resources
            .entry(schedule.resource.clone())
            .or_insert_with(|| ScheduleResourceEntry {
                resource: schedule.resource.clone(),
                schedules_active: 0,
                pending_claims: 0,
                next_run: None,
            });
        if schedule.enabled {
            entry.schedules_active = entry.schedules_active.saturating_add(1);
            if entry
                .next_run
                .as_ref()
                .is_none_or(|next| schedule.next_run < *next)
            {
                entry.next_run = Some(schedule.next_run.clone());
            }
        }
    }
    for family in schedule_families(runtime, family, &schedules) {
        let Ok(route_family) = RouteFamily::try_from(family) else {
            continue;
        };
        for claim in runtime.schedule_list_pending_claims(route_family) {
            let Some(operation) = parse_rpc_operation(&claim.route) else {
                continue;
            };
            if operation.realm != realm || operation.area != area {
                continue;
            }
            let entry = resources
                .entry(operation.resource.clone())
                .or_insert_with(|| ScheduleResourceEntry {
                    resource: operation.resource,
                    schedules_active: 0,
                    pending_claims: 0,
                    next_run: None,
                });
            entry.pending_claims = entry.pending_claims.saturating_add(1);
        }
    }
    ScheduleResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources: resources.into_values().collect(),
    }
}

pub fn kv_resources(runtime: &Runtime, family: Option<u64>) -> Vec<ResourceRef> {
    let mut resources = runtime
        .kv_inventory_entries(family)
        .unwrap_or_default()
        .into_iter()
        .map(IntoResourceRef::into_resource_ref)
        .collect::<Vec<_>>();
    resources.extend(collect_resource_refs(
        runtime
            .kv_list_transactions(None)
            .into_iter()
            .filter(|item| matches_family(family, item.route_family)),
    ));
    resources
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[must_use]
pub fn collect_kv_resources(
    runtime: &Runtime,
    realm: &str,
    area: &str,
    family: Option<u64>,
) -> ResourceCollection {
    let mut resources = runtime
        .kv_inventory_entries(family)
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| entry.realm == realm && entry.area == area)
        .map(|entry| {
            (
                entry.resource.clone(),
                ResourceEntry::from_kv_inventory(entry),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut active_counts = BTreeMap::<String, usize>::new();
    for tx in runtime
        .kv_list_transactions(Some(realm))
        .into_iter()
        .filter(|tx| matches_family(family, tx.route_family) && tx.area == area)
    {
        *active_counts.entry(tx.resource).or_default() += 1;
    }

    for (resource, transactions_active) in active_counts {
        resources
            .entry(resource.clone())
            .and_modify(|entry| entry.transactions_active = Some(transactions_active))
            .or_insert_with(|| {
                let mut entry = ResourceEntry::named(resource);
                entry.estimated_record_count = Some(0);
                entry.estimated_storage_bytes = Some(0);
                entry.estimate_complete = Some(true);
                entry.read_latency_avg_ms = Some(0.0);
                entry.read_latency_p95_ms = Some(0.0);
                entry.write_latency_avg_ms = Some(0.0);
                entry.write_latency_p95_ms = Some(0.0);
                entry.transactions_active = Some(transactions_active);
                entry
            });
    }

    ResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources: resources.into_values().collect(),
    }
}

#[must_use]
pub fn queue_resources(runtime: &Runtime, family: Option<u64>) -> Vec<ResourceRef> {
    collect_resource_refs(
        runtime
            .queue_list_queues(None)
            .into_iter()
            .filter(|item| matches_family(family, item.family)),
    )
}

#[must_use]
pub fn stream_resources(runtime: &Runtime, family: Option<u64>) -> Vec<ResourceRef> {
    collect_resource_refs(
        runtime
            .stream_list_streams(None)
            .into_iter()
            .filter(|item| matches_family(family, item.route_family)),
    )
}

#[must_use]
pub fn lease_resources(runtime: &Runtime, family: Option<u64>) -> Vec<ResourceRef> {
    collect_resource_refs(
        runtime
            .lease_list_leases(None)
            .into_iter()
            .filter(|item| matches_family(family, item.route_family)),
    )
}

#[must_use]
pub fn schedule_resources(runtime: &Runtime, family: Option<u64>) -> Vec<ResourceRef> {
    collect_resource_refs(
        runtime
            .schedule_list_schedules(None)
            .into_iter()
            .filter(|item| matches_family(family, item.route_family)),
    )
}

#[must_use]
pub fn notice_resources(runtime: &Runtime, family: Option<u64>) -> Vec<ResourceRef> {
    runtime
        .notice_list_subscriptions(None, None)
        .into_iter()
        .filter(|item| matches_family(family, item.route_family))
        .filter_map(|item| parse_flexible_route(&item.pattern))
        .collect()
}

#[must_use]
pub fn rpc_resources(runtime: &Runtime, family: Option<u64>) -> Vec<ResourceRef> {
    runtime
        .rpc_list_workers(None)
        .into_iter()
        .filter(|item| matches_family(family, item.route_family))
        .filter_map(|item| parse_flexible_route(&item.route))
        .collect()
}

#[must_use]
pub fn rpc_operations(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> OperationCollection {
    let operations = collect_distinct_entries(
        runtime
            .rpc_list_workers(None)
            .into_iter()
            .filter(|worker| matches_family(family, worker.route_family))
            .filter_map(|worker| parse_rpc_operation(&worker.route))
            .filter(|operation| operation.matches_resource_path(path))
            .map(|operation| operation.operation),
        |operation| OperationEntry { operation },
    );

    OperationCollection {
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        operations,
    }
}

/// Returns live admin session snapshots.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
///
/// # Panics
///
/// Panics if called without prior HTTP-boundary validation of `family`.
#[must_use]
pub fn list_sessions(runtime: &Runtime) -> Response {
    let sessions = runtime.list_sessions();
    crate::api::admin::json_response(SessionsList { sessions })
}

/// Returns live admin session snapshots for one authorized route family.
#[must_use]
pub fn list_sessions_for_family(runtime: &Runtime, family: u64) -> Response {
    let sessions = runtime
        .list_sessions()
        .into_iter()
        .filter(|session| session.route_family == family)
        .collect();
    crate::api::admin::json_response(SessionsList { sessions })
}

/// Returns active KV transactions for a specific resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
///
/// # Panics
///
/// Panics if called without prior HTTP-boundary validation of `family`.
#[must_use]
pub fn kv_transactions_for_resource(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> Response {
    let transactions = runtime
        .kv_list_transactions(Some(path.realm))
        .into_iter()
        .filter(|tx| {
            matches_family(family, tx.route_family)
                && path.matches(&tx.realm, &tx.area, &tx.resource)
        })
        .collect();
    crate::api::admin::json_response(KvTransactionsList { transactions })
}

/// Returns the committed KV value for a specific resource key.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
///
/// # Panics
///
/// Panics if called without prior HTTP-boundary validation of `family`.
pub fn kv_committed_value_for_resource(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: u64,
    key: &[u8],
) -> Result<Response, Infallible> {
    match runtime.kv_get_committed_value(
        crate::runtime::routing::RouteFamily::try_from(family)
            .expect("admin route family is validated at the HTTP boundary"),
        path.realm,
        path.area,
        path.resource,
        key,
    ) {
        Ok(value) => Ok(crate::api::admin::json_response(KvCommittedValueResponse {
            route_family: family,
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            key: kv_byte_value(key),
            found: value.is_some(),
            value: value.as_deref().map(kv_byte_value),
        })),
        Err(error) => Ok(kv_storage_error_response(&error)),
    }
}

/// Returns a committed KV prefix scan for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
///
/// # Panics
///
/// Panics if called without prior HTTP-boundary validation of `family`.
#[must_use]
pub fn kv_prefix_scan_for_resource(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: u64,
    prefix: &[u8],
    limit: usize,
) -> Result<Response, Infallible> {
    match runtime.kv_scan_committed_prefix(
        crate::runtime::routing::RouteFamily::try_from(family)
            .expect("admin route family is validated at the HTTP boundary"),
        path.realm,
        path.area,
        path.resource,
        prefix,
        limit,
    ) {
        Ok((items, has_more)) => Ok(crate::api::admin::json_response(KvPrefixScanResponse {
            route_family: family,
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            prefix: kv_byte_value(prefix),
            limit,
            has_more,
            items: items
                .into_iter()
                .map(|(key, value)| KvCommittedPair {
                    key: kv_byte_value(&key),
                    value: kv_byte_value(&value),
                })
                .collect(),
        })),
        Err(error) => Ok(kv_storage_error_response(&error)),
    }
}

/// Returns committed KV rows for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
///
/// # Panics
///
/// Panics if called without prior HTTP-boundary validation of `family`.
#[must_use]
pub fn kv_rows_for_resource(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: u64,
    starts_with: &[u8],
    cursor: Option<&[u8]>,
    limit: usize,
) -> Result<Response, Infallible> {
    match runtime.kv_scan_committed_rows(&AdminKvRowsRequest {
        route_family: crate::runtime::routing::RouteFamily::try_from(family)
            .expect("admin route family is validated at the HTTP boundary"),
        realm: path.realm,
        area: path.area,
        resource: path.resource,
        starts_with,
        cursor,
        limit,
    }) {
        Ok((items, next_cursor, has_more)) => {
            Ok(crate::api::admin::json_response(KvRowsResponse {
                route_family: family,
                realm: path.realm.to_string(),
                area: path.area.to_string(),
                resource: path.resource.to_string(),
                starts_with: kv_byte_value(starts_with),
                limit,
                next_cursor: next_cursor
                    .map(|cursor| base64::engine::general_purpose::STANDARD.encode(cursor)),
                has_more,
                items: items
                    .into_iter()
                    .map(|(key, value)| KvCommittedPair {
                        key: kv_byte_value(&key),
                        value: kv_byte_value(&value),
                    })
                    .collect(),
            }))
        }
        Err(error) => Ok(kv_storage_error_response(&error)),
    }
}
