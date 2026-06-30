//! Admin REST API module
//!
//! Provides HTTP endpoints for observability, health checks, and operational control.
//! All endpoints coexist with data plane on same port (path-based routing).

mod assets;
pub mod auth;
pub mod handlers;
mod list;
mod metrics;
mod probes;
mod search;
mod stats;
mod topology;
pub(crate) mod troubleshooting;

pub(crate) use stats::{build_global_stats, build_global_troubleshooting};

pub use handlers::handle_request;
pub use list::{
    collect_areas, collect_kv_resources, collect_queue_areas, collect_queue_realms,
    collect_queue_resources, collect_realms, collect_resources, kv_committed_value_for_resource,
    kv_compare_detail, kv_detail, kv_events_for_resource, kv_prefix_scan_for_resource,
    kv_resources, kv_rows_for_resource, kv_transactions_for_resource, lease_compare_detail,
    lease_detail, lease_events_for_resource, lease_resources, list_sessions, notice_compare_detail,
    notice_delivery_observations, notice_detail, notice_events_for_resource, notice_resources,
    notice_subscriptions_for_resource, parse_admin_record_limit, parse_kv_query_bytes,
    parse_kv_scan_limit, parse_limit_query_param, parse_optional_kv_cursor,
    parse_optional_kv_query_bytes, parse_optional_string_query_param,
    parse_optional_u64_query_param, parse_query_params, queue_area_detail, queue_compare_detail,
    queue_dead_letters_for_resource, queue_detail, queue_events_for_resource,
    queue_inflight_for_resource, queue_realm_detail, queue_resources, rpc_compare_detail,
    rpc_events_for_resource, rpc_operation_detail, rpc_operations, rpc_pending, rpc_resources,
    rpc_workers_for_operation, schedule_compare_detail, schedule_detail,
    schedule_events_for_resource, schedule_executions_for_resource, schedule_resources,
    stream_area_watermark_detail, stream_compare_detail, stream_detail, stream_events_for_resource,
    stream_realm_watermark_detail, stream_records_for_resource, stream_resources, AreaCollection,
    AreaDetail, AreaEntry, KvByteValue, KvCommittedPair, KvCommittedValueResponse,
    KvLatencySnapshot, KvPrefixScanResponse, KvResourceDetail, KvResourceInventoryEntry,
    KvRowsResponse, KvTransaction, KvTransactionsList, LeaseInfo, LeaseResourceDetail,
    LeaseSearchItem, LeaseSearchResponse, LeaseWaiterInfo, LeasesList, NoticeDeliveryObservation,
    NoticeDeliveryObservationList, NoticeResourceDetail, NoticeRouteInfo, NoticeRoutesList,
    NoticeSubscription, NoticeSubscriptionsList, OperationCollection, OperationEntry,
    QueueAgeBuckets, QueueAreaCollection, QueueAreaDetail, QueueAreaEntry, QueueDeadLetter,
    QueueDeadLettersList, QueueInflight, QueueInflightList, QueueInfo, QueueRealmCollection,
    QueueRealmDetail, QueueRealmEntry, QueueResourceCollection, QueueResourceDetail,
    QueueResourceEntry, QueuesList, RealmCollection, RealmDetail, RealmEntry, ResourceCollection,
    ResourceEntry, ResourcePath, ResourceRef, RpcCallObservation, RpcCallObservationList,
    RpcLatencyBuckets, RpcOperationDetail, RpcOperationPath, RpcPendingList, RpcPendingRequest,
    RpcWorker, RpcWorkersList, ScheduleExecutionObservation, ScheduleExecutionObservationList,
    ScheduleInfo, ScheduleLatencyBuckets, ScheduleMissedObservation, ScheduleMissedObservationList,
    SchedulePendingClaimInfo, ScheduleResourceDetail, SchedulesList, SessionInfo, SessionsList,
    StreamAdminRecord, StreamAreaWatermark, StreamAreaWatermarkDetail, StreamInfo,
    StreamLagBuckets, StreamLatencyBuckets, StreamRealmWatermark, StreamRealmWatermarkDetail,
    StreamRecordsResponse, StreamResourceDetail, StreamsList,
};
pub use probes::{HealthStatus, ReadyStatus, StartupStatus, TargetStatus};

use crate::api::http::{Body, Response};
use hyper::header::HeaderValue;
use hyper::StatusCode;
use serde::Serialize;

const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";
const HSTS_MAX_AGE: &str = "max-age=31536000";

pub(crate) fn with_browser_security_headers(
    mut response: Response,
    assume_external_tls: bool,
) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("Referrer-Policy", HeaderValue::from_static("no-referrer"));
    headers.insert(
        "Content-Security-Policy",
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    if assume_external_tls {
        headers.insert(
            "Strict-Transport-Security",
            HeaderValue::from_static(HSTS_MAX_AGE),
        );
    }
    response
}

/// Helper to create JSON responses.
pub(crate) fn json_response<T: Serialize>(data: T) -> Response {
    json_response_with_status(StatusCode::OK, data)
}

pub(crate) fn json_response_with_status<T: Serialize>(status: StatusCode, data: T) -> Response {
    match serde_json::to_string(&data) {
        Ok(json) => hyper::http::Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::from(json))
            .unwrap(),
        Err(_) => hyper::http::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("Failed to serialize response"))
            .unwrap(),
    }
}

pub(crate) fn error_response(status: StatusCode, message: &str) -> Response {
    hyper::http::Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(format!(r#"{{"error":"{message}"}}"#)))
        .unwrap()
}

/// Helper to create not found response
pub(crate) fn not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "Not Found")
}
