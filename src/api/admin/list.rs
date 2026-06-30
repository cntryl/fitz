mod admin_reads;
mod detail_views;
mod dto_operations;
mod dto_queue_runtime;
mod dto_resources;
mod query_params;
mod resource_inventory;
mod resource_paths;

use crate::api::admin::troubleshooting::{
    self, ResourceComparison, ResourceComparisonMetrics, ResourceComparisonScope,
    ResourceComparisonSide,
};
use crate::api::http::Response;
use crate::boot::Runtime;
use crate::domains::stream::sink::AdminStreamReadRequest;
use crate::runtime::routing::{route_quad, route_triplet, RouteFamily};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

pub use admin_reads::{
    kv_events_for_resource, lease_events_for_resource, notice_delivery_observations,
    notice_events_for_resource, notice_subscriptions_for_resource, queue_dead_letters_for_resource,
    queue_events_for_resource, queue_inflight_for_resource, rpc_events_for_resource, rpc_pending,
    rpc_workers_for_operation, schedule_events_for_resource, schedule_executions_for_resource,
    stream_events_for_resource, stream_records_for_resource,
};
pub(crate) use admin_reads::{
    lease_search, rpc_call_observations, schedule_missed_observations, stream_search,
};
pub use detail_views::{
    kv_compare_detail, kv_detail, lease_compare_detail, lease_detail, notice_compare_detail,
    notice_detail, queue_compare_detail, queue_detail, rpc_compare_detail, rpc_operation_detail,
    schedule_compare_detail, schedule_detail, stream_area_watermark_detail, stream_compare_detail,
    stream_detail, stream_realm_watermark_detail,
};
pub(crate) use detail_views::{
    matches_operation_route, matches_resource_route, parse_flexible_route, parse_rpc_operation,
};
pub use dto_operations::{
    KvByteValue, KvCommittedPair, KvCommittedValueResponse, KvLatencySnapshot,
    KvPrefixScanResponse, KvResourceInventoryEntry, KvRowsResponse, KvTransaction,
    KvTransactionsList, LeaseSearchItem, LeaseSearchResponse, LeaseWaiterInfo,
    NoticeDeliveryObservation, NoticeDeliveryObservationList, NoticeRouteInfo, NoticeRoutesList,
    NoticeSubscription, NoticeSubscriptionsList, RpcCallObservation, RpcCallObservationList,
    ScheduleExecutionObservation, ScheduleExecutionObservationList, ScheduleLatencyBuckets,
    ScheduleMissedObservation, ScheduleMissedObservationList, SchedulePendingClaimInfo,
    StreamAdminRecord, StreamInfo, StreamLagBuckets, StreamLatencyBuckets, StreamRecordsResponse,
    StreamsList,
};
pub(crate) use dto_operations::{
    LeaseSearchRequest, RpcCallObservationRequest, StreamSearchRequest,
};
pub use dto_queue_runtime::{
    LeaseInfo, LeasesList, QueueAreaCollection, QueueAreaDetail, QueueAreaEntry, QueueDeadLetter,
    QueueDeadLettersList, QueueInflight, QueueInflightList, QueueInfo, QueueRealmCollection,
    QueueRealmDetail, QueueRealmEntry, QueueResourceCollection, QueueResourceEntry, QueuesList,
    RpcLatencyBuckets, RpcPendingList, RpcPendingRequest, RpcWorker, RpcWorkersList, ScheduleInfo,
    SchedulesList, SessionInfo, SessionsList,
};
pub(crate) use dto_resources::{
    worse_queue_status, DEFAULT_ADMIN_RECORD_LIMIT, DEFAULT_KV_SCAN_LIMIT, MAX_ADMIN_RECORD_LIMIT,
    MAX_KV_SCAN_LIMIT,
};
pub use dto_resources::{
    AreaCollection, AreaDetail, AreaEntry, KvResourceDetail, LeaseResourceDetail,
    NoticeResourceDetail, OperationCollection, OperationEntry, QueueAgeBuckets,
    QueueResourceDetail, RealmCollection, RealmDetail, RealmEntry, ResourceCollection,
    ResourceEntry, ResourceRef, RpcOperationDetail, ScheduleResourceDetail, StreamAreaWatermark,
    StreamAreaWatermarkDetail, StreamRealmWatermark, StreamRealmWatermarkDetail,
    StreamResourceDetail,
};
pub use query_params::{
    parse_admin_record_limit, parse_kv_query_bytes, parse_kv_scan_limit, parse_limit_query_param,
    parse_optional_kv_cursor, parse_optional_kv_query_bytes, parse_optional_string_query_param,
    parse_optional_u64_query_param, parse_query_params,
};
pub(crate) use resource_inventory::kv_byte_value;
pub use resource_inventory::{
    collect_areas, collect_kv_resources, collect_realms, collect_resources,
    kv_committed_value_for_resource, kv_prefix_scan_for_resource, kv_resources,
    kv_rows_for_resource, kv_transactions_for_resource, lease_resources, list_sessions,
    notice_resources, queue_resources, rpc_operations, rpc_resources, schedule_resources,
    stream_resources,
};
pub(crate) use resource_paths::{
    collect_distinct_entries, collect_resource_refs, matches_family, IntoResourceRef,
    OwnedRpcOperation,
};
pub use resource_paths::{
    collect_queue_areas, collect_queue_realms, collect_queue_resources, queue_area_detail,
    queue_realm_detail, ResourcePath, RpcOperationPath,
};

#[cfg(test)]
mod tests;
