export type AdminFeaturesResponse = {
  "admin_auth_required": boolean;
  "admin_auth_mode": "protected" | "open";
  "route_families": Array<string>;
  "route_families_wildcard": boolean;
};

export type AdminSearchResponse = {
  "query": string;
  "route_family"?: string | null;
  "domain"?: string | null;
  "limit": number;
  "total": number;
  "truncated": boolean;
  "results": Array<AdminSearchResult>;
};

export type AdminSearchResult = {
  "id": string;
  "domain": string;
  "kind": string;
  "route_family"?: string | null;
  "realm"?: string | null;
  "area"?: string | null;
  "resource"?: string | null;
  "operation"?: string | null;
  "title": string;
  "summary": string;
  "health"?: string | null;
  "href": string;
  "matched_fields": Array<string>;
  "metadata": {
  [key: string]: string;
};
};

export type AreaCollection = {
  "realm": string;
  "areas": Array<AreaEntry>;
};

export type AreaDetail = {
  "realm": string;
  "area": string;
};

export type AreaEntry = {
  "area": string;
};

export type BrokerStats = {
  "uptime_seconds": number;
  "connections": number;
  "sessions": number;
  "realms": Array<string>;
  "messages_per_second": number;
  "router_backpressure_total": number;
  "router_high_lane_backpressure_total": number;
};

export type ConfidenceJustification = {
  "signals_matched": Array<string>;
  "signals_missing": Array<string>;
  "rationale": string;
};

export type DiagnosticHotspot = DiagnosticSnapshot & {
  "domain": string;
  "realm"?: string | null;
  "area"?: string | null;
  "resource"?: string | null;
  "operation"?: string | null;
  "family"?: number | null;
  "backlog"?: number | null;
  "inflight"?: number | null;
  "ready"?: number | null;
  "delayed"?: number | null;
  "dead_letters"?: number | null;
  "workers"?: number | null;
  "subscriptions"?: number | null;
  "owner_session"?: string | null;
  "worker_session"?: string | null;
};

export type DiagnosticSeverity = "informational" | "low" | "medium" | "high" | "critical";

export type DiagnosticSnapshot = {
  "current_stage": string;
  "trend": DiagnosticTrend;
  "severity": DiagnosticSeverity;
  "likely_bottleneck"?: string | null;
  "last_changed_at"?: string | null;
  "last_success_at"?: string | null;
  "last_failure_at"?: string | null;
  "age_seconds"?: number | null;
  "recent_transition_count": number;
  "failure_count": number;
  "contention_count": number;
  "waiter_count": number;
  "confidence": number;
  "confidence_justification": ConfidenceJustification;
  "explanation_hints": Array<string>;
  "delta_5m"?: number | null;
  "delta_1h"?: number | null;
};

export type DiagnosticTrend = "growing" | "shrinking" | "steady" | "stalled" | "unknown";

export type DomainStats = {
  "kv": KvStats;
  "stream": StreamStats;
  "notice": NoticeStats;
  "queue": QueueStats;
  "rpc": RpcStats;
  "lease": LeaseStats;
  "schedule": ScheduleStats;
};

export type Error = {
  "error": string;
};

export type GlobalStats = {
  "broker": BrokerStats;
  "domains": DomainStats;
  "diagnostics": GlobalTroubleshootingDiagnostics;
};

export type GlobalTroubleshootingDiagnostics = {
  "incident_summary": IncidentSummary;
  "top_bottleneck"?: DiagnosticHotspot;
  "last_significant_transition_at"?: string | null;
  "hotspots": Array<DiagnosticHotspot>;
};

export type IncidentStatus = "healthy" | "degraded" | "stalled" | "recovering" | "unknown";

export type IncidentSummary = {
  "status": IncidentStatus;
  "title": string;
  "likely_bottleneck"?: string | null;
  "severity": DiagnosticSeverity;
  "confidence": number;
  "explanation": string;
  "recommended_next_query": string | null;
  "suggested_next_queries": Array<SuggestedQuery>;
};

export type KvByteValue = {
  "base64": string;
  "utf8": string | null;
  "len_bytes": number;
};

export type KvCommittedPair = {
  "key": KvByteValue;
  "value": KvByteValue;
};

export type KvCommittedValueResponse = {
  "route_family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "key": KvByteValue;
  "found": boolean;
  "value": KvByteValue | null;
};

export type KvPrefixScanResponse = {
  "route_family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "prefix": KvByteValue;
  "limit": number;
  "has_more": boolean;
  "items": Array<KvCommittedPair>;
};

export type KvResourceDetail = {
  "route_family"?: number | null;
  "realm": string;
  "area": string;
  "resource": string;
  "estimated_record_count": number;
  "estimated_storage_bytes": number;
  "estimate_complete": boolean;
  "read_latency_avg_ms": number;
  "read_latency_p95_ms": number;
  "write_latency_avg_ms": number;
  "write_latency_p95_ms": number;
  "transactions_active": number;
  "diagnostics": DiagnosticSnapshot;
};

export type KvRowsResponse = {
  "route_family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "starts_with": KvByteValue;
  "limit": number;
  "next_cursor": string | null;
  "has_more": boolean;
  "items": Array<KvCommittedPair>;
};

export type KvStats = {
  "transactions_active": number;
  "keys_total": number;
  "commits_failed_total": number;
  "invalid_transaction_rejects_total": number;
  "operations_per_second": number;
  "diagnostics": DiagnosticSnapshot;
};

export type KvTransaction = {
  "tx_id"?: number;
  "realm"?: string;
  "area"?: string;
  "resource"?: string;
  "mode"?: string;
  "started_at"?: string;
  "operations_count"?: number;
  "idle_seconds"?: number;
};

export type KvTransactionsList = {
  "transactions": Array<KvTransaction>;
};

export type LeaseResourceDetail = {
  "realm": string;
  "area": string;
  "resource": string;
  "active_leases": number;
  "oldest_lease_age_seconds": number;
  "diagnostics": DiagnosticSnapshot;
};

export type LeaseSearchItem = {
  "route_family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "state": string;
  "owner_id": string | null;
  "owner_session_id": string | null;
  "queued_token": number | null;
  "expires_at": string | null;
  "acquired_at": string | null;
  "renewals": number | null;
  "pending_waiters": number;
};

export type LeaseSearchResponse = {
  "route_family": number;
  "limit": number;
  "items": Array<LeaseSearchItem>;
};

export type LeaseStats = {
  "leases_active": number;
  "waiter_depth": number;
  "oldest_lease_age_seconds": number;
  "requests_total": number;
  "success_total": number;
  "failure_total": number;
  "acquire_timeouts_total": number;
  "forced_releases_total": number;
  "invalid_token_rejects_total": number;
  "ownership_churn_total": number;
  "operations_per_second": number;
  "diagnostics": DiagnosticSnapshot;
};

export type LoginRequest = {
  "username": string;
  "password": string;
};

export type MessagingTopology = {
  "generated_at": string;
  "broker": BrokerStats;
  "diagnostics": GlobalTroubleshootingDiagnostics;
  "session_groups": Array<TopologySessionGroup>;
  "lanes": Array<TopologyLane>;
  "connections": TopologyConnectionPage;
};

export type NoticeDeliveryObservation = {
  "route_family": number;
  "realm": string;
  "area": string | null;
  "resource": string | null;
  "route": string;
  "session_id": string | null;
  "subscription_id": number | null;
  "status": string;
  "notifications_received": number;
  "publishes_total": number;
  "publishes_per_minute": number;
};

export type NoticeDeliveryObservationList = {
  "route_family": number;
  "limit": number;
  "observations": Array<NoticeDeliveryObservation>;
};

export type NoticeResourceDetail = {
  "realm": string;
  "area": string;
  "resource": string;
  "subscriptions_active": number;
  "diagnostics": DiagnosticSnapshot;
};

export type NoticeStats = {
  "subscriptions_active": number;
  "routes_active": number;
  "max_route_subscribers": number;
  "requests_total": number;
  "success_total": number;
  "failure_total": number;
  "delivery_drops_total": number;
  "unsubscribes_total": number;
  "wildcard_limit_rejects_total": number;
  "publishes_per_second": number;
  "diagnostics": DiagnosticSnapshot;
};

export type NoticeSubscription = {
  "route_family": number;
  "subscription_id": number;
  "session_id": string;
  "realm": string;
  "pattern": string;
  "created_at": string;
  "notifications_received": number;
};

export type NoticeSubscriptionsList = {
  "subscriptions": Array<NoticeSubscription>;
};

export type OperationCollection = {
  "realm": string;
  "area": string;
  "resource": string;
  "operations": Array<OperationEntry>;
};

export type OperationEntry = {
  "operation": string;
};

export type QueueAgeBuckets = {
  "under_1m": number;
  "under_5m": number;
  "under_15m": number;
  "over_15m": number;
};

export type QueueAreaCollection = {
  "realm": string;
  "areas": Array<QueueAreaEntry>;
};

export type QueueAreaDetail = {
  "realm": string;
  "area": string;
  "queue_count": number;
  "subscriptions_active": number;
  "messages_ready": number;
  "messages_delayed": number;
  "messages_inflight": number;
  "messages_dead_lettered": number;
  "messages_total": number;
  "oldest_backlog_age_seconds": number;
  "enqueue_success_total": number;
  "complete_success_total": number;
  "in_rate_per_second": number;
  "out_rate_per_second": number;
  "status": "idle" | "draining" | "backlogged" | "falling_behind";
  "queues": Array<QueueResourceEntry>;
};

export type QueueAreaEntry = {
  "realm": string;
  "area": string;
  "queue_count": number;
  "subscriptions_active": number;
  "messages_ready": number;
  "messages_delayed": number;
  "messages_inflight": number;
  "messages_dead_lettered": number;
  "messages_total": number;
  "oldest_backlog_age_seconds": number;
  "enqueue_success_total": number;
  "complete_success_total": number;
  "in_rate_per_second": number;
  "out_rate_per_second": number;
  "status": "idle" | "draining" | "backlogged" | "falling_behind";
};

export type QueueDeadLetter = {
  "message_id": number;
  "family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "dead_lettered_at": string;
  "attempts": number;
  "reason": string;
};

export type QueueDeadLettersList = {
  "messages": Array<QueueDeadLetter>;
};

export type QueueInflight = {
  "message_id": number;
  "family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "inflight_token": string;
  "session_id": string;
  "expires_at": string;
  "attempts": number;
};

export type QueueInflightList = {
  "inflight": Array<QueueInflight>;
};

export type QueueRealmCollection = {
  "realms": Array<QueueRealmEntry>;
};

export type QueueRealmDetail = {
  "realm": string;
  "area_count": number;
  "queue_count": number;
  "subscriptions_active": number;
  "messages_ready": number;
  "messages_delayed": number;
  "messages_inflight": number;
  "messages_dead_lettered": number;
  "messages_total": number;
  "oldest_backlog_age_seconds": number;
  "enqueue_success_total": number;
  "complete_success_total": number;
  "in_rate_per_second": number;
  "out_rate_per_second": number;
  "status": "idle" | "draining" | "backlogged" | "falling_behind";
  "areas": Array<QueueAreaEntry>;
  "queues": Array<QueueResourceEntry>;
};

export type QueueRealmEntry = {
  "realm": string;
  "area_count": number;
  "queue_count": number;
  "subscriptions_active": number;
  "messages_ready": number;
  "messages_delayed": number;
  "messages_inflight": number;
  "messages_dead_lettered": number;
  "messages_total": number;
  "oldest_backlog_age_seconds": number;
  "enqueue_success_total": number;
  "complete_success_total": number;
  "in_rate_per_second": number;
  "out_rate_per_second": number;
  "status": "idle" | "draining" | "backlogged" | "falling_behind";
};

export type QueueResourceCollection = {
  "realm": string;
  "area": string;
  "resources": Array<QueueResourceEntry>;
};

export type QueueResourceDetail = {
  "realm": string;
  "area": string;
  "resource": string;
  "subscriptions_active": number;
  "messages_ready": number;
  "messages_delayed": number;
  "messages_inflight": number;
  "messages_dead_lettered": number;
  "messages_total": number;
  "oldest_message_age_seconds": number;
  "oldest_backlog_age_seconds": number;
  "backlog_age_buckets": QueueAgeBuckets;
  "delay_age_buckets": QueueAgeBuckets;
  "enqueue_success_total": number;
  "complete_success_total": number;
  "in_rate_per_second": number;
  "out_rate_per_second": number;
  "status": "idle" | "draining" | "backlogged" | "falling_behind";
  "diagnostics": DiagnosticSnapshot;
};

export type QueueResourceEntry = {
  "realm": string;
  "area": string;
  "resource": string;
  "family_count": number;
  "subscriptions_active": number;
  "messages_ready": number;
  "messages_delayed": number;
  "messages_inflight": number;
  "messages_dead_lettered": number;
  "messages_total": number;
  "oldest_backlog_age_seconds": number;
  "enqueue_success_total": number;
  "complete_success_total": number;
  "in_rate_per_second": number;
  "out_rate_per_second": number;
  "status": "idle" | "draining" | "backlogged" | "falling_behind";
};

export type QueueStats = {
  "messages_ready": number;
  "messages_delayed": number;
  "messages_pending": number;
  "messages_dead_lettered": number;
  "oldest_message_age_seconds": number;
  "oldest_backlog_age_seconds": number;
  "backlog_age_buckets": QueueAgeBuckets;
  "delay_age_buckets": QueueAgeBuckets;
  "inflight_active": number;
  "requests_total": number;
  "success_total": number;
  "failure_total": number;
  "enqueues_total": number;
  "reserves_total": number;
  "completes_total": number;
  "releases_total": number;
  "extends_total": number;
  "notify_drops_total": number;
  "redeliveries_total": number;
  "dead_letter_transitions_total": number;
  "complete_rejected_total": number;
  "operations_per_second": number;
  "diagnostics": DiagnosticSnapshot;
};

export type RealmCollection = {
  "realms": Array<RealmEntry>;
};

export type RealmDetail = {
  "realm": string;
};

export type RealmEntry = {
  "realm": string;
};

export type ResourceCollection = {
  "realm": string;
  "area": string;
  "resources": Array<ResourceEntry>;
};

export type ResourceComparison = {
  "domain": string;
  "comparison_mode": string;
  "derived": boolean;
  "left": ResourceComparisonSide;
  "right": ResourceComparisonSide;
  "delta": ResourceComparisonDelta;
  "summary": string;
};

export type ResourceComparisonDelta = {
  "backlog"?: number | null;
  "inflight"?: number | null;
  "ready"?: number | null;
  "delayed"?: number | null;
  "dead_letters"?: number | null;
  "workers"?: number | null;
  "subscriptions"?: number | null;
  "waiters"?: number | null;
  "age_seconds"?: number | null;
  "recent_transition_count"?: number | null;
  "failure_count"?: number | null;
  "contention_count"?: number | null;
  "operations_total"?: number | null;
};

export type ResourceComparisonMetrics = {
  "backlog"?: number | null;
  "inflight"?: number | null;
  "ready"?: number | null;
  "delayed"?: number | null;
  "dead_letters"?: number | null;
  "workers"?: number | null;
  "subscriptions"?: number | null;
  "waiters"?: number | null;
  "age_seconds"?: number | null;
  "recent_transition_count"?: number | null;
  "failure_count"?: number | null;
  "contention_count"?: number | null;
  "operations_total"?: number | null;
};

export type ResourceComparisonScope = {
  "realm": string;
  "area": string;
  "resource": string;
  "family"?: number | null;
};

export type ResourceComparisonSide = {
  "scope": ResourceComparisonScope;
  "diagnostics": DiagnosticSnapshot;
  "metrics": ResourceComparisonMetrics;
};

export type ResourceEntry = {
  "resource": string;
  "estimated_record_count"?: number;
  "estimated_storage_bytes"?: number;
  "estimate_complete"?: boolean;
  "read_latency_avg_ms"?: number;
  "read_latency_p95_ms"?: number;
  "write_latency_avg_ms"?: number;
  "write_latency_p95_ms"?: number;
  "transactions_active"?: number;
};

export type ResourceTimeline = {
  "domain": string;
  "realm": string;
  "area": string;
  "resource": string;
  "family"?: number | null;
  "derived": boolean;
  "limit": number;
  "diagnostics": DiagnosticSnapshot;
  "events": Array<ResourceTimelineEvent>;
};

export type ResourceTimelineEvent = {
  "domain": string;
  "kind": ResourceTimelineKind;
  "observed_at": string;
  "summary": string;
  "realm": string;
  "area": string;
  "resource": string;
  "operation"?: string | null;
  "family"?: number | null;
  "age_seconds"?: number | null;
  "owner_session"?: string | null;
  "worker_session"?: string | null;
  "correlation_id"?: string | null;
  "message_id"?: number | null;
  "attempts"?: number | null;
};

export type ResourceTimelineKind = "observation" | "transition" | "failure" | "retry" | "ownership_change" | "state_flip" | "registration";

export type RpcCallObservation = {
  "route_family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "operation": string | null;
  "route": string;
  "correlation_id": string | null;
  "state": string;
  "submitted_at": string | null;
  "registered_at": string | null;
  "age_seconds": number | null;
  "worker_session_id": string | null;
  "requests_handled": number | null;
  "average_latency_ms": number | null;
};

export type RpcCallObservationList = {
  "route_family": number;
  "limit": number;
  "observations": Array<RpcCallObservation>;
};

export type RpcLatencyBuckets = {
  "under_5ms": number;
  "under_25ms": number;
  "under_100ms": number;
  "over_100ms": number;
};

export type RpcOperationDetail = {
  "realm": string;
  "area": string;
  "resource": string;
  "operation": string;
  "workers_registered": number;
  "requests_pending": number;
  "slowest_worker_average_latency_ms": number;
  "worker_latency_buckets": RpcLatencyBuckets;
  "diagnostics": DiagnosticSnapshot;
};

export type RpcPendingList = {
  "requests": Array<RpcPendingRequest>;
};

export type RpcPendingRequest = {
  "route_family": number;
  "correlation_id": string;
  "route": string;
  "submitted_at": string;
  "age_seconds": number;
  "worker_session_id": string | null;
};

export type RpcStats = {
  "workers_registered": number;
  "requests_pending": number;
  "oldest_pending_request_age_seconds": number;
  "pending_routes_active": number;
  "slowest_worker_average_latency_ms": number;
  "worker_latency_buckets": RpcLatencyBuckets;
  "requests_total": number;
  "success_total": number;
  "failure_total": number;
  "request_timeouts_total": number;
  "backpressure_rejects_total": number;
  "duplicate_correlation_rejects_total": number;
  "wrong_worker_rejects_total": number;
  "responses_dropped_closed_caller_total": number;
  "responses_missing_pending_total": number;
  "acks_rejected_wrong_worker_total": number;
  "invalid_sequence_responses_total": number;
  "invalid_sequence_errors_forwarded_total": number;
  "invalid_sequence_errors_dropped_total": number;
  "operations_per_second": number;
  "diagnostics": DiagnosticSnapshot;
};

export type RpcWorker = {
  "route_family": number;
  "session_id": string;
  "realm": string;
  "route": string;
  "registered_at": string;
  "requests_handled": number;
  "average_latency_ms": number;
};

export type RpcWorkersList = {
  "workers": Array<RpcWorker>;
};

export type RuntimeDrainResponse = {
  "lifecycle_state": "running" | "draining" | "shutting_down";
  "active_sessions": number;
  "drain_grace_seconds": number;
  "drain_started_epoch_ms"?: number | null;
  "drain_deadline_epoch_ms"?: number | null;
  "close_reason": string;
};

export type ScheduleExecutionObservation = {
  "route_family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "operation": string;
  "status": string;
  "cron": string;
  "delivery_mode": "broadcast" | "single";
  "next_run": string;
  "last_run": string | null;
  "executions_total": number;
};

export type ScheduleExecutionObservationList = {
  "route_family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "limit": number;
  "observations": Array<ScheduleExecutionObservation>;
};

export type ScheduleLatencyBuckets = {
  "under_1ms": number;
  "under_5ms": number;
  "under_10ms": number;
  "under_50ms": number;
  "under_100ms": number;
  "under_500ms": number;
  "under_1s": number;
  "under_5s": number;
  "over_5s": number;
};

export type ScheduleMissedObservation = {
  "route_family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "operation": string;
  "delivery_mode": "broadcast" | "single";
  "fire_ms": number;
  "fire_at": string;
  "claimed_at": string;
  "age_seconds": number;
  "status": string;
};

export type ScheduleMissedObservationList = {
  "route_family": number;
  "limit": number;
  "observations": Array<ScheduleMissedObservation>;
};

export type ScheduleResourceDetail = {
  "realm": string;
  "area": string;
  "resource": string;
  "enabled": boolean;
  "cron"?: string | null;
  "next_run"?: string | null;
  "executions_total": number;
  "diagnostics": DiagnosticSnapshot;
};

export type ScheduleStats = {
  "schedules_active": number;
  "executions_per_minute": number;
  "subscriptions_active": number;
  "pending_fire_claims": number;
  "pending_ack_retries": number;
  "oldest_pending_claim_age_seconds": number;
  "request_latency_buckets": ScheduleLatencyBuckets;
  "notify_failures_total": number;
  "ack_failures_total": number;
  "overdue_normalizations_total": number;
  "create_persistence_failures_total": number;
  "upsert_persistence_failures_total": number;
  "cancel_persistence_failures_total": number;
  "diagnostics": DiagnosticSnapshot;
};

export type SessionInfo = {
  "session_id"?: string;
  "route_family"?: number;
  "subject"?: string;
  "identity_claim"?: string;
  "identity_value"?: string;
  "connected_at"?: string;
  "idle_seconds"?: number;
  "messages_received"?: number;
  "messages_sent"?: number;
  "transport"?: string;
  "remote_addr"?: string;
};

export type SessionResponse = {
  "authenticated": boolean;
  "username": string;
  "route_families": Array<string>;
  "route_families_wildcard": boolean;
};

export type SessionsList = {
  "sessions": Array<SessionInfo>;
};

export type StreamAdminRecord = {
  "route_family": number;
  "realm": string;
  "area": string;
  "resource": string;
  "resource_offset": number;
  "area_offset": number | null;
  "realm_offset": number | null;
  "created_at_ms": number;
  "body": KvByteValue;
  "metadata": KvByteValue | null;
};

export type StreamAreaWatermark = {
  "family": number;
  "watermark": number;
};

export type StreamAreaWatermarkDetail = {
  "realm": string;
  "area": string;
  "resource_count": number;
  "family_watermarks": Array<StreamAreaWatermark>;
};

export type StreamLagBuckets = {
  "caught_up": number;
  "under_10": number;
  "under_100": number;
  "over_100": number;
};

export type StreamLatencyBuckets = {
  "under_1ms": number;
  "under_5ms": number;
  "under_10ms": number;
  "under_50ms": number;
  "under_100ms": number;
  "under_500ms": number;
  "under_1s": number;
  "under_5s": number;
  "over_5s": number;
};

export type StreamRealmWatermark = {
  "family": number;
  "watermark": number;
};

export type StreamRealmWatermarkDetail = {
  "realm": string;
  "area_count": number;
  "resource_count": number;
  "family_watermarks": Array<StreamRealmWatermark>;
};

export type StreamRecordsResponse = {
  "route_family": number;
  "realm": string | null;
  "area": string | null;
  "resource": string | null;
  "from_offset": number;
  "limit": number;
  "has_more": boolean;
  "records": Array<StreamAdminRecord>;
};

export type StreamResourceDetail = {
  "realm": string;
  "area": string;
  "resource": string;
  "offset": number;
  "watermark": number;
  "size_bytes": number;
  "sessions_active": number;
  "diagnostics": DiagnosticSnapshot;
};

export type StreamStats = {
  "streams_active": number;
  "append_sessions_active": number;
  "events_total": number;
  "requests_total": number;
  "success_total": number;
  "failure_total": number;
  "append_sessions_started_total": number;
  "append_sessions_ended_total": number;
  "append_conflicts_total": number;
  "notify_drops_total": number;
  "watermark_lag_buckets": StreamLagBuckets;
  "request_latency_buckets": StreamLatencyBuckets;
  "operations_per_second": number;
  "subscriptions_active": number;
  "diagnostics": DiagnosticSnapshot;
};

export type StructuredMetricSample = {
  "name": string;
  "kind": "counter" | "gauge" | "histogram" | "summary";
  "help": string;
  "labels": {
  [key: string]: string;
};
  "value": number;
};

export type StructuredMetricsResponse = {
  "scope": "all" | "family";
  "family"?: number | null;
  "generated_at": number;
  "samples": Array<StructuredMetricSample>;
};

export type SuggestedQuery = {
  "priority": number;
  "title": string;
  "endpoint": string;
  "rationale": string;
  "remediation": string;
};

export type TopologyConnection = {
  "id": string;
  "kind": TopologyConnectionKind;
  "source": string;
  "target": string;
  "label": string;
  "state": TopologyState;
  "scope": TopologyScope;
  "metrics": Array<TopologyCounter>;
};

export type TopologyConnectionKind = "broker_domain_flow" | "notice_subscription" | "rpc_worker" | "rpc_pending_assignment" | "queue_inflight_consumer" | "lease_owner" | "stream_append_activity" | "schedule_subscription_activity" | "kv_transaction_activity";

export type TopologyConnectionPage = {
  "items": Array<TopologyConnection>;
  "total": number;
  "truncated": boolean;
  "limit": number;
};

export type TopologyCounter = {
  "key": string;
  "label": string;
  "value": number;
};

export type TopologyLane = {
  "id": "queue" | "rpc" | "notice" | "schedule" | "stream" | "lease" | "kv";
  "title": string;
  "state": TopologyState;
  "activity_per_second": number;
  "diagnostics": DiagnosticSnapshot;
  "counters": Array<TopologyCounter>;
  "consumers": number;
  "observers": number;
  "top_scoped_resources": Array<TopologyScopedResource>;
};

export type TopologyScope = {
  "realm"?: string;
  "route_family"?: number;
  "area"?: string;
  "resource"?: string;
  "operation"?: string;
  "route"?: string;
  "pattern"?: string;
  "session_id"?: string;
};

export type TopologyScopedResource = {
  "id": string;
  "label": string;
  "state": TopologyState;
  "scope": TopologyScope;
  "counters": Array<TopologyCounter>;
};

export type TopologySessionGroup = {
  "route_family": number;
  "sessions": number;
  "messages_received": number;
  "messages_sent": number;
  "transports": Array<string>;
  "max_idle_seconds": number;
  "representative_sessions": Array<SessionInfo>;
};

export type TopologyState = "quiet" | "flowing" | "pressure" | "blocked";
