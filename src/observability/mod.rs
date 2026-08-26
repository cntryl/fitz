/// Centralized observability configuration, constants, and helpers.
///
/// This module defines:
/// - Span names and sampling rules
/// - Metric names and types
/// - Common attribute keys
/// - Helpers for safe span/metric operations
///
/// ## Sampling Strategy
///
/// **Hot paths (0.1% sampling):**
/// - routing (route matching)
/// - `tlv_codec` (TLV encode/decode)
/// - `frame_processing` (TCP/WS frame I/O)
///
/// **`AlwaysSample` (100%):**
/// - request (top-level request boundaries)
/// - session (session lifecycle)
/// - `domain_operation` (`domain::Actor::receive`)
/// - `permission_check` (authorization)
///
/// **Debug level (typically filtered):**
/// - family actor workers and internal operations
pub mod global;
pub mod metrics;
pub mod tracing;

pub use global::{
    counter_add, counter_inc, gauge_dec, gauge_inc, gauge_set, histogram_observe_us,
    hot_path_counter_inc, hot_path_histogram_observe_us, hot_path_metrics_enabled,
    init_observability, metrics, try_init_observability, ScopedHistogramUs,
};

// ============================================================================
// SPAN NAMES (for consistent naming and quick reference)
// ============================================================================

/// Top-level span for an incoming request (always sampled)
pub const SPAN_REQUEST: &str = "request";

/// Service identity spans and attributes
pub const SERVICE_NAME: &str = "fitz";
pub const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const SERVICE_INSTANCE_ID_ENV: &str = "FITZ_SERVICE_INSTANCE_ID";
pub const DEPLOYMENT_ENVIRONMENT_ENV: &str = "FITZ_DEPLOYMENT_ENVIRONMENT";

/// TLV codec span (hot path, 0.1% sample)
pub const SPAN_TLV_ENCODE: &str = "tlv::encode";
pub const SPAN_TLV_DECODE: &str = "tlv::decode";

/// Route matching span (hot path, 0.1% sample)
pub const SPAN_ROUTE_MATCH: &str = "routing::match";

/// Frame I/O span (hot path, 0.1% sample)
pub const SPAN_FRAME_READ: &str = "frame::read";
pub const SPAN_FRAME_WRITE: &str = "frame::write";

/// Session lifecycle
pub const SPAN_SESSION_CREATE: &str = "session::create";
pub const SPAN_SESSION_AUTH: &str = "session::authenticate";
pub const SPAN_PERMISSION_CHECK: &str = "permission::check";

/// Runtime/router delivery
pub const SPAN_MESSAGE_DELIVER: &str = "router::deliver";
pub const SPAN_MAILBOX_ENQUEUE: &str = "mailbox::enqueue";

/// Domain operation (always sampled)
pub const SPAN_DOMAIN_OPERATION: &str = "domain::operation";

/// Actor scheduling (debug level)
pub const SPAN_ACTOR_SCHEDULE: &str = "actor::schedule";

// ============================================================================
// METRIC NAMES
// ============================================================================

// Counters
pub const METRIC_CONNECTIONS_OPENED: &str = "fitz_connections_opened_total";
pub const METRIC_CONNECTIONS_CLOSED: &str = "fitz_connections_closed_total";
pub const METRIC_SESSIONS_CREATED: &str = "fitz_sessions_created_total";
pub const METRIC_SESSIONS_CLOSED: &str = "fitz_sessions_closed_total";
pub const METRIC_SESSION_CLEANUP_FAILURES: &str = "fitz_session_cleanup_failures_total";
pub const METRIC_SESSION_CLEANUP_RETRIES: &str = "fitz_session_cleanup_retries_total";
pub const METRIC_SESSION_CLEANUP_SUCCESSES: &str = "fitz_session_cleanup_successes_total";
/// A cleanup ticket exhausted its retry budget and was dropped instead of
/// being retried forever. Distinct from `METRIC_SESSION_CLEANUP_FAILURES`,
/// which counts every individual failed attempt (including ones that go on
/// to succeed on retry).
pub const METRIC_SESSION_CLEANUP_PERMANENT_FAILURES: &str =
    "fitz_session_cleanup_permanent_failures_total";

pub const METRIC_FRAMES_RECEIVED: &str = "fitz_frames_received_total";
pub const METRIC_FRAMES_SENT: &str = "fitz_frames_sent_total";
pub const METRIC_FRAMES_MALFORMED: &str = "fitz_frames_malformed_total";
pub const METRIC_TCP_BACKPRESSURE: &str = "fitz_tcp_backpressure_total";
pub const METRIC_WS_BACKPRESSURE: &str = "fitz_ws_backpressure_total";
pub const METRIC_OUTBOUND_BACKPRESSURE: &str = "fitz_outbound_backpressure_total";

pub const METRIC_TLV_ENCODE_ERRORS: &str = "fitz_tlv_encode_errors_total";
pub const METRIC_TLV_DECODE_ERRORS: &str = "fitz_tlv_decode_errors_total";

pub const METRIC_ROUTE_MISMATCHES: &str = "fitz_route_mismatches_total";
pub const METRIC_DELIVERY_FAILURES: &str = "fitz_delivery_failures_total";
pub const METRIC_ROUTER_BACKPRESSURE: &str = "fitz_router_backpressure_total";
pub const METRIC_ROUTER_HIGH_LANE_BACKPRESSURE: &str = "fitz_router_high_lane_backpressure_total";
pub const METRIC_INGRESS_DOMAIN_BACKPRESSURE_RETRIES: &str =
    "fitz_ingress_domain_backpressure_retries_total";
pub const METRIC_INGRESS_DOMAIN_BACKPRESSURE_ACCEPTED: &str =
    "fitz_ingress_domain_backpressure_accepted_total";
pub const METRIC_INGRESS_DOMAIN_BACKPRESSURE_EXHAUSTED: &str =
    "fitz_ingress_domain_backpressure_exhausted_total";
/// Domain commands answered with a retryable error because the actor did not
/// reply in time. Previously these closed the whole session, so they were
/// only visible as disconnects.
pub const METRIC_INGRESS_DOMAIN_DISPATCH_TIMEOUTS: &str =
    "fitz_ingress_domain_dispatch_timeouts_total";

pub const METRIC_AUTH_FAILURES: &str = "fitz_auth_failures_total";
pub const METRIC_PERMISSION_DENIALS: &str = "fitz_permission_denials_total";
pub const METRIC_WORKER_BUSY_TIME: &str = "fitz_worker_busy_us_total";
pub const METRIC_WORKER_IDLE_TIME: &str = "fitz_worker_idle_us_total";

// Domain-specific counters (per domain)
pub const METRIC_DOMAIN_OPERATIONS: &str = "fitz_domain_operations_total";
pub const METRIC_DOMAIN_ERRORS: &str = "fitz_domain_errors_total";
pub const METRIC_QUEUE_RECOVERY_INDEX_HITS: &str = "fitz_queue_recovery_index_hits_total";
pub const METRIC_QUEUE_RECOVERY_INDEX_MISSING: &str = "fitz_queue_recovery_index_missing_total";
pub const METRIC_QUEUE_RECOVERY_INDEX_INVALID: &str = "fitz_queue_recovery_index_invalid_total";
pub const METRIC_QUEUE_RECOVERY_INDEX_FALLBACKS: &str = "fitz_queue_recovery_index_fallbacks_total";
pub const METRIC_QUEUE_REDELIVERIES: &str = "fitz_queue_redeliveries_total";
pub const METRIC_QUEUE_DLQ_TRANSITIONS: &str = "fitz_queue_dlq_transitions_total";
pub const METRIC_QUEUE_HYDRATION_DLQ_TRANSITIONS: &str =
    "fitz_queue_hydration_dlq_transitions_total";
pub const METRIC_QUEUE_COMPLETE_REJECTED: &str = "fitz_queue_complete_rejected_total";

// Queue operation-specific counters and histograms
pub const METRIC_QUEUE_ENQUEUE_TOTAL: &str = "fitz_queue_enqueue_total";
pub const METRIC_QUEUE_RESERVE_TOTAL: &str = "fitz_queue_reserve_total";
pub const METRIC_QUEUE_COMPLETE_TOTAL: &str = "fitz_queue_complete_total";
pub const METRIC_QUEUE_RELEASE_TOTAL: &str = "fitz_queue_release_total";
pub const METRIC_QUEUE_EXTEND_TOTAL: &str = "fitz_queue_extend_total";
pub const METRIC_QUEUE_ENQUEUE_LATENCY_MS: &str = "fitz_queue_enqueue_latency_ms";
pub const METRIC_QUEUE_RESERVE_LATENCY_MS: &str = "fitz_queue_reserve_latency_ms";

// Gauges
pub const METRIC_CONNECTIONS_ACTIVE: &str = "fitz_connections_active";
pub const METRIC_SESSIONS_ACTIVE: &str = "fitz_sessions_active";
pub const METRIC_SESSION_CLEANUP_PENDING: &str = "fitz_session_cleanup_pending";
pub const METRIC_SESSION_CLEANUP_OLDEST_AGE_MS: &str = "fitz_session_cleanup_oldest_age_ms";
pub const METRIC_MAILBOX_DEPTH: &str = "fitz_mailbox_depth";
pub const METRIC_MESSAGES_PENDING: &str = "fitz_messages_pending";

// Histograms (latency in milliseconds)
pub const METRIC_MESSAGE_LATENCY: &str = "fitz_message_latency_ms";
pub const METRIC_QUEUE_WAIT_LATENCY: &str = "fitz_queue_wait_us";
pub const METRIC_TLV_CODEC_LATENCY: &str = "fitz_tlv_codec_latency_us";
pub const METRIC_ROUTE_MATCH_LATENCY: &str = "fitz_route_match_latency_us";
pub const METRIC_DOMAIN_OPERATION_LATENCY: &str = "fitz_domain_operation_latency_ms";
pub const METRIC_PERMISSION_CHECK_LATENCY: &str = "fitz_permission_check_latency_us";
pub const METRIC_INGRESS_AUTH_ROUTE_LATENCY: &str = "fitz_ingress_auth_route_latency_us";
pub const METRIC_INGRESS_DOMAIN_DISPATCH_LATENCY: &str = "fitz_ingress_domain_dispatch_latency_us";
pub const METRIC_INGRESS_DOMAIN_BACKPRESSURE_WAIT_LATENCY: &str =
    "fitz_ingress_domain_backpressure_wait_latency_us";
pub const METRIC_INGRESS_FRAME_TOTAL_LATENCY: &str = "fitz_ingress_frame_total_latency_us";
pub const METRIC_SESSION_FRAME_PROCESS_LATENCY: &str = "fitz_session_frame_process_latency_us";
pub const METRIC_SESSION_TLV_DECODE_LATENCY: &str = "fitz_session_tlv_decode_latency_us";
pub const METRIC_SESSION_MUX_ROUTE_LATENCY: &str = "fitz_session_mux_route_latency_us";
pub const METRIC_SESSION_INGRESS_HANDOFF_LATENCY: &str = "fitz_session_ingress_handoff_latency_us";
pub const METRIC_OUTBOUND_DELIVER_LATENCY: &str = "fitz_outbound_deliver_latency_us";
pub const METRIC_OUTBOUND_ENCODE_LATENCY: &str = "fitz_outbound_encode_latency_us";
pub const METRIC_OUTBOUND_SEND_LATENCY: &str = "fitz_outbound_send_latency_us";
pub const METRIC_TCP_CHANNEL_HANDOFF_LATENCY: &str = "fitz_tcp_channel_handoff_latency_us";
pub const METRIC_WS_CHANNEL_HANDOFF_LATENCY: &str = "fitz_ws_channel_handoff_latency_us";
pub const METRIC_QUEUE_RECOVERY_INDEX_LOAD_LATENCY: &str =
    "fitz_queue_recovery_index_load_latency_us";
pub const METRIC_QUEUE_RECOVERY_FALLBACK_SCAN_LATENCY: &str =
    "fitz_queue_recovery_fallback_scan_latency_us";
pub const METRIC_QUEUE_ENQUEUE_COMMIT_LATENCY: &str = "fitz_queue_enqueue_commit_latency_us";
pub const METRIC_QUEUE_RECEIVE_HYDRATE_LATENCY: &str = "fitz_queue_receive_hydrate_latency_us";
pub const METRIC_QUEUE_REDELIVERY_UPDATE_LATENCY: &str = "fitz_queue_redelivery_update_latency_us";
pub const METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY: &str = "fitz_queue_actor_lock_hold_latency_us";
pub const METRIC_QUEUE_ACTOR_EXECUTION_LATENCY: &str = "fitz_queue_actor_execution_latency_us";

// ============================================================================
// ATTRIBUTE KEYS (for structured logging and span fields)
// ============================================================================

pub const ATTR_MESSAGE_ID: &str = "message_id";
pub const ATTR_PARENT_MESSAGE_ID: &str = "parent_message_id";
pub const ATTR_CAUSATION_ID: &str = "causation_id";

pub const ATTR_ROUTE: &str = "route";
pub const ATTR_ROUTE_FAMILY: &str = "route_family";
pub const ATTR_DOMAIN: &str = "domain";
pub const ATTR_REALM: &str = "realm";
pub const ATTR_AREA: &str = "area";
pub const ATTR_RESOURCE: &str = "resource";

pub const ATTR_SESSION_ID: &str = "session_id";
pub const ATTR_CONNECTION_ID: &str = "connection_id";
pub const ATTR_PEER_ADDR: &str = "peer_addr";

pub const ATTR_ACTOR_ID: &str = "actor_id";
pub const ATTR_OPERATION: &str = "operation";

pub const ATTR_AUTH_METHOD: &str = "auth_method";
pub const ATTR_PERMISSION_RESULT: &str = "permission_result";
pub const ATTR_ERROR_TYPE: &str = "error_type";
pub const ATTR_ERROR_REASON: &str = "error_reason";

pub const ATTR_PATTERN: &str = "pattern";
pub const ATTR_MATCH_COUNT: &str = "match_count";

pub const ATTR_PROTOCOL: &str = "protocol";
pub const ATTR_FRAME_SIZE: &str = "frame_size";

pub const ATTR_TRACE_ID: &str = "trace_id";
pub const ATTR_SPAN_ID: &str = "span_id";
pub const ATTR_SERVICE_NAME: &str = "service.name";
pub const ATTR_SERVICE_VERSION: &str = "service.version";
pub const ATTR_SERVICE_INSTANCE_ID: &str = "service.instance.id";
pub const ATTR_DEPLOYMENT_ENVIRONMENT: &str = "deployment.environment";

// ============================================================================
// SAMPLING CONFIGURATION
// ============================================================================

/// Sampling ratio for hot paths (0.1% = sample 1 out of 1000)
pub const SAMPLING_RATIO_HOT_PATH: f64 = 0.001;

/// Always sample critical paths (100%)
pub const SAMPLING_RATIO_ALWAYS: f64 = 1.0;

// ============================================================================
// ERROR TYPES (for standardized error classification)
// ============================================================================

pub const ERROR_TYPE_MALFORMED_FRAME: &str = "malformed_frame";
pub const ERROR_TYPE_CODEC_ERROR: &str = "codec_error";
pub const ERROR_TYPE_ROUTE_NOT_FOUND: &str = "route_not_found";
pub const ERROR_TYPE_AUTH_FAILED: &str = "auth_failed";
pub const ERROR_TYPE_PERMISSION_DENIED: &str = "permission_denied";
pub const ERROR_TYPE_DELIVERY_FAILED: &str = "delivery_failed";
pub const ERROR_TYPE_ACTOR_ERROR: &str = "actor_error";
pub const ERROR_TYPE_TIMEOUT: &str = "timeout";
