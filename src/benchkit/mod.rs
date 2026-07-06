//! Benchmark utilities and helpers
//!
//! This module provides reusable benchmark infrastructure for performance testing,
//! including common setup helpers, mock factories, and test data generators.
//! Available when compiled with bench configuration.

pub mod live_sink;
pub mod queue;
pub mod rpc;
pub mod runtime;
pub mod storage;
pub mod stream;
pub mod transport;

pub use live_sink::{
    create_bench_lease_sink, create_bench_notice_sink, create_bench_queue_sink,
    create_bench_rpc_sink, create_bench_rpc_sink_with_metrics,
    create_bench_rpc_sink_with_route_pending_capacity, create_bench_rpc_sink_with_timeout,
    create_bench_schedule_sink, create_bench_stream_sink, create_bench_stream_sink_with_layout,
    create_write_heavy_bench_stream_sink, drain_frame_queue_sinks_after_each_count,
    drain_frame_queue_sinks_after_total_count, register_session_counting_sink,
    register_session_queue_sink, route_frame, route_frame_to_address, route_raw_frame,
    session_inbox_route, wait_for_counting_sinks_each_count, wait_for_counting_sinks_total_count,
    CountingSink, DirectLeaseAcquireRelease, FrameQueueSink,
};
pub use queue::{create_bench_queue_actor, create_local_bench_queue_actor};
pub use rpc::create_bench_rpc_context;
pub use runtime::shared_bench_runtime;
pub use storage::{
    create_bench_store, create_bench_store_with_cfs, create_local_bench_store,
    create_write_heavy_bench_store, create_write_heavy_bench_store_with_cfs,
};
pub use stream::{
    create_bench_event_payloads, create_bench_stream_actor, create_bench_stream_actor_with_layout,
    create_local_bench_stream_actor, create_local_bench_stream_actor_with_layout,
};
pub use transport::{
    build_kv_begin, build_kv_commit, build_kv_put, build_kv_rollback,
    build_lease_acquire_immediate, build_lease_extend, build_lease_query, build_lease_release,
    build_notice_publish, build_notice_subscribe, build_notice_unsubscribe, build_queue_complete,
    build_queue_dequeue, build_queue_dequeue_batch, build_queue_enqueue, build_queue_watch,
    build_rpc_request, build_rpc_response_frame, build_rpc_response_frame_bytes,
    build_rpc_subscribe, build_rpc_subscribe_with_max_concurrent, build_schedule_create,
    build_schedule_create_batch, build_stream_append, build_stream_append_with_metadata,
    build_stream_begin, build_stream_commit, build_stream_get_metadata, build_stream_last,
    build_stream_read, build_stream_read_with_limit, build_stream_subscribe,
    count_stream_read_records_from_payload, ensure_schedule_ok, extract_single_tlv_field,
    parse_kv_response, parse_kv_tx_id, parse_lease_extend_token_response, parse_lease_response,
    parse_lease_token_response, parse_notice_delivery, parse_notice_response,
    parse_notice_subscription_id, parse_queue_response, parse_rpc_response,
    parse_schedule_response, parse_stream_read_record_count, parse_stream_response,
    parse_stream_session_id, NoticeDelivery,
};
