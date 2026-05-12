//! Queue domain basics tests
//!
//! Contains specification-validation stubs. Realm isolation is enforced by the
//! actor and routing model and is covered elsewhere.

// ============================================================================
// REALM ISOLATION TESTS
// ============================================================================

use fitz::domains::queue::protocol::QueueKey;
use fitz::domains::queue::QueueActor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

#[allow(dead_code)]
fn make_queue_actor(realm: &str, area: &str, resource: &str) -> (QueueActor, Context<QueueActor>) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let addr = RouteAddress::new(
        family,
        Route::new(format!("queue://{}/{}/{}/enqueue", realm, area, resource)),
    );

    let db = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open store"),
    );

    let queue_key = QueueKey {
        family,
        realm: realm.to_string(),
        area: area.to_string(),
        resource: resource.to_string(),
    };
    let actor = QueueActor::new(
        family,
        queue_key,
        db,
        None,
        fitz::utils::idempotency::default_dedup_store(),
    ); // max_attempts = None = unlimited retries
    let ctx = Context::new(addr, router);

    (actor, ctx)
}

// ============================================================================
// Specification validation stubs
// ============================================================================

macro_rules! ignored_queue_stub {
        ($name:ident) => {
            #[test]
            #[ignore = "queue basics placeholder: requires full protocol harness"]
            fn $name() {
                todo!()
            }
        };
    }

    ignored_queue_stub!(should_rely_on_auth_layer_for_queue_realm_validation);
    ignored_queue_stub!(should_support_enqueue_operation);
    ignored_queue_stub!(should_support_reserve_operation_with_batch_size);
    ignored_queue_stub!(should_support_extend_operation_for_inflight_reservation);
    ignored_queue_stub!(should_support_complete_operation_with_inflight_token);
    ignored_queue_stub!(should_return_server_assigned_message_id);
    ignored_queue_stub!(should_have_inflight_token_for_exclusive_access);
    ignored_queue_stub!(should_have_visibility_timeout_for_inflight_duration);
    ignored_queue_stub!(should_have_queue_error_code_range_4000_4099);
    ignored_queue_stub!(should_use_4001_for_unauthorized_access);
    ignored_queue_stub!(should_use_4002_for_invalid_scope);
    ignored_queue_stub!(should_use_4003_for_realm_mismatch);
    ignored_queue_stub!(should_use_4010_for_queue_not_found);
    ignored_queue_stub!(should_use_4012_for_inflight_expired);
    ignored_queue_stub!(should_use_4013_for_invalid_inflight_token);
    ignored_queue_stub!(should_use_4014_for_batch_size_out_of_range);
    ignored_queue_stub!(should_complete_enqueue_reserve_complete_cycle);
    ignored_queue_stub!(should_persist_message_until_completed);
    ignored_queue_stub!(should_return_message_to_queue_on_inflight_expiry);
    ignored_queue_stub!(should_allow_inflight_extension_before_expiry);
    ignored_queue_stub!(should_batch_multiple_messages_in_reserve);
    ignored_queue_stub!(should_respect_batch_size_upper_limit);
    ignored_queue_stub!(should_reject_complete_with_wrong_inflight_token);
    ignored_queue_stub!(should_support_multiple_concurrent_consumers);
    ignored_queue_stub!(should_isolate_inflight_tokens_between_consumers);
    ignored_queue_stub!(should_distribute_messages_fairly_among_consumers);
    ignored_queue_stub!(should_reject_reserve_with_invalid_batch_size);
    ignored_queue_stub!(should_reject_extend_with_expired_lease);
    ignored_queue_stub!(should_reject_operations_without_read_scope);
    ignored_queue_stub!(should_reject_operations_without_write_scope);
    ignored_queue_stub!(should_reject_complete_without_write_scope);
    ignored_queue_stub!(should_deduplicate_complete_for_same_inflight_token);
    ignored_queue_stub!(should_allow_reenqueue_after_abandoned_inflight_reservation);
    ignored_queue_stub!(should_preserve_message_payload_bytes);
    ignored_queue_stub!(should_support_empty_message_payload);
    ignored_queue_stub!(should_assign_unique_inflight_tokens);
    ignored_queue_stub!(should_maintain_message_order_fifo);
