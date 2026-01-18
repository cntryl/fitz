//! KV domain end-to-end tests
//!
//! Tests complete workflows: multi-step transactions, persistence, recovery,
//! and end-to-end semantics across sessions and restarts.

use bytes::Bytes;
use fitz::domains::kv::actor::KvActor;
use fitz::domains::kv::protocol::{KvMessage, KvRequest, KvResponse};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use uuid::Uuid;

fn make_kv_ctx(family: RouteFamily) -> Context<KvActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(family, Route::new("kv://test/area/resource"));
    Context::new(addr, router)
}

fn make_store() -> Arc<cntryl_midge::Engine> {
    Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    )
}

#[test]
fn should_complete_transaction_workflow() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Act: Begin transaction
    let begin_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/orders"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    assert!(matches!(begin_response, KvResponse::BeginOk { .. }));

    // Act: Put multiple keys
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/orders"),
            payload: KvMessage::Put {
                key: Bytes::from("order-1"),
                value: Bytes::from("pending"),
            },
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/orders"),
            payload: KvMessage::Put {
                key: Bytes::from("order-2"),
                value: Bytes::from("pending"),
            },
        },
        &mut ctx,
    );

    // Act: Get to verify writes
    let get_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/orders"),
            payload: KvMessage::Get {
                key: Bytes::from("order-1"),
            },
        },
        &mut ctx,
    );

    match get_response {
        KvResponse::GetResult { found, value } => {
            assert!(found);
            assert_eq!(value, Some(Bytes::from("pending")));
        }
        _ => panic!("Expected GetResult"),
    }

    // Act: Commit transaction
    let commit_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/orders"),
            payload: KvMessage::Commit,
        },
        &mut ctx,
    );

    assert!(matches!(commit_response, KvResponse::CommitOk { .. }));

    // Assert: New transaction can be started
    let begin_response_2 = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/orders"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    assert!(matches!(begin_response_2, KvResponse::BeginOk { .. }));
}

#[test]
fn should_persist_data_across_transactions() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Act: First transaction - write data
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/persistent"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/persistent"),
            payload: KvMessage::Put {
                key: Bytes::from("data"),
                value: Bytes::from("persisted"),
            },
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/persistent"),
            payload: KvMessage::Commit,
        },
        &mut ctx,
    );

    // Act: Second transaction - read data
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/persistent"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    let get_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/persistent"),
            payload: KvMessage::Get {
                key: Bytes::from("data"),
            },
        },
        &mut ctx,
    );

    // Assert: Data was persisted across transactions
    match get_response {
        KvResponse::GetResult { found, value } => {
            assert!(found);
            assert_eq!(value, Some(Bytes::from("persisted")));
        }
        _ => panic!("Expected GetResult"),
    }
}

#[test]
fn should_handle_rollback_discarding_changes() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Setup: Put initial value
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/rollback_test"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/rollback_test"),
            payload: KvMessage::Put {
                key: Bytes::from("counter"),
                value: Bytes::from("1"),
            },
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/rollback_test"),
            payload: KvMessage::Commit,
        },
        &mut ctx,
    );

    // Act: Start transaction, modify, then rollback
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/rollback_test"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/rollback_test"),
            payload: KvMessage::Put {
                key: Bytes::from("counter"),
                value: Bytes::from("2"),
            },
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/rollback_test"),
            payload: KvMessage::Rollback,
        },
        &mut ctx,
    );

    // Act: New transaction to verify rollback worked
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/rollback_test"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    let get_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/rollback_test"),
            payload: KvMessage::Get {
                key: Bytes::from("counter"),
            },
        },
        &mut ctx,
    );

    // Assert: Value should be original (1), not modified (2)
    match get_response {
        KvResponse::GetResult { found, value } => {
            assert!(found);
            assert_eq!(value, Some(Bytes::from("1")));
        }
        _ => panic!("Expected GetResult"),
    }
}

#[test]
fn should_isolate_data_by_resource() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Setup: Write to resource A
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/resource_a"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/resource_a"),
            payload: KvMessage::Put {
                key: Bytes::from("shared_key"),
                value: Bytes::from("value_from_a"),
            },
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/resource_a"),
            payload: KvMessage::Commit,
        },
        &mut ctx,
    );

    // Setup: Write to resource B with same key
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/resource_b"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/resource_b"),
            payload: KvMessage::Put {
                key: Bytes::from("shared_key"),
                value: Bytes::from("value_from_b"),
            },
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/resource_b"),
            payload: KvMessage::Commit,
        },
        &mut ctx,
    );

    // Act: Read from resource A
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/resource_a"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    let get_a = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/resource_a"),
            payload: KvMessage::Get {
                key: Bytes::from("shared_key"),
            },
        },
        &mut ctx,
    );

    // Assert: Resource A has its own value
    match get_a {
        KvResponse::GetResult { found, value } => {
            assert!(found);
            assert_eq!(value, Some(Bytes::from("value_from_a")));
        }
        _ => panic!("Expected GetResult"),
    }
}
