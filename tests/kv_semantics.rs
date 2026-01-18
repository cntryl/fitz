//! KV domain semantics tests
//!
//! Tests specific KV operation semantics: insert vs put, scan ordering,
//! delete range behavior, error conditions, and boundary cases.

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
fn should_distinguish_put_insert_semantics() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Act: Begin
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/semantics"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    // Act: First insert succeeds
    let insert1 = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/semantics"),
            payload: KvMessage::Insert {
                key: Bytes::from("key1"),
                value: Bytes::from("value1"),
            },
        },
        &mut ctx,
    );
    assert!(matches!(insert1, KvResponse::InsertOk));

    // Act: Duplicate insert fails with AlreadyExists
    let insert2 = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/semantics"),
            payload: KvMessage::Insert {
                key: Bytes::from("key1"),
                value: Bytes::from("value2"),
            },
        },
        &mut ctx,
    );
    assert!(matches!(insert2, KvResponse::Error { error: e } if e.contains("AlreadyExists")));

    // Act: Put overwrites
    let put_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/semantics"),
            payload: KvMessage::Put {
                key: Bytes::from("key1"),
                value: Bytes::from("value2"),
            },
        },
        &mut ctx,
    );
    assert!(matches!(put_response, KvResponse::PutOk));

    // Assert: Verify overwrite
    let get_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/semantics"),
            payload: KvMessage::Get {
                key: Bytes::from("key1"),
            },
        },
        &mut ctx,
    );

    match get_response {
        KvResponse::GetResult { found, value } => {
            assert!(found);
            assert_eq!(value, Some(Bytes::from("value2")));
        }
        _ => panic!("Expected GetResult"),
    }
}

#[test]
fn should_return_not_found_for_missing_keys() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/missing"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    // Act: Get non-existent key
    let get_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/missing"),
            payload: KvMessage::Get {
                key: Bytes::from("does_not_exist"),
            },
        },
        &mut ctx,
    );

    // Assert
    match get_response {
        KvResponse::GetResult { found, value } => {
            assert!(!found);
            assert_eq!(value, None);
        }
        _ => panic!("Expected GetResult"),
    }
}

#[test]
fn should_delete_keys_successfully() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Setup
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/delete"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/delete"),
            payload: KvMessage::Put {
                key: Bytes::from("to_delete"),
                value: Bytes::from("value"),
            },
        },
        &mut ctx,
    );

    // Act: Delete the key
    let delete_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/delete"),
            payload: KvMessage::Delete {
                key: Bytes::from("to_delete"),
            },
        },
        &mut ctx,
    );
    assert!(matches!(delete_response, KvResponse::DeleteOk));

    // Act: Verify deletion
    let get_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/delete"),
            payload: KvMessage::Get {
                key: Bytes::from("to_delete"),
            },
        },
        &mut ctx,
    );

    // Assert
    match get_response {
        KvResponse::GetResult { found, value } => {
            assert!(!found);
            assert_eq!(value, None);
        }
        _ => panic!("Expected GetResult"),
    }
}

#[test]
fn should_handle_delete_range_with_boundaries() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Setup: Insert range of keys
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/range"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    for i in 0..10 {
        actor.handle(
            KvRequest {
                id: Uuid::new_v4(),
                route: Route::new("kv://test/area/range"),
                payload: KvMessage::Put {
                    key: Bytes::from(format!("key{:02}", i)),
                    value: Bytes::from(format!("value{}", i)),
                },
            },
            &mut ctx,
        );
    }

    // Act: Delete range from key03 to key07 (exclusive end)
    let delete_range_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/range"),
            payload: KvMessage::DeleteRange {
                start: Bytes::from("key03"),
                end: Bytes::from("key07"),
            },
        },
        &mut ctx,
    );
    assert!(matches!(delete_range_response, KvResponse::DeleteRangeOk { .. }));

    // Assert: Keys outside range should exist, keys inside should not
    for i in 0..10 {
        let get_response = actor.handle(
            KvRequest {
                id: Uuid::new_v4(),
                route: Route::new("kv://test/area/range"),
                payload: KvMessage::Get {
                    key: Bytes::from(format!("key{:02}", i)),
                },
            },
            &mut ctx,
        );

        match get_response {
            KvResponse::GetResult { found, .. } => {
                // Keys 0-2 should exist, 3-6 should be deleted, 7-9 should exist
                if i < 3 || i >= 7 {
                    assert!(found, "Key {} should exist", i);
                } else {
                    assert!(!found, "Key {} should be deleted", i);
                }
            }
            _ => panic!("Expected GetResult"),
        }
    }
}

#[test]
fn should_reject_operations_outside_transaction() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Act: Try to get without Begin
    let get_response = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/no_tx"),
            payload: KvMessage::Get {
                key: Bytes::from("any_key"),
            },
        },
        &mut ctx,
    );

    // Assert
    assert!(matches!(
        get_response,
        KvResponse::Error { error: e } if e.contains("NoActiveTx")
    ));
}

#[test]
fn should_reject_duplicate_begin() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Act: Begin first transaction
    let begin1 = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/duplicate"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );
    assert!(matches!(begin1, KvResponse::BeginOk { .. }));

    // Act: Try to Begin again
    let begin2 = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/duplicate"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    // Assert
    assert!(matches!(
        begin2,
        KvResponse::Error { error: e } if e.contains("TxAlreadyActive")
    ));
}

#[test]
fn should_enforce_transaction_scope() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Setup: Begin transaction for resource A
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/scope_test_a"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    // Act: Try to operate on different resource (scope violation)
    let put_wrong_scope = actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/scope_test_b"),
            payload: KvMessage::Put {
                key: Bytes::from("key"),
                value: Bytes::from("value"),
            },
        },
        &mut ctx,
    );

    // Assert
    assert!(matches!(
        put_wrong_scope,
        KvResponse::Error { error: e } if e.contains("TxScopeViolation")
    ));
}
