use super::*;

#[test]
fn should_return_named_transaction_introspection_values() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());

    // Act
    let resource_scope = actor.resource_scope_for_tx(tx_id);
    let snapshots = actor.active_transaction_snapshots();

    // Assert
    assert_eq!(resource_scope, Some(scope.clone()));
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].tx_id, tx_id);
    assert_eq!(snapshots[0].scope, scope);
}

#[test]
fn should_reject_kv_put_given_realm_mismatch_without_mutation() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm-a", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope);

    // Act
    let response = actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "realm-b", "area", "table"),
        key: Bytes::from_static(b"key"),
        value: Bytes::from_static(b"value"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::RealmMismatch
        }
    ));
    assert_eq!(actor.mutation_count_for_tx(tx_id), Some(0));
}

#[test]
fn should_reject_operation_with_area_mismatching_transaction_without_mutation() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area-a", "table");
    let tx_id = begin_with_scope(&mut actor, scope);

    // Act
    let response = actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "realm", "area-b", "table"),
        key: Bytes::from_static(b"key"),
        value: Bytes::from_static(b"value"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::TxScopeViolation { .. }
        }
    ));
    assert_eq!(actor.mutation_count_for_tx(tx_id), Some(0));
}

#[test]
fn should_preserve_kv_writes_after_mismatched_commit_domain_invariant() {
    // Arrange
    for (case, rejected_scope) in [
        KvResourceScope::new(RouteFamily::new(2), "realm", "area", "table"),
        KvResourceScope::new(RouteFamily::new(1), "other", "area", "table"),
        KvResourceScope::new(RouteFamily::new(1), "realm", "other", "table"),
        KvResourceScope::new(RouteFamily::new(1), "realm", "area", "other"),
    ]
    .into_iter()
    .enumerate()
    {
        let mut actor = test_actor();
        let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
        let tx_id = begin_with_scope(&mut actor, scope.clone());
        assert!(matches!(
            actor.handle(KvMessage::Put {
                tx_id,
                scope: scope.clone(),
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"retained")
            }),
            KvResponse::PutOk
        ));

        // Act
        let rejected = actor.handle(KvMessage::Commit {
            tx_id,
            scope: rejected_scope,
            persistence: crate::domains::CommitPersistence::Buffered,
        });
        let count = actor.transaction_count();
        let accepted = actor.handle(KvMessage::Commit {
            tx_id,
            scope: scope.clone(),
            persistence: crate::domains::CommitPersistence::Buffered,
        });
        let read_id = begin_with_scope(&mut actor, scope.clone());
        let read = actor.handle(KvMessage::Get {
            tx_id: read_id,
            scope,
            key: Bytes::from_static(b"key"),
        });

        // Assert
        match (case, rejected) {
            (
                0,
                KvResponse::Error {
                    error: KvError::InvalidRouteFamily,
                },
            )
            | (
                1,
                KvResponse::Error {
                    error: KvError::RealmMismatch,
                },
            )
            | (
                2 | 3,
                KvResponse::Error {
                    error: KvError::TxScopeViolation { .. },
                },
            ) => {}
            other => panic!("unexpected commit scope rejection: {other:?}"),
        }
        assert_eq!(count, 1);
        assert!(matches!(accepted, KvResponse::CommitOk));
        let KvResponse::GetResult {
            found: true,
            value: Some(value),
        } = read
        else {
            panic!("staged value lost after rejected commit")
        };
        assert_eq!(value.as_ref(), b"retained");
    }
}

#[test]
fn should_keep_transaction_active_when_rollback_scope_mismatches() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());

    // Act
    let response = actor.handle(KvMessage::Rollback {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "realm", "other", "table"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::TxScopeViolation { .. }
        }
    ));
    assert_eq!(actor.transaction_count(), 1);
    assert!(matches!(
        actor.handle(KvMessage::Rollback { tx_id, scope }),
        KvResponse::RollbackOk
    ));
}
