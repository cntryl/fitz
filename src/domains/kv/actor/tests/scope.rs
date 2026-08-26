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
fn should_reject_kv_commit_given_any_scope_component_mismatch() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());

    // Act
    let response = actor.handle(KvMessage::Commit {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "other", "area", "table"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::RealmMismatch
        }
    ));
    assert_eq!(actor.transaction_count(), 1);
    assert!(matches!(
        actor.handle(KvMessage::Rollback { tx_id, scope }),
        KvResponse::RollbackOk
    ));
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
