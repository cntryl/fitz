use super::*;

#[test]
pub(super) fn should_begin_transaction_for_resource() {
    // Arrange
    let mut actor = test_actor();

    // Act
    let response = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });

    // Assert
    assert!(matches!(response, KvResponse::BeginOk { tx_id: _ }));
}

#[test]
pub(super) fn should_enforce_transaction_scope_to_single_resource() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });

    let KvResponse::BeginOk { tx_id } = begin_response else {
        panic!("Expected BeginOk");
    };

    // Act - Try to operate on different resource
    let response = actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table2".to_string()),
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::TxScopeViolation { .. }
        }
    ));
}

#[test]
pub(super) fn should_reject_kv_operation_given_route_family_mismatch_without_mutation() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id } = begin_response else {
        panic!("Expected BeginOk");
    };

    // Act
    let response = actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(2), "test", "kv", "table1".to_string()),
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::InvalidRouteFamily
        }
    ));
}

#[test]
pub(super) fn should_reject_operations_without_active_transaction() {
    // Arrange
    let mut actor = test_actor();

    // Act
    let response = actor.handle(KvMessage::Get {
        tx_id: 999,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: Bytes::from("key"),
    });
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::InvalidTxId
        }
    ));

    // Verify: Put also rejected without active transaction
    let response = actor.handle(KvMessage::Put {
        tx_id: 999,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::InvalidTxId
        }
    ));
}

#[test]
pub(super) fn should_preserve_kv_scope_given_follow_up_put_on_same_transaction() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });

    let KvResponse::BeginOk { tx_id } = begin_response else {
        panic!("Expected BeginOk");
    };

    let key = Bytes::from("testkey");
    let value = Bytes::from("testvalue");

    // Act
    let put_response = actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
        value: value.clone(),
    });
    assert!(matches!(put_response, KvResponse::PutOk));

    // Step 2: retrieve the value
    let get_response = actor.handle(KvMessage::Get {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
    });

    // Assert
    match get_response {
        KvResponse::GetResult {
            found: true,
            value: Some(v),
        } => assert_eq!(v, value),
        _ => panic!("Expected GetResult with value"),
    }
}

#[test]
pub(super) fn should_commit_empty_transaction() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id } = begin_response else {
        panic!("Expected BeginOk");
    };

    // Act - Commit immediately without writing anything
    let response = actor.handle(KvMessage::Commit {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1"),
    });

    // Assert
    assert!(matches!(response, KvResponse::CommitOk));
}

#[test]
pub(super) fn should_rollback_transaction() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id } = begin_response else {
        panic!("Expected BeginOk");
    };

    let key = Bytes::from("rollbackkey");
    actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
        value: Bytes::from("will_rollback"),
    });

    // Act - Rollback
    let response = actor.handle(KvMessage::Rollback {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1"),
    });

    // Assert
    assert!(matches!(response, KvResponse::RollbackOk));

    // Verify transaction is no longer active
    let get_response = actor.handle(KvMessage::Get {
        tx_id: 9999,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key,
    });
    assert!(matches!(
        get_response,
        KvResponse::Error {
            error: KvError::InvalidTxId
        }
    ));
}

#[test]
pub(super) fn should_isolate_resources_in_same_family() {
    // Arrange
    let mut actor = test_actor();

    // Begin transaction for resource1
    let begin_response = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id } = begin_response else {
        panic!("Expected BeginOk");
    };

    let key = Bytes::from("testkey");
    actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
        value: Bytes::from("value1"),
    });

    // Act - Try to put to different resource in same transaction (should fail)
    let response = actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table2".to_string()),
        key: key.clone(),
        value: Bytes::from("value2"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::TxScopeViolation { .. }
        }
    ));
}

#[test]
pub(super) fn should_handle_key_scoping_correctly() {
    // Arrange
    let mut actor1 = test_actor();
    let mut actor2 = test_actor();

    // Both start transactions for different resources
    let begin_response1 = actor1.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id: tx_id1 } = begin_response1 else {
        panic!("Expected BeginOk");
    };

    let begin_response2 = actor2.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table2".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id: tx_id2 } = begin_response2 else {
        panic!("Expected BeginOk");
    };

    let key = Bytes::from("samekey");

    // Act - Put same key to both resources
    actor1.handle(KvMessage::Put {
        tx_id: tx_id1,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
        value: Bytes::from("value1"),
    });

    actor2.handle(KvMessage::Put {
        tx_id: tx_id2,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table2".to_string()),
        key: key.clone(),
        value: Bytes::from("value2"),
    });

    // Assert - Both succeed, they are isolated
    let get1 = actor1.handle(KvMessage::Get {
        tx_id: tx_id1,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
    });

    let get2 = actor2.handle(KvMessage::Get {
        tx_id: tx_id2,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table2".to_string()),
        key: key.clone(),
    });

    match (get1, get2) {
        (
            KvResponse::GetResult {
                found: true,
                value: Some(v1),
            },
            KvResponse::GetResult {
                found: true,
                value: Some(v2),
            },
        ) => {
            assert_eq!(v1, Bytes::from("value1"));
            assert_eq!(v2, Bytes::from("value2"));
        }
        _ => panic!("Expected both gets to succeed with different values"),
    }
}

#[test]
pub(super) fn should_enforce_realm_isolation_for_kv() {
    // Arrange
    let mut actor = test_actor();

    // Begin transactions in two different realms but same resource/key
    let r1 = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "realm_a".to_string(),
            "kv".to_string(),
            "users".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id: tx1 } = r1 else {
        panic!("Expected BeginOk for realm_a");
    };

    let r2 = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "realm_b".to_string(),
            "kv".to_string(),
            "users".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id: tx2 } = r2 else {
        panic!("Expected BeginOk for realm_b");
    };

    let key = Bytes::from("same_key");

    // Act
    actor.handle(KvMessage::Put {
        tx_id: tx1,
        scope: KvResourceScope::new(RouteFamily::new(1), "realm_a", "kv", "users"),
        key: key.clone(),
        value: Bytes::from("value_in_a"),
    });

    actor.handle(KvMessage::Put {
        tx_id: tx2,
        scope: KvResourceScope::new(RouteFamily::new(1), "realm_b", "kv", "users"),
        key: key.clone(),
        value: Bytes::from("value_in_b"),
    });

    // Assert - reads in each transaction return the realm-scoped value
    let get_a = actor.handle(KvMessage::Get {
        tx_id: tx1,
        scope: KvResourceScope::new(RouteFamily::new(1), "realm_a", "kv", "users"),
        key: key.clone(),
    });
    let get_b = actor.handle(KvMessage::Get {
        tx_id: tx2,
        scope: KvResourceScope::new(RouteFamily::new(1), "realm_b", "kv", "users"),
        key: key.clone(),
    });

    match (get_a, get_b) {
        (
            KvResponse::GetResult {
                found: true,
                value: Some(va),
            },
            KvResponse::GetResult {
                found: true,
                value: Some(vb),
            },
        ) => {
            assert_eq!(va, Bytes::from("value_in_a"));
            assert_eq!(vb, Bytes::from("value_in_b"));
        }
        _ => panic!("Expected realm-scoped values to be returned"),
    }
}
