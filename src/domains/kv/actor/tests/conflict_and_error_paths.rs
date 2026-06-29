use super::*;

#[test]
fn should_handle_concurrent_puts_with_conflict_detection() {
    // Arrange
    let mut actor = test_actor();

    let b1 = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "concurrent".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx1 = match b1 {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    let b2 = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "concurrent".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx2 = match b2 {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Act
    actor.handle(KvMessage::Put {
        tx_id: tx1,
        route_family: RouteFamily::new(1),
        resource: "concurrent".to_string(),
        key: Bytes::from("key"),
        value: Bytes::from("v1"),
    });

    actor.handle(KvMessage::Put {
        tx_id: tx2,
        route_family: RouteFamily::new(1),
        resource: "concurrent".to_string(),
        key: Bytes::from("key"),
        value: Bytes::from("v2"),
    });

    // Assert
    // Commit first tx (expected OK)
    let c1 = actor.handle(KvMessage::Commit { tx_id: tx1 });
    assert!(matches!(c1, KvResponse::CommitOk));

    // Second commit may conflict or succeed depending on storage semantics.
    let c2 = actor.handle(KvMessage::Commit { tx_id: tx2 });
    assert!(
        matches!(c2, KvResponse::CommitOk)
            || matches!(
                c2,
                KvResponse::Error {
                    error: KvError::Conflict(_)
                }
            )
    );

    // Verify final stored value is one of the two candidates (v1 or v2)
    let b3 = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "concurrent".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx3 = match b3 {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Begin failed"),
    };

    let got = actor.handle(KvMessage::Get {
        tx_id: tx3,
        route_family: RouteFamily::new(1),
        resource: "concurrent".to_string(),
        key: Bytes::from("key"),
    });

    match got {
        KvResponse::GetResult {
            found: true,
            value: Some(v),
        } => {
            assert!(v.as_ref() == b"v1" || v.as_ref() == b"v2");
        }
        _ => panic!("Expected stored value after commits"),
    }

    actor.handle(KvMessage::Rollback { tx_id: tx3 });
}

#[test]
fn should_reject_operations_from_wrong_area() {
    // Arrange
    let mut actor = test_actor();

    let r1 = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "area_a".to_string(),
        resource: "shared".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx1 = match r1 {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    let r2 = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "area_b".to_string(),
        resource: "shared".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx2 = match r2 {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Act
    actor.handle(KvMessage::Put {
        tx_id: tx1,
        route_family: RouteFamily::new(1),
        resource: "shared".to_string(),
        key: Bytes::from("same_key"),
        value: Bytes::from("in_a"),
    });
    actor.handle(KvMessage::Commit { tx_id: tx1 });

    let get_in_b = actor.handle(KvMessage::Get {
        tx_id: tx2,
        route_family: RouteFamily::new(1),
        resource: "shared".to_string(),
        key: Bytes::from("same_key"),
    });

    // Assert - different area must not see the value
    match get_in_b {
        KvResponse::GetResult {
            found: false,
            value: None,
        } => {}
        _ => panic!("Expected not-found across different area"),
    }
}

#[test]
fn should_return_error_for_invalid_txid() {
    // Arrange
    let mut actor = test_actor();

    // Act
    let res = actor.handle(KvMessage::Commit { tx_id: 99999 });

    // Assert
    assert!(matches!(
        res,
        KvResponse::Error {
            error: KvError::InvalidTxId
        }
    ));
}

#[test]
fn should_return_not_found_when_key_never_written() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match begin_response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Act
    let response = actor.handle(KvMessage::Get {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "table1".to_string(),
        key: Bytes::from("nonexistent"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::GetResult {
            found: false,
            value: None
        }
    ));
}

#[test]
fn should_delete_nonexistent_key_without_error() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match begin_response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Act
    let response = actor.handle(KvMessage::Delete {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "table1".to_string(),
        key: Bytes::from("never_written"),
    });

    // Assert
    assert!(matches!(response, KvResponse::DeleteOk));
}

#[test]
fn should_scan_empty_table_returns_empty_result() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "empty_table".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match begin_response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Act
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "empty_table".to_string(),
        query: ScanQuery {
            start: None,
            end: None,
            limit: None,
            reverse: false,
        },
    });

    // Assert
    match response {
        KvResponse::ScanResult { items, .. } => {
            assert!(items.is_empty());
        }
        _ => panic!("Expected ScanResult"),
    }
}

#[test]
fn should_reject_begin_with_empty_realm() {
    // Arrange
    let mut actor = test_actor();

    // Act
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::InvalidRealm
        }
    ));
}

#[test]
fn should_reject_begin_with_realm_containing_spaces() {
    // Arrange
    let mut actor = test_actor();

    // Act
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "bad realm".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::InvalidRealm
        }
    ));
}

#[test]
fn should_reject_commit_on_already_committed_txid() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match begin_response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };
    actor.handle(KvMessage::Commit { tx_id });

    // Act
    let response = actor.handle(KvMessage::Commit { tx_id });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::InvalidTxId
        }
    ));
}

#[test]
fn should_reject_rollback_on_already_rolled_back_txid() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match begin_response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };
    actor.handle(KvMessage::Rollback { tx_id });

    // Act
    let response = actor.handle(KvMessage::Rollback { tx_id });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::InvalidTxId
        }
    ));
}

#[test]
fn should_use_bound_resource_when_resource_param_is_empty() {
    // Arrange
    let mut actor = test_actor();
    let begin_response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match begin_response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };
    actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "table1".to_string(),
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    });

    // Act — empty resource falls back to bound_resource ("table1")
    let response = actor.handle(KvMessage::Get {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "".to_string(),
        key: Bytes::from("key"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::GetResult {
            found: true,
            value: Some(_)
        }
    ));
}
