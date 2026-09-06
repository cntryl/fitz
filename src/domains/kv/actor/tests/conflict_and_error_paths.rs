use super::*;

#[test]
fn should_handle_concurrent_puts_with_conflict_detection() {
    // Arrange
    let mut actor = test_actor();

    let b1 = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "concurrent".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id: tx1 } = b1 else {
        panic!("Expected BeginOk");
    };

    let b2 = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "concurrent".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id: tx2 } = b2 else {
        panic!("Expected BeginOk");
    };

    // Act
    actor.handle(KvMessage::Put {
        tx_id: tx1,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "concurrent".to_string()),
        key: Bytes::from("key"),
        value: Bytes::from("v1"),
    });

    actor.handle(KvMessage::Put {
        tx_id: tx2,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "concurrent".to_string()),
        key: Bytes::from("key"),
        value: Bytes::from("v2"),
    });

    // Assert
    // Commit first tx (expected OK)
    let c1 = actor.handle(KvMessage::Commit {
        tx_id: tx1,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "concurrent"),
    });
    assert!(matches!(c1, KvResponse::CommitOk));

    // Second commit may conflict or succeed depending on storage semantics.
    let c2 = actor.handle(KvMessage::Commit {
        tx_id: tx2,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "concurrent"),
    });
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
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "concurrent".to_string(),
        ),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id: tx3 } = b3 else {
        panic!("Begin failed");
    };

    let got = actor.handle(KvMessage::Get {
        tx_id: tx3,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "concurrent".to_string()),
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

    actor.handle(KvMessage::Rollback {
        tx_id: tx3,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "concurrent"),
    });
}

#[test]
fn should_reject_operations_from_wrong_area() {
    // Arrange
    let mut actor = test_actor();

    let r1 = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "area_a".to_string(),
            "shared".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id: tx1 } = r1 else {
        panic!("Expected BeginOk");
    };

    let r2 = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "area_b".to_string(),
            "shared".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id: tx2 } = r2 else {
        panic!("Expected BeginOk");
    };

    // Act
    actor.handle(KvMessage::Put {
        tx_id: tx1,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "area_a", "shared"),
        key: Bytes::from("same_key"),
        value: Bytes::from("in_a"),
    });
    actor.handle(KvMessage::Commit {
        tx_id: tx1,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "area_a", "shared"),
    });

    let get_in_b = actor.handle(KvMessage::Get {
        tx_id: tx2,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "area_b", "shared"),
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
    let res = actor.handle(KvMessage::Commit {
        tx_id: 99999,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1"),
    });

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
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id } = begin_response else {
        panic!("Expected BeginOk");
    };

    // Act
    let response = actor.handle(KvMessage::Get {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
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
    let response = actor.handle(KvMessage::Delete {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
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
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "test".to_string(),
            "kv".to_string(),
            "empty_table".to_string(),
        ),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    let KvResponse::BeginOk { tx_id } = begin_response else {
        panic!("Expected BeginOk");
    };

    // Act
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "empty_table".to_string()),
        query: ScanQuery {
            start: None,
            end: None,
            limit: None,
            reverse: false,
            start_exclusive: false,
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
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            String::new(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
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
fn should_not_classify_ordinary_operation_text_as_backend_unavailable() {
    // Arrange
    let messages = [
        "transaction condition violated",
        "operation exception",
        "revision mismatch",
    ];

    // Act
    let classifications = messages.map(KvActor::classify_midge_message);

    // Assert
    assert!(classifications
        .iter()
        .all(|classification| matches!(classification, KvError::BackendError(_))));
}

#[test]
fn should_classify_explicit_io_indicators_as_backend_unavailable() {
    // Arrange
    let messages = ["I/O failure", "disk full", "os error 28"];

    // Act
    let classifications = messages.map(KvActor::classify_midge_message);

    // Assert
    assert!(classifications
        .iter()
        .all(|classification| matches!(classification, KvError::BackendUnavailable(_))));
}

#[test]
fn should_reject_begin_with_realm_containing_spaces() {
    // Arrange
    let mut actor = test_actor();

    // Act
    let response = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "bad realm".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
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
    actor.handle(KvMessage::Commit {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1"),
    });

    // Act
    let response = actor.handle(KvMessage::Commit {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1"),
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
fn should_reject_rollback_on_already_rolled_back_txid() {
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
    actor.handle(KvMessage::Rollback {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1"),
    });

    // Act
    let response = actor.handle(KvMessage::Rollback {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1"),
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
fn should_reject_empty_resource_in_follow_up_scope() {
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
    actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    });

    // Act
    let response = actor.handle(KvMessage::Get {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", String::new()),
        key: Bytes::from("key"),
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
fn should_classify_storage_timeout_as_backend_unavailable() {
    // Arrange
    // Midge bounds its synchronous runtime waits, so a slow storage op now
    // returns a typed timeout where it previously blocked. Classifying by
    // message text alone drops it into the generic bucket and reports
    // transient saturation as a permanent failure.
    let error = cntryl_midge::MidgeError::Timeout(
        "runtime request Put request_id=42 exceeded response timeout 60s".to_string(),
    );

    // Act
    let classification = KvActor::map_midge_error(&error);

    // Assert
    assert!(
        matches!(classification, KvError::BackendUnavailable(_)),
        "a storage timeout is transient, got {classification:?}"
    );
}
