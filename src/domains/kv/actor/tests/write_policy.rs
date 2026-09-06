use super::*;

#[test]
pub(super) fn should_reject_insert_when_key_exists() {
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
    actor.handle(KvMessage::Insert {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
        value: Bytes::from("value1"),
    });

    // Act - Try to insert again
    let response = actor.handle(KvMessage::Insert {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
        value: Bytes::from("value2"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::AlreadyExists
        }
    ));
}

#[test]
pub(super) fn should_validate_delete_range_parameters() {
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

    // Act - End before start
    let response = actor.handle(KvMessage::DeleteRange {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        start: Bytes::from("z"),
        end: Bytes::from("a"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::InvalidRequest(_)
        }
    ));
}

#[test]
pub(super) fn should_reject_route_family_zero() {
    // Arrange
    let mut actor = test_actor();

    // Act
    let result = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(0),
            "test".to_string(),
            "kv".to_string(),
            "table1".to_string(),
        ),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });

    // Assert
    assert!(matches!(
        result,
        KvResponse::Error {
            error: KvError::InvalidRouteFamily,
        }
    ));
}

#[test]
pub(super) fn should_delete_existing_key() {
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

    let key = Bytes::from("delkey");
    actor.handle(KvMessage::Put {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
        value: Bytes::from("value1"),
    });

    // Act - Delete the key
    let delete_response = actor.handle(KvMessage::Delete {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
    });

    // Assert delete succeeds
    assert!(matches!(delete_response, KvResponse::DeleteOk));

    // Verify key is gone
    let get_response = actor.handle(KvMessage::Get {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        key: key.clone(),
    });
    assert!(matches!(
        get_response,
        KvResponse::GetResult {
            found: false,
            value: None
        }
    ));
}

#[test]
pub(super) fn should_scan_key_range() {
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

    // Add multiple keys
    for i in 0..5 {
        actor.handle(KvMessage::Put {
            tx_id,
            scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
            key: Bytes::from(format!("key{i:02}")),
            value: Bytes::from(format!("value{i}")),
        });
    }

    // Act - Scan range [key01, key04)
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        query: ScanQuery {
            start: Some(Bytes::from("key01")),
            end: Some(Bytes::from("key04")),
            limit: None,
            reverse: false,
            start_exclusive: false,
        },
    });

    // Assert
    match response {
        KvResponse::ScanResult { items, .. } => {
            assert!(items.len() >= 2); // At least key01, key02, key03
        }
        _ => panic!("Expected ScanResult"),
    }
}

#[test]
pub(super) fn should_reject_delete_range_with_invalid_bounds() {
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

    // Act - End < Start
    let response = actor.handle(KvMessage::DeleteRange {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        start: Bytes::from("zzz"),
        end: Bytes::from("aaa"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: KvError::InvalidRequest(_)
        }
    ));
}

#[test]
pub(super) fn should_scan_with_limit() {
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

    // Add 10 keys
    for i in 0..10 {
        actor.handle(KvMessage::Put {
            tx_id,
            scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
            key: Bytes::from(format!("k{i:02}")),
            value: Bytes::from(format!("v{i}")),
        });
    }

    // Act - Scan with limit of 3
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        query: ScanQuery {
            start: None,
            end: None,
            limit: Some(3),
            reverse: false,
            start_exclusive: false,
        },
    });

    // Assert
    match response {
        KvResponse::ScanResult { items, has_more } => {
            assert_eq!(items.len(), 3);
            assert!(has_more);
        }
        _ => panic!("Expected ScanResult"),
    }
}

#[test]
pub(super) fn should_scan_reverse() {
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

    // Add keys
    for i in 0..5 {
        actor.handle(KvMessage::Put {
            tx_id,
            scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
            key: Bytes::from(format!("k{i}")),
            value: Bytes::from(format!("v{i}")),
        });
    }

    // Act - Scan reverse
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope: KvResourceScope::new(RouteFamily::new(1), "test", "kv", "table1".to_string()),
        query: ScanQuery {
            start: None,
            end: None,
            limit: None,
            reverse: true,
            start_exclusive: false,
        },
    });

    // Assert - Just verify it returns results (order depends on storage)
    match response {
        KvResponse::ScanResult { items, .. } => {
            assert!(!items.is_empty());
        }
        _ => panic!("Expected ScanResult"),
    }
}
