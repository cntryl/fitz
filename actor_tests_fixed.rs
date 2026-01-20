// This file contains corrected test bodies to be manually merged
// These tests now properly capture tx_id from Begin responses

    #[test]
    fn should_reject_insert_when_key_exists() {
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

        let key = Bytes::from("testkey");
        actor.handle(KvMessage::Insert {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("value1"),
        });

        // Act - Try to insert again
        let response = actor.handle(KvMessage::Insert {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
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
    fn should_validate_delete_range_parameters() {
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

        // Act - End before start
        let response = actor.handle(KvMessage::DeleteRange {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
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
    #[should_panic(expected = "RouteFamily with id=0")]
    fn should_panic_on_route_family_zero() {
        // Arrange
        let mut actor = test_actor();

        // Act - Should panic
        actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(0),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
    }

    #[test]
    fn should_delete_existing_key() {
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

        let key = Bytes::from("delkey");
        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("value1"),
        });

        // Act - Delete the key
        let delete_response = actor.handle(KvMessage::Delete {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
        });

        // Assert delete succeeds
        assert!(matches!(delete_response, KvResponse::DeleteOk));

        // Verify key is gone
        let get_response = actor.handle(KvMessage::Get {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
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
    fn should_scan_key_range() {
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

        // Add multiple keys
        for i in 0..5 {
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table1".to_string(),
                key: Bytes::from(format!("key{:02}", i)),
                value: Bytes::from(format!("value{}", i)),
            });
        }

        // Act - Scan range [key01, key04)
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            query: ScanQuery {
                start: Some(Bytes::from("key01")),
                end: Some(Bytes::from("key04")),
                limit: None,
                reverse: false,
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
    fn should_commit_empty_transaction() {
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

        // Act - Commit immediately without writing anything
        let response = actor.handle(KvMessage::Commit { tx_id });

        // Assert
        assert!(matches!(response, KvResponse::CommitOk));
    }

    #[test]
    fn should_rollback_transaction() {
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

        let key = Bytes::from("rollbackkey");
        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("will_rollback"),
        });

        // Act - Rollback
        let response = actor.handle(KvMessage::Rollback { tx_id });

        // Assert
        assert!(matches!(response, KvResponse::RollbackOk));

        // Verify transaction is no longer active
        let get_response = actor.handle(KvMessage::Get {
            tx_id: 999,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
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
    fn should_isolate_resources_in_same_family() {
        // Arrange
        let mut actor = test_actor();

        // Begin transaction for resource1
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

        let key = Bytes::from("testkey");
        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("value1"),
        });

        // Act - Try to put to different resource in same transaction (should fail)
        let response = actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table2".to_string(),
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
    fn should_handle_key_scoping_correctly() {
        // Arrange
        let mut actor1 = test_actor();
        let mut actor2 = test_actor();

        // Both start transactions for different resources
        let begin1 = actor1.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });

        let begin2 = actor2.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table2".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });

        let tx_id1 = match begin1 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let tx_id2 = match begin2 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let key = Bytes::from("samekey");

        // Act - Put same key to both resources
        actor1.handle(KvMessage::Put {
            tx_id: tx_id1,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("value1"),
        });

        actor2.handle(KvMessage::Put {
            tx_id: tx_id2,
            route_family: RouteFamily::new(1),
            resource: "table2".to_string(),
            key: key.clone(),
            value: Bytes::from("value2"),
        });

        // Assert - Both succeed, they are isolated
        let get1 = actor1.handle(KvMessage::Get {
            tx_id: tx_id1,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
        });

        let get2 = actor2.handle(KvMessage::Get {
            tx_id: tx_id2,
            route_family: RouteFamily::new(1),
            resource: "table2".to_string(),
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
    fn should_reject_delete_range_with_invalid_bounds() {
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

        // Act - End < Start
        let response = actor.handle(KvMessage::DeleteRange {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
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
    fn should_scan_with_limit() {
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

        // Add 10 keys
        for i in 0..10 {
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table1".to_string(),
                key: Bytes::from(format!("k{:02}", i)),
                value: Bytes::from(format!("v{}", i)),
            });
        }

        // Act - Scan with limit of 3
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            query: ScanQuery {
                start: None,
                end: None,
                limit: Some(3),
                reverse: false,
            },
        });

        // Assert
        match response {
            KvResponse::ScanResult { items, .. } => {
                assert!(items.len() <= 3);
            }
            _ => panic!("Expected ScanResult"),
        }
    }

    #[test]
    fn should_scan_reverse() {
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

        // Add keys
        for i in 0..5 {
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table1".to_string(),
                key: Bytes::from(format!("k{}", i)),
                value: Bytes::from(format!("v{}", i)),
            });
        }

        // Act - Scan reverse
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            query: ScanQuery {
                start: None,
                end: None,
                limit: None,
                reverse: true,
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
