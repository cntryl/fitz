use super::*;

#[test]
fn should_cap_scan_when_client_omits_limit() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "bounded-scan");
    let KvResponse::BeginOk { tx_id } = actor.handle(KvMessage::Begin {
        scope: scope.clone(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    }) else {
        panic!("transaction should begin");
    };
    for index in 0..=MAX_SCAN_ITEMS {
        assert!(matches!(
            actor.handle(KvMessage::Put {
                tx_id,
                scope: scope.clone(),
                key: Bytes::from(format!("key-{index:04}")),
                value: Bytes::from_static(b"value"),
            }),
            KvResponse::PutOk
        ));
    }

    // Act
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope,
        query: ScanQuery {
            start: None,
            end: None,
            limit: None,
            reverse: false,
        },
    });

    // Assert
    let KvResponse::ScanResult { items, has_more } = response else {
        panic!("scan should succeed");
    };
    assert_eq!(items.len(), MAX_SCAN_ITEMS);
    assert!(has_more);
}
fn begin_with_scope(actor: &mut KvActor, scope: KvResourceScope) -> u64 {
    let response = actor.handle(KvMessage::Begin {
        scope,
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let KvResponse::BeginOk { tx_id } = response else {
        panic!("expected transaction begin, got {response:?}");
    };
    tx_id
}

fn put_scan_keys(actor: &mut KvActor, tx_id: u64, scope: &KvResourceScope, keys: &[&[u8]]) {
    for key in keys {
        let response = actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(key),
        });
        assert!(matches!(response, KvResponse::PutOk));
    }
}

fn scan_keys(
    actor: &mut KvActor,
    tx_id: u64,
    scope: &KvResourceScope,
    query: ScanQuery,
) -> (Vec<Bytes>, bool) {
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope: scope.clone(),
        query,
    });
    let KvResponse::ScanResult { items, has_more } = response else {
        panic!("expected scan result, got {response:?}");
    };
    (items.into_iter().map(|item| item.key).collect(), has_more)
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

#[test]
fn should_apply_forward_and_reverse_scan_boundaries() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    put_scan_keys(&mut actor, tx_id, &scope, &[b"a", b"b", b"c", b"c\0", b"d"]);

    // Act
    let (forward, _) = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: Some(Bytes::from_static(b"b")),
            end: Some(Bytes::from_static(b"d")),
            limit: None,
            reverse: false,
        },
    );
    let (reverse, _) = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: Some(Bytes::from_static(b"c")),
            end: Some(Bytes::from_static(b"a")),
            limit: None,
            reverse: true,
        },
    );

    // Assert
    assert_eq!(
        forward,
        [b"b".as_slice(), b"c", b"c\0"].map(Bytes::copy_from_slice)
    );
    assert_eq!(reverse, [b"c".as_slice(), b"b"].map(Bytes::copy_from_slice));
}

#[test]
fn should_handle_every_omitted_scan_bound_combination() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    put_scan_keys(&mut actor, tx_id, &scope, &[b"a", b"b", b"c"]);

    // Act
    let queries = [
        (Some(Bytes::from_static(b"b")), None, false),
        (None, Some(Bytes::from_static(b"c")), false),
        (Some(Bytes::from_static(b"b")), None, true),
        (None, Some(Bytes::from_static(b"b")), true),
    ];
    let results = queries.map(|(start, end, reverse)| {
        scan_keys(
            &mut actor,
            tx_id,
            &scope,
            ScanQuery {
                start,
                end,
                limit: None,
                reverse,
            },
        )
        .0
    });

    // Assert
    assert_eq!(
        results[0],
        [Bytes::from_static(b"b"), Bytes::from_static(b"c")]
    );
    assert_eq!(
        results[1],
        [Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );
    assert_eq!(
        results[2],
        [Bytes::from_static(b"b"), Bytes::from_static(b"a")]
    );
    assert_eq!(results[3], [Bytes::from_static(b"c")]);
}

#[test]
fn should_lower_reverse_exact_bounds_around_binary_successors() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    put_scan_keys(&mut actor, tx_id, &scope, &[b"k", b"k\0", b"k\0\0", b"l"]);

    // Act
    let (keys, _) = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: Some(Bytes::from_static(b"k\0")),
            end: Some(Bytes::from_static(b"k")),
            limit: None,
            reverse: true,
        },
    );

    // Assert
    assert_eq!(keys, [Bytes::from_static(b"k\0")]);
}

#[test]
fn should_report_has_more_for_limited_scans_in_both_directions() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    put_scan_keys(&mut actor, tx_id, &scope, &[b"a", b"b", b"c"]);

    // Act
    let forward = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: None,
            end: None,
            limit: Some(2),
            reverse: false,
        },
    );
    let reverse = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: None,
            end: None,
            limit: Some(3),
            reverse: true,
        },
    );

    // Assert
    assert_eq!(
        forward,
        (
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            true
        )
    );
    assert_eq!(
        reverse,
        (
            vec![
                Bytes::from_static(b"c"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"a"),
            ],
            false,
        )
    );
}

#[test]
fn should_report_matches_without_returning_items_for_zero_limit() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    put_scan_keys(&mut actor, tx_id, &scope, &[b"a"]);

    // Act
    let populated = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: None,
            end: None,
            limit: Some(0),
            reverse: false,
        },
    );
    let empty = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: Some(Bytes::from_static(b"z")),
            end: None,
            limit: Some(0),
            reverse: false,
        },
    );

    // Assert
    assert_eq!(populated, (Vec::new(), true));
    assert_eq!(empty, (Vec::new(), false));
}

#[test]
fn should_return_empty_success_for_equal_or_inverted_scan_intervals() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    put_scan_keys(&mut actor, tx_id, &scope, &[b"a", b"b"]);

    // Act
    let forward = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: Some(Bytes::from_static(b"b")),
            end: Some(Bytes::from_static(b"a")),
            limit: None,
            reverse: false,
        },
    );
    let reverse = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: Some(Bytes::from_static(b"a")),
            end: Some(Bytes::from_static(b"b")),
            limit: None,
            reverse: true,
        },
    );

    // Assert
    assert_eq!(forward, (Vec::new(), false));
    assert_eq!(reverse, (Vec::new(), false));
}
