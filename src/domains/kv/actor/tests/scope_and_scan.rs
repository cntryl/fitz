use super::*;

#[test]
fn should_treat_explicit_zero_limit_as_unbounded_not_a_dead_end() {
    // Arrange
    // `limit=0` is a legal encoding on the wire (has_limit=1, limit=0), but
    // treated literally it makes every SCAN return zero items with
    // `has_more=1` and no key to resume from - a request the client can never
    // make progress on, no matter how many times it retries. An explicit zero
    // is therefore folded into "no limit supplied", matching what an omitted
    // limit already means.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "zero-limit");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: Bytes::from_static(b"only-key"),
            value: Bytes::from_static(b"value"),
        }),
        KvResponse::PutOk
    ));

    // Act
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope,
        query: ScanQuery {
            start: None,
            end: None,
            limit: Some(0),
            reverse: false,
            start_exclusive: false,
        },
    });

    // Assert
    let KvResponse::ScanResult { items, has_more } = response else {
        panic!("expected ScanResult, got {response:?}");
    };
    assert_eq!(
        items.len(),
        1,
        "an explicit zero limit must not starve the page"
    );
    assert!(!has_more);
}

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
            start_exclusive: false,
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
fn should_apply_forward_plus_reverse_scan_boundaries() {
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
            start_exclusive: false,
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
            start_exclusive: false,
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
                start_exclusive: false,
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
            start_exclusive: false,
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
            start_exclusive: false,
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
            start_exclusive: false,
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
fn should_treat_zero_limit_as_unbounded_for_an_empty_match_set() {
    // Arrange
    //
    // `limit=0` is folded into "no limit supplied" (unbounded), not a dead
    // end: see `should_treat_explicit_zero_limit_as_unbounded_not_a_dead_end`
    // for the populated case. This test covers the other half - an
    // unbounded scan over a range with no matches must still report zero
    // items and `has_more=false`, not the old dead-end `has_more=true`.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "realm", "area", "table");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    put_scan_keys(&mut actor, tx_id, &scope, &[b"a"]);

    // Act
    let empty = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: Some(Bytes::from_static(b"z")),
            end: None,
            limit: Some(0),
            reverse: false,
            start_exclusive: false,
        },
    );

    // Assert
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
            start_exclusive: false,
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
            start_exclusive: false,
        },
    );

    // Assert
    assert_eq!(forward, (Vec::new(), false));
    assert_eq!(reverse, (Vec::new(), false));
}

#[test]
fn should_bound_scan_response_to_one_wire_frame() {
    // Arrange
    // A scan response is carried as one TLV value with a u16 length. The item
    // cap alone does not bound it: 1 KiB values overflow the frame at roughly
    // 63 items, far below the 1,024-item ceiling, and a client that omits
    // `limit` takes that default. Every pair here is individually legal; only
    // the aggregate is unencodable.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "wire-bounded-scan");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    let value = Bytes::from(vec![b'v'; 1024]);
    for index in 0..300 {
        assert!(matches!(
            actor.handle(KvMessage::Put {
                tx_id,
                scope: scope.clone(),
                key: Bytes::from(format!("key-{index:04}")),
                value: value.clone(),
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
            start_exclusive: false,
        },
    });

    // Assert
    let KvResponse::ScanResult { items, has_more } = response else {
        panic!("scan should succeed, got {response:?}");
    };
    let encoded = crate::dispatch::protocol::kv::encode_response(&KvResponse::ScanResult {
        items: items.clone(),
        has_more,
    });
    assert!(
        u16::try_from(encoded.len()).is_ok(),
        "scan response is {} bytes, past the {}-byte TLV value limit",
        encoded.len(),
        u16::MAX
    );
    assert!(!items.is_empty(), "the page must make forward progress");
    assert!(
        has_more,
        "a truncated page must tell the client to continue"
    );
}

#[test]
fn should_refuse_a_key_that_cannot_become_a_continuation_boundary() {
    // Arrange
    // A key can be large enough to fit its own PUT and to fit once inside a
    // SCAN response, yet still be too large to safely echo back as
    // `start_key` in a follow-up SCAN request - the request has the same
    // wire ceiling as the response. Manufacturing `has_more=1` for such a
    // key would hand the client a page it can never resume past, silently
    // stranding every later key. This must fail loudly at the boundary
    // instead, exactly as an unencodable pair already does.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "huge-key-resume");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    // Lexicographically first, so it lands on page one; a second key exists
    // so a real scan WOULD have more to return, forcing `has_more` to depend
    // on whether the huge key can serve as a resume boundary.
    let huge_key = Bytes::from([vec![b'0'; 4], vec![b'k'; 65_505]].concat());
    let next_key = Bytes::from_static(b"1-next-key");
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: huge_key.clone(),
            value: Bytes::from_static(b"v"),
        }),
        KvResponse::PutOk
    ));
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: next_key,
            value: Bytes::from_static(b"v"),
        }),
        KvResponse::PutOk
    ));

    // Act
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope,
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
        KvResponse::Error { .. } => {}
        KvResponse::ScanResult { has_more, .. } => {
            assert!(
                !has_more,
                "a page must never promise a continuation it cannot honour"
            );
        }
        other => panic!("expected Error or ScanResult, got {other:?}"),
    }
}

#[test]
fn should_return_frame_valid_terminal_key_larger_than_continuation_limit() {
    // Arrange
    // A key only needs to fit a continuation request when another matching row
    // remains. This key is deliberately one byte beyond that conservative
    // boundary, but its PUT and the terminal SCAN response both remain valid
    // u16-sized TLV payloads.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "terminal-large-key");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    let key_len = crate::domains::kv::scan_wire_budget::kv_scan_continuation_max_key_bytes() + 1;
    let terminal_key = Bytes::from(vec![b'z'; key_len]);
    let value = Bytes::from_static(b"v");
    let route = format!("kv://{}/{}/{}", scope.realm, scope.area, scope.resource);
    let put_payload_len = 20 + route.len() + terminal_key.len() + value.len();
    assert!(u16::try_from(put_payload_len).is_ok());
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: terminal_key.clone(),
            value: value.clone(),
        }),
        KvResponse::PutOk
    ));

    // Act
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope,
        query: ScanQuery {
            start: None,
            end: None,
            limit: None,
            reverse: false,
            start_exclusive: false,
        },
    });

    // Assert
    let KvResponse::ScanResult { items, has_more } = &response else {
        panic!("terminal large key should remain scannable, got {response:?}");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].key, terminal_key);
    assert_eq!(items[0].value, value);
    assert!(!has_more, "a terminal result needs no continuation");
    let encoded = crate::dispatch::protocol::kv::encode_response(&response);
    assert!(u16::try_from(encoded.len()).is_ok());
}

#[test]
fn should_keep_oversized_scan_pair_error_inside_one_wire_frame() {
    // Arrange
    // The error for an unencodable pair must not itself be unencodable. A key
    // can approach the frame limit on its own, and lossy UTF-8 conversion
    // widens every invalid byte to three, so echoing it whole would recreate
    // the failure this branch exists to prevent.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "oversized-pair");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    // Invalid UTF-8 throughout, so lossy conversion expands every byte
    // threefold, and large enough that echoing it whole would blow the frame.
    // The key must be hostile enough that echoing it whole would itself blow
    // the frame: 30,000 invalid bytes render as 90,000 replacement characters,
    // well past u16::MAX. A short or printable key would pass even with the
    // truncation removed, testing nothing.
    //
    // The value then pushes the pair past the exact budget (8 + key + value >
    // 65_529). Note the pair is unreachable over the wire - a PUT is itself one
    // TLV value - so this branch guards in-process writers and data stored
    // before the budget existed.
    let hostile_key = Bytes::from(vec![0xffu8; 30_000]);
    let value = Bytes::from(vec![b'v'; 40_000]);
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: hostile_key,
            value,
        }),
        KvResponse::PutOk
    ));

    // Act
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope,
        query: ScanQuery {
            start: None,
            end: None,
            limit: None,
            reverse: false,
            start_exclusive: false,
        },
    });

    // Assert
    let encoded = crate::dispatch::protocol::kv::encode_response(&response);
    assert!(
        u16::try_from(encoded.len()).is_ok(),
        "the oversized-pair error is itself {} bytes, past the {}-byte TLV limit",
        encoded.len(),
        u16::MAX
    );
    assert!(
        matches!(response, KvResponse::Error { .. }),
        "an unencodable pair must be reported, got {response:?}"
    );
}

#[test]
fn should_scan_keys_that_begin_with_the_range_end_marker() {
    // Arrange
    // KV appends raw, unencoded user bytes after a lexkey-encoded prefix, so a
    // user key may begin with 0xff - the same byte lexkey uses as its range end
    // marker. Bounding the scan with `prefix || 0xff` therefore sorts such keys
    // outside their own resource: the write succeeds and the key is then
    // invisible to every scan, which is silent data loss from the client's
    // point of view.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "end-marker-keys");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    let keys: [Vec<u8>; 5] = [
        b"aaa".to_vec(),
        vec![0x80, 0x80],
        vec![0xfe, 0xfe],
        vec![0xff, 0x00],
        vec![0xff, 0xff],
    ];
    for key in &keys {
        assert!(matches!(
            actor.handle(KvMessage::Put {
                tx_id,
                scope: scope.clone(),
                key: Bytes::from(key.clone()),
                value: Bytes::from_static(b"v"),
            }),
            KvResponse::PutOk
        ));
    }

    // Act
    let (scanned, _) = scan_keys(
        &mut actor,
        tx_id,
        &scope,
        ScanQuery {
            start: None,
            end: None,
            limit: None,
            reverse: false,
            start_exclusive: false,
        },
    );

    // Assert
    assert_eq!(
        scanned.len(),
        keys.len(),
        "every stored key must be scannable, got {scanned:?}"
    );
    for key in &keys {
        assert!(
            scanned.iter().any(|found| found.as_ref() == key.as_slice()),
            "key {key:?} was stored but is invisible to scan"
        );
    }
}

#[test]
fn should_page_a_byte_bounded_scan_to_completion_via_start_key() {
    // Arrange
    // An omitted limit does not mean unlimited: a page also ends at the
    // response frame budget. The spec's continuation rule must therefore work
    // for a client that supplied no limit at all - re-issue with `start_key`
    // set to the last key returned, which is inclusive and so repeats as the
    // first item of the next page.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "paged-scan");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    let value = Bytes::from(vec![b'v'; 1024]);
    let total = 300;
    for index in 0..total {
        assert!(matches!(
            actor.handle(KvMessage::Put {
                tx_id,
                scope: scope.clone(),
                key: Bytes::from(format!("key-{index:04}")),
                value: value.clone(),
            }),
            KvResponse::PutOk
        ));
    }

    // Act
    let mut seen: Vec<Bytes> = Vec::new();
    let mut start: Option<Bytes> = None;
    let mut pages = 0;
    loop {
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            scope: scope.clone(),
            query: ScanQuery {
                start: start.clone(),
                end: None,
                limit: None,
                reverse: false,
                start_exclusive: false,
            },
        });
        let KvResponse::ScanResult { items, has_more } = response else {
            panic!("scan should succeed, got {response:?}");
        };
        pages += 1;
        assert!(pages < 50, "pagination must converge");
        assert!(!items.is_empty(), "each page must make forward progress");

        for item in &items {
            // `start_key` is inclusive, so the first item of a continuation
            // repeats the previous page's last key.
            if seen.last() == Some(&item.key) {
                continue;
            }
            seen.push(item.key.clone());
        }
        if !has_more {
            break;
        }
        start = Some(items.last().expect("non-empty page").key.clone());
    }

    // Assert
    assert!(pages > 1, "1 KiB values must not fit in a single page");
    assert_eq!(
        seen.len(),
        total,
        "following has_more must recover every key"
    );
    for index in 0..total {
        let expected = Bytes::from(format!("key-{index:04}"));
        assert!(seen.contains(&expected), "key {index} was never returned");
    }
}

#[test]
fn should_advance_pagination_when_only_one_pair_fits_a_page() {
    // Arrange
    // Two adjacent pairs that are each wire-valid but cannot share a page. The
    // first page returns only pair A. Resuming with an inclusive `start_key`
    // returns pair A again, forever: the documented procedure never reaches
    // pair B, so a compliant client loops and the data is unreachable.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "single-pair-pages");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    let big = Bytes::from(vec![b'v'; 40_000]);
    for key in ["key-a", "key-b"] {
        assert!(matches!(
            actor.handle(KvMessage::Put {
                tx_id,
                scope: scope.clone(),
                key: Bytes::from(key),
                value: big.clone(),
            }),
            KvResponse::PutOk
        ));
    }

    // Act
    let mut seen: Vec<Bytes> = Vec::new();
    let mut start: Option<Bytes> = None;
    let mut exclusive = false;
    for _ in 0..8 {
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            scope: scope.clone(),
            query: ScanQuery {
                start: start.clone(),
                end: None,
                limit: None,
                reverse: false,
                start_exclusive: exclusive,
            },
        });
        let KvResponse::ScanResult { items, has_more } = response else {
            panic!("scan should succeed");
        };
        for item in &items {
            if !seen.contains(&item.key) {
                seen.push(item.key.clone());
            }
        }
        if !has_more {
            break;
        }
        start = Some(items.last().expect("non-empty page").key.clone());
        exclusive = true;
    }

    // Assert
    assert_eq!(
        seen.len(),
        2,
        "pagination must reach both pairs, saw {seen:?}"
    );
}

#[test]
fn should_page_forward_without_the_exclusive_flag_via_successor_key() {
    // Arrange
    // The documented fallback for clients that cannot yet encode
    // `start_exclusive`: resume from the last returned key followed by a single
    // 0x00 byte, which is that key's immediate successor and so an exclusive
    // resume using only fields those clients already send. Forward scans must
    // therefore paginate to completion today, with no wire change.
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "successor-paging");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    let big = Bytes::from(vec![b'v'; 40_000]);
    for key in ["key-a", "key-b", "key-c"] {
        assert!(matches!(
            actor.handle(KvMessage::Put {
                tx_id,
                scope: scope.clone(),
                key: Bytes::from(key),
                value: big.clone(),
            }),
            KvResponse::PutOk
        ));
    }

    // Act
    let mut seen: Vec<Bytes> = Vec::new();
    let mut start: Option<Bytes> = None;
    for _ in 0..10 {
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            scope: scope.clone(),
            query: ScanQuery {
                start: start.clone(),
                end: None,
                limit: None,
                reverse: false,
                // Deliberately never set: this is the old-client path.
                start_exclusive: false,
            },
        });
        let KvResponse::ScanResult { items, has_more } = response else {
            panic!("scan should succeed");
        };
        for item in &items {
            if !seen.contains(&item.key) {
                seen.push(item.key.clone());
            }
        }
        if !has_more {
            break;
        }
        let mut resume = items.last().expect("non-empty page").key.to_vec();
        resume.push(0);
        start = Some(Bytes::from(resume));
    }

    // Assert
    assert_eq!(seen.len(), 3, "forward paging must complete, saw {seen:?}");
}
