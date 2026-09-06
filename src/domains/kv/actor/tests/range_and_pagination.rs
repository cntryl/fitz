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
        write_options: cntryl_midge::WriteOptions::buffered().into(),
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
