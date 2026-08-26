use super::*;

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
    //
    // The scan must fail loudly here rather than silently omit the pair: a
    // skipped entry would make SCAN report success while permanently missing
    // an in-range, authoritative value - and if it were the last entry,
    // `has_more` would read false too, leaving the client no way to detect
    // the gap. An explicit, retried-forever-safe error is the honest
    // response; only a direct GET can still return this particular value.
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
