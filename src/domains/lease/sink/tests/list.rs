//! Coverage for the review findings on Lease `LIST` and wildcard `SUBSCRIBE`:
//! the wildcard registration cap, exact keyed lookup, eager atomic snapshot
//! materialization with a bounded-candidates ceiling, cursor-offset binding,
//! retained-memory eviction, and session-bound snapshot lifecycle.

use super::*;
use crate::domains::lease::protocol::{LeaseListCursor, LEASE_LIST_MAX_CANDIDATES_PER_SCAN};

fn new_list_test_sink() -> LeaseDomainSink {
    LeaseDomainSink::new(
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    )
}

fn acquire_immediate(
    sink: &LeaseDomainSink,
    family: RouteFamily,
    route: &str,
    owner_session_id: u64,
    owner_id: &str,
) -> u64 {
    let request = LeaseAcquireRequest {
        key: lease_key(family, route),
        owner_session_id,
        // Sink-level tests bypass ingress, which normally scopes the raw
        // client-supplied owner_id before it ever reaches `handle_acquire`
        // (see `session_scoped_owner_id`). Scope it here too so
        // `logical_owner_id` (what LIST actually reports) round-trips the
        // same way it would over the real wire path.
        owner_id: crate::domains::lease::protocol::session_scoped_owner_id(
            owner_session_id,
            owner_id,
        ),
        ttl_secs: 300,
        wait_seconds: 0,
        reply_source: RouteAddress::new(family, Route::new("internal://domain/lease")),
        reply_destination: None,
        channel: ClientChannel::Lease,
        route_family: family,
    };
    let response = sink.acquire_for_tests(request);
    let LeaseResponse::Acquired { fencing_token } = response else {
        panic!("expected immediate acquisition of {route}, got {response:?}");
    };
    fencing_token
}

#[test]
fn should_enforce_wildcard_lease_subscription_cap_per_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("lease://inbound"));
    let mailbox = Arc::new(Mailbox::new(256));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = LeaseDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );

    let subscribe = |resource: usize| {
        crate::domains::lease::LeaseClientRequest::new(
            crate::runtime::ClientFrameMeta::new(
                7,
                ClientChannel::Sub,
                crate::dispatch::protocol::lease_codec::msg_type::SUBSCRIBE,
                family,
            ),
            Ok(crate::domains::lease::LeaseClientFrame::Sub(
                crate::domains::lease::LeaseSubscriptionMessage::Subscribe {
                    family_id: family,
                    route: Route::new(format!("lease://acme/area{resource}/*")),
                    session_id: 7,
                    subscriber: source.clone(),
                },
            )),
        )
    };

    // Act: fill the 128-wildcard-per-session cap exactly, then try one more.
    for resource in 0..128 {
        sink.deliver(Envelope::from_route(
            source.clone(),
            destination.clone(),
            subscribe(resource),
        ))
        .expect("deliver wildcard subscribe within cap");
        let response = receive_envelope(&mailbox, "wildcard subscribe within cap")
            .into_payload::<FrameContext>()
            .expect("subscribe response frame");
        let (_msg_type, status, _data) = parse_status_only(&response.payload);
        assert_eq!(status, 0, "subscription {resource} within cap must succeed");
    }
    sink.deliver(Envelope::from_route(
        source.clone(),
        destination,
        subscribe(128),
    ))
    .expect("deliver wildcard subscribe past cap");
    let over_cap = receive_envelope(&mailbox, "wildcard subscribe past cap")
        .into_payload::<FrameContext>()
        .expect("over-cap subscribe response frame");

    // Assert
    let (_msg_type, status, _data) = parse_status_only(&over_cap.payload);
    assert_eq!(
        status, 1,
        "the 129th wildcard subscription must be rejected"
    );
    assert_eq!(sink.subscription_count(), 128);
}

fn parse_status_only(payload: &bytes::Bytes) -> (u8, u8, ()) {
    (0, payload.first().copied().unwrap_or(1), ())
}

#[test]
fn should_scope_lease_list_to_requesting_family_only() {
    // Arrange
    let family_a = RouteFamily::new(1);
    let family_b = RouteFamily::new(2);
    let sink = new_list_test_sink();
    acquire_immediate(&sink, family_a, "lease://acme/renderers/doc-a", 1, "owner");
    acquire_immediate(&sink, family_b, "lease://acme/renderers/doc-b", 2, "owner");

    // Act
    let response = sink.list_for_tests(
        family_a,
        Route::new("lease://acme/renderers/*"),
        None,
        None,
        1,
    );

    // Assert: family B's matching lease is never examined or returned, even
    // though it matches the same selector.
    let LeaseResponse::ListPage { items, next_cursor } = response else {
        panic!("expected a ListPage response, got {response:?}");
    };
    assert_eq!(next_cursor, None);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].route.as_str(), "lease://acme/renderers/doc-a");
}

#[test]
fn should_use_keyed_lookup_for_exact_lease_selector_instead_of_scanning() {
    // Arrange: enough leases in the family to blow the wildcard scan's
    // candidate ceiling, so an exact LIST that instead scanned the family
    // would hit "too many candidates" — proving the exact path is a direct
    // keyed lookup, not a scan, when it doesn't.
    let family = RouteFamily::new(1);
    let sink = new_list_test_sink();
    for i in 0..(LEASE_LIST_MAX_CANDIDATES_PER_SCAN + 5) {
        acquire_immediate(
            &sink,
            family,
            &format!("lease://acme/bulk/doc-{i:05}"),
            1,
            "owner",
        );
    }
    acquire_immediate(&sink, family, "lease://acme/exact/target", 1, "owner-exact");

    // Act
    let hit = sink.list_for_tests(
        family,
        Route::new("lease://acme/exact/target"),
        None,
        None,
        1,
    );
    let miss = sink.list_for_tests(
        family,
        Route::new("lease://acme/exact/nonexistent"),
        None,
        None,
        1,
    );

    // Assert
    let LeaseResponse::ListPage { items, next_cursor } = hit else {
        panic!("expected a ListPage response, got {hit:?}");
    };
    assert_eq!(next_cursor, None);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].route.as_str(), "lease://acme/exact/target");
    assert_eq!(items[0].owner_id, "owner-exact");

    let LeaseResponse::ListPage { items, next_cursor } = miss else {
        panic!("expected a ListPage response, got {miss:?}");
    };
    assert_eq!(next_cursor, None);
    assert!(items.is_empty());
}

#[test]
fn should_reject_wildcard_scan_exceeding_the_candidate_ceiling() {
    // Arrange: one more matching lease than the (test-shrunk) ceiling.
    let family = RouteFamily::new(1);
    let sink = new_list_test_sink();
    for i in 0..=LEASE_LIST_MAX_CANDIDATES_PER_SCAN {
        acquire_immediate(
            &sink,
            family,
            &format!("lease://acme/renderers/doc-{i:05}"),
            1,
            "owner",
        );
    }

    // Act
    let response = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        None,
        None,
        1,
    );

    // Assert: a typed, explicit failure — never a silently truncated or
    // partially-filled result.
    let LeaseResponse::Error(message) = response else {
        panic!("expected a bounded Error response, got {response:?}");
    };
    assert!(
        message.contains("too many candidates"),
        "unexpected message: {message}"
    );
    assert!(
        LeaseDomainRuntime::lease_response_is_failure(&LeaseResponse::Error(message)),
        "a bounded scan-too-large response must count as a failure for metrics"
    );
}

#[test]
fn should_reject_wildcard_scan_exceeding_the_snapshot_byte_ceiling() {
    // Arrange: stay below the candidate ceiling while using long, valid
    // resource segments whose captured inventory exceeds the test byte cap.
    let family = RouteFamily::new(1);
    let sink = new_list_test_sink();
    for i in 0..LEASE_LIST_MAX_CANDIDATES_PER_SCAN {
        acquire_immediate(
            &sink,
            family,
            &format!("lease://acme/renderers/{}-{i}", "x".repeat(2_000)),
            1,
            "owner",
        );
    }

    // Act
    let response = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        None,
        None,
        1,
    );

    // Assert
    let LeaseResponse::Error(message) = response else {
        panic!("expected a bounded Error response, got {response:?}");
    };
    assert!(
        message.contains("too much inventory"),
        "unexpected message: {message}"
    );
    assert!(sink.state.core.list_snapshots.lock().is_empty());
}

#[test]
fn should_take_an_atomic_snapshot_immune_to_concurrent_mutation_mid_scan() {
    // Arrange: a scan spanning two pages...
    let family = RouteFamily::new(1);
    let sink = new_list_test_sink();
    let routes = ["a", "b", "c"];
    let mut tokens = std::collections::HashMap::new();
    for route in routes {
        let token = acquire_immediate(
            &sink,
            family,
            &format!("lease://acme/renderers/{route}"),
            1,
            "owner",
        );
        tokens.insert(route, token);
    }
    let first = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        None,
        Some(1),
        1,
    );
    let LeaseResponse::ListPage { next_cursor, .. } = first else {
        panic!("expected a ListPage response, got {first:?}");
    };
    let cursor = next_cursor.expect("expected a continuation cursor");

    // Act: release one already-snapshotted lease and acquire a brand new
    // one that would also match, both before continuing the scan.
    let released_key = lease_key(family, "lease://acme/renderers/a");
    let scoped_owner = crate::domains::lease::protocol::session_scoped_owner_id(1, "owner");
    let release_response =
        sink.state
            .runtime()
            .handle_release(&released_key, &scoped_owner, tokens[&"a"]);
    assert!(
        matches!(release_response, LeaseResponse::Released),
        "expected release to succeed, got {release_response:?}"
    );
    acquire_immediate(&sink, family, "lease://acme/renderers/z-new", 1, "owner");

    // Assert: paging the rest of the scan still yields exactly the three
    // routes captured at snapshot time — not the released one dropped, and
    // not the new one added.
    let mut seen = vec![];
    let mut cursor = Some(cursor);
    while let Some(current) = cursor {
        let response = sink.list_for_tests(
            family,
            Route::new("lease://acme/renderers/*"),
            Some(current),
            Some(1),
            1,
        );
        let LeaseResponse::ListPage { items, next_cursor } = response else {
            panic!("expected a ListPage response, got {response:?}");
        };
        seen.extend(
            items
                .into_iter()
                .map(|item| item.route.as_str().to_string()),
        );
        cursor = next_cursor;
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![
            "lease://acme/renderers/b".to_string(),
            "lease://acme/renderers/c".to_string(),
        ],
        "the released 'a' must still appear (captured at snapshot time) and the new 'z-new' must not"
    );
}

#[test]
fn should_reject_cursor_with_a_tampered_offset() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = new_list_test_sink();
    for route in ["a", "b", "c"] {
        acquire_immediate(
            &sink,
            family,
            &format!("lease://acme/renderers/{route}"),
            1,
            "owner",
        );
    }
    let first = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        None,
        Some(1),
        1,
    );
    let LeaseResponse::ListPage { next_cursor, .. } = first else {
        panic!("expected a ListPage response, got {first:?}");
    };
    let cursor = next_cursor.expect("expected a continuation cursor");

    // Act: present the same snapshot ID with an offset that wasn't issued
    // — neither replaying (too low) nor skipping ahead (too high) is
    // accepted, even though both remain within `items.len()`.
    let replayed = LeaseListCursor {
        snapshot_id: cursor.snapshot_id,
        offset: cursor.offset.saturating_sub(1),
    };
    let skipped = LeaseListCursor {
        snapshot_id: cursor.snapshot_id,
        offset: cursor.offset + 1,
    };

    // Assert
    for tampered in [replayed, skipped] {
        let response = sink.list_for_tests(
            family,
            Route::new("lease://acme/renderers/*"),
            Some(tampered),
            Some(1),
            1,
        );
        assert_eq!(response, LeaseResponse::InvalidListCursor);
    }

    // The untampered cursor still resolves correctly afterward.
    let continued = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        Some(cursor),
        Some(1),
        1,
    );
    assert!(matches!(continued, LeaseResponse::ListPage { .. }));
}

#[test]
fn should_drain_served_items_from_retained_snapshot_memory() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = new_list_test_sink();
    for route in ["a", "b", "c"] {
        acquire_immediate(
            &sink,
            family,
            &format!("lease://acme/renderers/{route}"),
            1,
            "owner",
        );
    }
    let first = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        None,
        Some(1),
        1,
    );
    let LeaseResponse::ListPage { next_cursor, .. } = first else {
        panic!("expected a ListPage response, got {first:?}");
    };
    let cursor = next_cursor.expect("expected a continuation cursor");

    // Assert: only the two not-yet-served items remain retained, not the
    // original three.
    assert_eq!(
        sink.state
            .core
            .list_snapshots
            .lock()
            .get(&cursor.snapshot_id)
            .map(|snapshot| snapshot.items.len()),
        Some(2)
    );

    // Act: serve the next page.
    let second = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        Some(cursor),
        Some(1),
        1,
    );
    let LeaseResponse::ListPage { next_cursor, .. } = second else {
        panic!("expected a ListPage response, got {second:?}");
    };
    let cursor = next_cursor.expect("expected a continuation cursor");

    // Assert: down to one retained item.
    assert_eq!(
        sink.state
            .core
            .list_snapshots
            .lock()
            .get(&cursor.snapshot_id)
            .map(|snapshot| snapshot.items.len()),
        Some(1)
    );
}

#[test]
fn should_evict_least_recently_touched_snapshot_when_retained_item_budget_is_exceeded() {
    // Arrange: several independent multi-page scans, each retaining a few
    // not-yet-served items, until the (test-shrunk) global retained-item
    // budget is exceeded. Each scan uses its own family so the (also
    // test-shrunk) per-family candidate ceiling never interferes — this
    // test is about the cross-family retained-memory bound specifically.
    let sink = new_list_test_sink();
    let scans_needed = crate::domains::lease::protocol::LEASE_LIST_MAX_RETAINED_ITEMS_TOTAL / 2 + 2;
    let mut first_snapshot_id = None;
    for scan in 0..scans_needed {
        let family = RouteFamily::new(u32::try_from(scan).expect("scan index fits u32") + 1);
        for item in 0..3 {
            acquire_immediate(
                &sink,
                family,
                &format!("lease://acme/scan{scan}/doc-{item}"),
                1,
                "owner",
            );
        }
        let response = sink.list_for_tests(
            family,
            Route::new(format!("lease://acme/scan{scan}/*")),
            None,
            Some(1),
            1,
        );
        let LeaseResponse::ListPage { next_cursor, .. } = response else {
            panic!("expected a ListPage response, got {response:?}");
        };
        let cursor = next_cursor.expect("expected a continuation cursor (2 items retained)");
        if scan == 0 {
            first_snapshot_id = Some(cursor.snapshot_id);
        }
    }

    // Assert: the retained total across every outstanding snapshot never
    // exceeds the global bound, and the oldest scan was evicted to make
    // room rather than growing past it.
    let total_retained: usize = sink
        .state
        .core
        .list_snapshots
        .lock()
        .values()
        .map(|snapshot| snapshot.items.len())
        .sum();
    assert!(
        total_retained <= crate::domains::lease::protocol::LEASE_LIST_MAX_RETAINED_ITEMS_TOTAL,
        "retained {total_retained} items, over the global budget"
    );
    assert!(
        !sink
            .state
            .core
            .list_snapshots
            .lock()
            .contains_key(&first_snapshot_id.expect("first scan issued a cursor")),
        "the least-recently-touched snapshot should have been evicted"
    );
}

#[test]
fn should_reject_lease_list_cursor_from_a_different_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = new_list_test_sink();
    for route in ["a", "b", "c"] {
        acquire_immediate(
            &sink,
            family,
            &format!("lease://acme/renderers/{route}"),
            1,
            "owner",
        );
    }
    let first = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        None,
        Some(1),
        1, // session 1 starts the scan
    );
    let LeaseResponse::ListPage { next_cursor, .. } = first else {
        panic!("expected a ListPage response, got {first:?}");
    };
    let cursor = next_cursor.expect("expected a continuation cursor");

    // Act: session 2 tries to continue session 1's scan.
    let hijack_attempt = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        Some(cursor),
        Some(1),
        2,
    );

    // Assert
    assert_eq!(hijack_attempt, LeaseResponse::InvalidListCursor);

    // The rightful session can still continue it afterward.
    let continued = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        Some(cursor),
        Some(1),
        1,
    );
    assert!(matches!(continued, LeaseResponse::ListPage { .. }));
}

#[test]
fn should_remove_lease_list_snapshot_on_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = new_list_test_sink();
    for route in ["a", "b", "c"] {
        acquire_immediate(
            &sink,
            family,
            &format!("lease://acme/renderers/{route}"),
            9,
            "owner",
        );
    }
    let first = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        None,
        Some(1),
        9,
    );
    let LeaseResponse::ListPage { next_cursor, .. } = first else {
        panic!("expected a ListPage response, got {first:?}");
    };
    let cursor = next_cursor.expect("expected a continuation cursor");
    assert_eq!(sink.state.core.list_snapshots.lock().len(), 1);

    // Act: session 9 disconnects before finishing the scan.
    let _ = sink.cleanup_session(9);

    // Assert: the abandoned snapshot is gone, and the stale cursor no
    // longer resolves.
    assert_eq!(sink.state.core.list_snapshots.lock().len(), 0);
    let after_cleanup = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        Some(cursor),
        Some(1),
        9,
    );
    assert_eq!(after_cleanup, LeaseResponse::InvalidListCursor);
}

#[test]
fn should_reclaim_idle_lease_list_snapshot_past_its_ttl() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = new_list_test_sink();
    for route in ["a", "b", "c"] {
        acquire_immediate(
            &sink,
            family,
            &format!("lease://acme/renderers/{route}"),
            1,
            "owner",
        );
    }
    let first = sink.list_for_tests(
        family,
        Route::new("lease://acme/renderers/*"),
        None,
        Some(1),
        1,
    );
    assert!(matches!(first, LeaseResponse::ListPage { .. }));
    assert_eq!(sink.state.core.list_snapshots.lock().len(), 1);

    // Back-date the snapshot's last-touched time past the idle TTL rather
    // than sleeping in a unit test.
    {
        let mut snapshots = sink.state.core.list_snapshots.lock();
        for snapshot in snapshots.values_mut() {
            snapshot.last_touched_at = std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(
                    crate::domains::lease::protocol::LEASE_LIST_SNAPSHOT_IDLE_TTL_SECS + 1,
                ))
                .expect("instant far enough in the past");
        }
    }

    // Act
    sink.state.runtime().sweep_idle_list_snapshots();

    // Assert
    assert_eq!(sink.state.core.list_snapshots.lock().len(), 0);
}

#[test]
fn should_classify_invalid_list_responses_as_failures() {
    // A LIST response the reviewer flagged as miscounted: malformed
    // patterns and stale cursors must count as failures for latency/SLO
    // metrics, the same as any other Lease error.
    assert!(LeaseDomainRuntime::lease_response_is_failure(
        &LeaseResponse::InvalidListPattern("bad pattern".to_string())
    ));
    assert!(LeaseDomainRuntime::lease_response_is_failure(
        &LeaseResponse::InvalidListCursor
    ));
}
