//! Lease LIST: patterned inventory reads, pagination/cursor semantics, and
//! waiter exclusion.

use super::common::*;

async fn should_list_leases_matching_wildcard_selector<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let selector = "lease://acme/renderers/*";
    let mut holder = C::connect(server).await.expect("holder connect");
    let mut lister = C::connect(server).await.expect("lister connect");

    for route in [
        "lease://acme/renderers/document-1",
        "lease://acme/renderers/document-2",
    ] {
        let response = holder
            .send_and_receive(&build_lease_acquire_immediate(route, "owner1", 30), 2000)
            .await
            .expect("acquire matching lease");
        let (_msg_type, status, _data) = parse_lease_response(&response);
        assert_eq!(status, 0, "Expected acquire success: {route}");
    }
    let response = holder
        .send_and_receive(
            &build_lease_acquire_immediate("lease://acme/printers/document-3", "owner1", 30),
            2000,
        )
        .await
        .expect("acquire non-matching lease");
    let (_msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(status, 0, "Expected non-matching acquire success");

    // Act
    let list_response = lister
        .send_and_receive(&build_lease_list(selector, 0), 2000)
        .await
        .expect("list wildcard lease inventory");
    let (_msg_type, status, data) = parse_lease_response(&list_response);
    assert_eq!(status, 0, "Expected LIST success");
    let page = parse_lease_list_page(&data).expect("lease list page");

    // Assert
    assert_eq!(page.next_cursor, None, "Expected a single unpaged page");
    let mut routes: Vec<_> = page.items.iter().map(|item| item.route.clone()).collect();
    routes.sort();
    assert_eq!(
        routes,
        vec![
            "lease://acme/renderers/document-1".to_string(),
            "lease://acme/renderers/document-2".to_string(),
        ]
    );
    for item in &page.items {
        assert_eq!(item.owner_id, "owner1", "Expected logical owner_id");
        assert_eq!(item.renewals, 0);
        assert!(item.expires_in_secs > 0 && item.expires_in_secs <= 30);
        assert_ne!(
            item.holder_incarnation, 0,
            "holder_incarnation should be a derived nonzero token"
        );
    }
}

async fn should_paginate_lease_list_across_multiple_pages<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let selector = "lease://acme/paginated/*";
    let mut holder = C::connect(server).await.expect("holder connect");
    let mut lister = C::connect(server).await.expect("lister connect");
    let routes = [
        "lease://acme/paginated/a",
        "lease://acme/paginated/b",
        "lease://acme/paginated/c",
    ];
    for route in routes {
        let response = holder
            .send_and_receive(&build_lease_acquire_immediate(route, "owner1", 30), 2000)
            .await
            .expect("acquire lease for pagination");
        let (_msg_type, status, _data) = parse_lease_response(&response);
        assert_eq!(status, 0, "Expected acquire success: {route}");
    }

    // Act: page through with limit=1, three times.
    let mut seen = std::collections::HashSet::new();
    let mut cursor: Option<(u64, u32)> = None;
    let mut pages = 0;
    loop {
        let frame = match cursor {
            None => build_lease_list(selector, 1),
            Some((snapshot_id, offset)) => {
                build_lease_list_with_cursor(selector, snapshot_id, offset, 1)
            }
        };
        let response = lister
            .send_and_receive(&frame, 2000)
            .await
            .expect("list page");
        let (_msg_type, status, data) = parse_lease_response(&response);
        assert_eq!(status, 0, "Expected LIST page success");
        let page = parse_lease_list_page(&data).expect("lease list page");
        pages += 1;
        assert!(pages <= 10, "pagination did not terminate");

        assert_eq!(page.items.len(), 1, "Expected exactly one item per page");
        for item in &page.items {
            assert!(seen.insert(item.route.clone()), "duplicate item in scan");
        }

        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    // Assert: no duplicates or omissions across the whole scan.
    let mut seen: Vec<_> = seen.into_iter().collect();
    seen.sort();
    assert_eq!(
        seen,
        routes.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
}

async fn should_reject_lease_list_cursor_from_different_selector<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let selector = "lease://acme/cursors/*";
    let mut holder = C::connect(server).await.expect("holder connect");
    let mut lister = C::connect(server).await.expect("lister connect");
    for route in ["lease://acme/cursors/a", "lease://acme/cursors/b"] {
        let response = holder
            .send_and_receive(&build_lease_acquire_immediate(route, "owner1", 30), 2000)
            .await
            .expect("acquire lease for cursor mismatch test");
        let (_msg_type, status, _data) = parse_lease_response(&response);
        assert_eq!(status, 0);
    }
    let first_page = lister
        .send_and_receive(&build_lease_list(selector, 1), 2000)
        .await
        .expect("list first page");
    let (_msg_type, status, data) = parse_lease_response(&first_page);
    assert_eq!(status, 0);
    let page = parse_lease_list_page(&data).expect("lease list page");
    let (snapshot_id, offset) = page.next_cursor.expect("expected a continuation cursor");

    // Act: reuse the cursor with a different selector string.
    let mismatched = lister
        .send_and_receive(
            &build_lease_list_with_cursor("lease://acme/other-selector/*", snapshot_id, offset, 1),
            2000,
        )
        .await
        .expect("mismatched cursor response");

    // Assert
    let (_msg_type, status, data) = parse_lease_response(&mismatched);
    assert_eq!(status, 1, "Expected mismatched cursor to fail");
    let (code, _message) = fitz::protocol::error_codes::decode_error_body(&data)
        .expect("Lease LIST cursor error envelope");
    assert_eq!(
        code,
        fitz::protocol::error_codes::lease::ERR_INVALID_LIST_CURSOR
    );
}

async fn should_exclude_queued_waiters_from_lease_list<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let route = "lease://acme/waiters/resource";
    let mut holder = C::connect(server).await.expect("holder connect");
    let mut waiter = C::connect(server).await.expect("waiter connect");
    let mut lister = C::connect(server).await.expect("lister connect");

    let acquire_response = holder
        .send_and_receive(&build_lease_acquire_immediate(route, "owner1", 30), 2000)
        .await
        .expect("acquire lease");
    let (_msg_type, status, _data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0);

    let queue_frame = build_lease_acquire_with_wait(route, "owner2", 30, 5);
    let queue_response = waiter
        .send_and_receive(&queue_frame, 2000)
        .await
        .expect("queued acquire response");
    let (_msg_type, status, data) = parse_lease_response(&queue_response);
    assert_eq!(
        status, 0,
        "Expected the wait-listed acquire to be queued, not rejected"
    );
    assert_eq!(
        parse_lease_acquire_response_type(&data).expect("acquire response type"),
        fitz::protocol::lease_codec::acquire_response_type::QUEUED,
        "Expected acquire to be queued"
    );

    // Act
    let list_response = lister
        .send_and_receive(&build_lease_list(route, 0), 2000)
        .await
        .expect("list exact route with a pending waiter");

    // Assert
    let (_msg_type, status, data) = parse_lease_response(&list_response);
    assert_eq!(status, 0);
    let page = parse_lease_list_page(&data).expect("lease list page");
    assert_eq!(
        page.items.len(),
        1,
        "Expected only the held lease, not the pending waiter"
    );
    assert_eq!(page.items[0].owner_id, "owner1");
}

// ===== TCP TESTS =====

#[tokio::test]
async fn should_list_leases_matching_wildcard_selector_tcp() {
    let server = TestServer::start().await.expect("start");
    should_list_leases_matching_wildcard_selector::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_paginate_lease_list_across_multiple_pages_tcp() {
    let server = TestServer::start().await.expect("start");
    should_paginate_lease_list_across_multiple_pages::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_lease_list_cursor_from_different_selector_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_lease_list_cursor_from_different_selector::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_exclude_queued_waiters_from_lease_list_tcp() {
    let server = TestServer::start().await.expect("start");
    should_exclude_queued_waiters_from_lease_list::<TcpLeaseConnector>(&server).await;
}

// ===== WEBSOCKET TESTS =====

#[tokio::test]
async fn should_list_leases_matching_wildcard_selector_ws() {
    let server = TestServer::start().await.expect("start");
    should_list_leases_matching_wildcard_selector::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_paginate_lease_list_across_multiple_pages_ws() {
    let server = TestServer::start().await.expect("start");
    should_paginate_lease_list_across_multiple_pages::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_lease_list_cursor_from_different_selector_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_lease_list_cursor_from_different_selector::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_exclude_queued_waiters_from_lease_list_ws() {
    let server = TestServer::start().await.expect("start");
    should_exclude_queued_waiters_from_lease_list::<WsLeaseConnector>(&server).await;
}
