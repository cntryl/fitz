use super::super::requests::RpcPendingRequestInit;
use super::*;
use crate::runtime::routing::RouteAddress;
use std::time::{Duration, Instant};

#[test]
fn should_reserve_correlation_while_request_is_queued() {
    // Arrange
    let family = RouteFamily::new(1);
    let correlation_id = uuid::Uuid::new_v4();
    let route = Route::new("rpc://bench/system/resource/operation");
    let request = crate::domains::rpc::protocol::RpcRequest::new(
        family,
        correlation_id,
        route,
        bytes::Bytes::from_static(b"queued"),
    );
    let queued = RpcQueuedRequest::from_request(
        request,
        7,
        RouteAddress::new(
            family,
            Route::new("inbox://bench/system/resource/operation"),
        ),
        Instant::now() + Duration::from_secs(30),
    );
    let mut table = RpcPendingTable::new();

    // Act
    table.track_queued_for_family(family, correlation_id, queued);

    // Assert
    assert!(table.contains_correlation_in_family(family, &correlation_id));
    assert_eq!(table.live_len(), 1);
    assert!(table
        .remove_queued_for_family(family, &correlation_id)
        .is_some());
    assert_eq!(table.live_len(), 0);
}

#[test]
fn should_ignore_stale_queued_expiration_after_replacement() {
    // Arrange
    let family = RouteFamily::new(1);
    let correlation_id = uuid::Uuid::new_v4();
    let route = Route::new("rpc://bench/system/resource/operation");
    let now = Instant::now();
    let make_queued = |expires_at| {
        RpcQueuedRequest::from_request(
            crate::domains::rpc::protocol::RpcRequest::new(
                family,
                correlation_id,
                route.clone(),
                bytes::Bytes::from_static(b"queued"),
            ),
            7,
            RouteAddress::new(
                family,
                Route::new("inbox://bench/system/resource/operation"),
            ),
            expires_at,
        )
    };
    let mut table = RpcPendingTable::new();
    table.track_queued_for_family(
        family,
        correlation_id,
        make_queued(now + Duration::from_secs(1)),
    );
    table.track_queued_for_family(
        family,
        correlation_id,
        make_queued(now + Duration::from_secs(60)),
    );

    // Act
    let early = table.next_expired_queued_key(now + Duration::from_secs(2));
    let due = table.next_expired_queued_key(now + Duration::from_secs(61));

    // Assert
    assert_eq!(early, None);
    assert_eq!(due.map(|key| key.correlation_id), Some(correlation_id));
}

#[test]
fn should_preserve_legacy_pending_request_when_cancellation_is_unsupported() {
    // Arrange
    let family = RouteFamily::new(1);
    let correlation_id = uuid::Uuid::new_v4();
    let route = Route::new("rpc://bench/system/resource/operation");
    let caller_inbox = RouteAddress::new(family, Route::new("inbox://bench/system/caller"));
    let worker_addr = RouteAddress::new(family, route.clone());
    let expires_at = Instant::now() + Duration::from_secs(30);
    let mut pending = RpcPendingRequest::new(RpcPendingRequestInit {
        route,
        caller_session_id: 7,
        caller_inbox_addr: caller_inbox.clone(),
        registration_addr: worker_addr,
        registration_session_id: 42,
        registration_id: 1,
        submitted_at: chrono::Utc::now(),
        submitted_at_instant: Instant::now(),
        expires_at,
    });
    pending.supports_cancellation = false;
    let mut table = RpcPendingTable::new();
    table.track_pending_for_family(family, correlation_id, pending);

    // Act
    let wrong_family = table.cancel_pending_for_caller(
        RouteFamily::new(2),
        &correlation_id,
        7,
        Instant::now() + Duration::from_secs(5),
    );
    let wrong_caller = table.cancel_pending_for_caller(
        family,
        &correlation_id,
        8,
        Instant::now() + Duration::from_secs(5),
    );
    let result = table.cancel_pending_for_caller(
        family,
        &correlation_id,
        7,
        Instant::now() + Duration::from_secs(5),
    );

    // Assert
    assert_eq!(
        wrong_family,
        RpcCancellationDisposition::UnauthorizedOrUnknown
    );
    assert_eq!(
        wrong_caller,
        RpcCancellationDisposition::UnauthorizedOrUnknown
    );
    assert_eq!(
        result,
        RpcCancellationDisposition::Dispatched {
            worker_session_id: 42,
            supports_cancellation: false,
        }
    );
    let tracked = table
        .pending_for_key(&RpcCorrelationKey {
            family,
            correlation_id,
        })
        .expect("legacy request remains pending");
    assert!(!tracked.cancelled);
    assert!(!tracked.close_requested);
    assert_eq!(tracked.dispatch_info.caller_inbox_addr, Some(caller_inbox));
    assert_eq!(tracked.expires_at, expires_at);
}

#[test]
fn should_bound_stale_expiration_entries_after_fast_completions() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let now = Instant::now();
    let expires_at = now + Duration::from_secs(30);
    let mut table = RpcPendingTable::new();

    // Act
    for _ in 0..1024 {
        let correlation_id = uuid::Uuid::new_v4();
        let pending = make_pending_request(family, &route, now, expires_at);
        table.track_pending_for_family(family, correlation_id, pending);
        let _ = table.remove(&RpcCorrelationKey {
            family,
            correlation_id,
        });

        let queued_id = uuid::Uuid::new_v4();
        let queued = make_queued_request(family, &route, queued_id, expires_at);
        table.track_queued_for_family(family, queued_id, queued);
        let _ = table.remove_queued_for_family(family, &queued_id);
    }

    let live_pending_id = uuid::Uuid::new_v4();
    table.track_pending_for_family(
        family,
        live_pending_id,
        make_pending_request(family, &route, now, expires_at),
    );
    let live_queued_id = uuid::Uuid::new_v4();
    table.track_queued_for_family(
        family,
        live_queued_id,
        make_queued_request(family, &route, live_queued_id, expires_at),
    );

    // Assert
    assert!(table.expirations.len() <= expiration_heap_limit(1));
    assert!(table.queued_expirations.len() <= expiration_heap_limit(1));
    assert_eq!(
        table
            .next_expired_pending_key(expires_at)
            .map(|key| key.correlation_id),
        Some(live_pending_id)
    );
    assert_eq!(
        table
            .next_expired_queued_key(expires_at)
            .map(|key| key.correlation_id),
        Some(live_queued_id)
    );
}

fn make_pending_request(
    family: RouteFamily,
    route: &Route,
    now: Instant,
    expires_at: Instant,
) -> RpcPendingRequest {
    use crate::runtime::routing::RouteAddress;

    let worker_addr = RouteAddress::new(family, route.clone());
    let caller_inbox_addr = RouteAddress::new(
        family,
        Route::new("inbox://bench/system/resource/operation"),
    );
    RpcPendingRequest::new(RpcPendingRequestInit {
        route: route.clone(),
        caller_session_id: 7,
        caller_inbox_addr,
        registration_addr: worker_addr,
        registration_session_id: 8,
        registration_id: 9,
        submitted_at: chrono::Utc::now(),
        submitted_at_instant: now,
        expires_at,
    })
}

fn make_queued_request(
    family: RouteFamily,
    route: &Route,
    correlation_id: uuid::Uuid,
    expires_at: Instant,
) -> RpcQueuedRequest {
    use crate::runtime::routing::RouteAddress;

    RpcQueuedRequest::from_request(
        crate::domains::rpc::protocol::RpcRequest::new(
            family,
            correlation_id,
            route.clone(),
            bytes::Bytes::from_static(b"queued"),
        ),
        7,
        RouteAddress::new(
            family,
            Route::new("inbox://bench/system/resource/operation"),
        ),
        expires_at,
    )
}
