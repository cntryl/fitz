use super::*;

#[test]
fn should_skip_disconnected_waiter_when_holder_expires() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "lease://acme/locks/waiter-disconnect";
    let key = lease_key(family, route);
    let source = RouteAddress::new(family, Route::new(route));
    let router = Arc::new(Router::new());
    let mailboxes: Vec<_> = (7..=9)
        .map(|session| {
            let address =
                RouteAddress::new(family, Route::new(format!("inbox://session/{session}")));
            let mailbox = Arc::new(Mailbox::new(8));
            router.register(address.clone(), mailbox.clone());
            (address, mailbox)
        })
        .collect();
    let sink = LeaseDomain::new_with_families(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        &[family],
    );
    let request = |session, owner: &str, index: usize, wait| LeaseAcquireRequest {
        key: key.clone(),
        owner_session_id: session,
        owner_id: owner.to_string(),
        ttl_secs: 30,
        wait_seconds: wait,
        reply_source: source.clone(),
        reply_destination: Some(mailboxes[index].0.clone()),
        channel: ClientChannel::Lease,
        route_family: family,
        correlation: None,
    };
    let LeaseResponse::Acquired { fencing_token } =
        sink.acquire_for_tests(request(7, "holder", 0, 0))
    else {
        panic!("holder acquire")
    };
    assert!(matches!(
        sink.acquire_for_tests(request(8, "first", 1, 30)),
        LeaseResponse::Queued { .. }
    ));
    assert!(matches!(
        sink.acquire_for_tests(request(9, "survivor", 2, 30)),
        LeaseResponse::Queued { .. }
    ));

    // Act
    sink.cleanup_session(8).expect("waiter disconnect");
    assert!(sink.expire_lease_for_tests(&key));
    let expired = sink.extend_for_tests(&key, "holder", fencing_token, 30);
    let grant = receive_envelope(&mailboxes[2].1, "surviving waiter grant");

    // Assert
    assert_eq!(expired, LeaseResponse::Expired);
    assert!(grant.payload::<FrameContext>().is_some());
    assert!(sink.session_leases_contain_for_tests(9, &key));
    assert!(!sink.session_leases_contain_for_tests(8, &key));
    assert_eq!(sink.pending_waiter_count_for_tests(&key), 0);
    assert_eq!(sink.lease_count(), 1);
    assert!(mailboxes[1].1.receiver().try_recv().is_err());
}
