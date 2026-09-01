use super::*;

#[test]
fn should_authorize_lease_wildcard_selectors_as_pattern_coverage() {
    // Arrange
    let short_selector = encode_lease_subscribe("lease://**");
    let realm_selector = encode_lease_subscribe("lease://acme/**");
    let collapsing_selector = encode_lease_subscribe("lease://acme/**/db-migration/extra");

    // Act
    let short = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Lease,
        MessageType::new(407),
        short_selector.as_ref(),
        auth_spec(407).policy,
    )
    .expect("resolve lease://** auth");
    let realm = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Lease,
        MessageType::new(407),
        realm_selector.as_ref(),
        auth_spec(407).policy,
    )
    .expect("resolve lease://acme/** auth");
    let collapsing_route =
        extract_auth_route_for_domain(DispatchDomain::Lease, 407, collapsing_selector.as_ref())
            .expect("extract collapsing lease selector route")
            .expect("collapsing lease selector route");

    // Assert
    assert!(matches!(short.0, AuthorizationTargets::Registration(_)));
    assert!(matches!(realm.0, AuthorizationTargets::Registration(_)));
    assert_eq!(collapsing_route, "lease://acme/**/db-migration/extra");
}

#[test]
fn should_reject_malformed_lease_selector_at_authorization() {
    // Arrange
    let malformed = encode_lease_subscribe("lease://acme/lock*/db");

    // Act
    let result = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Lease,
        MessageType::new(407),
        malformed.as_ref(),
        auth_spec(407).policy,
    );

    // Assert
    assert!(result.is_err(), "partial wildcard segment must be rejected");
}

#[tokio::test]
async fn should_return_5012_for_malformed_authenticated_lease_list_selector() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 616;
    let router = Arc::new(crate::runtime::Router::new());
    let lease_sink = Arc::new(crate::domains::lease::sink::LeaseDomainSink::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    ));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("lease", lease_sink);
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/616")),
        inbox_mailbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        session_id,
        TransportKind::Tcp,
        family,
        &["lease://acme/*/*#read"],
    );
    ingress.on_open(session).await.unwrap();
    let mut payload = PayloadEncoder::new();
    payload.put_string("lease://acme/lock*/db");
    payload.put_u8(0);
    payload.put_u32(0);

    // Act
    let decision = ingress
        .on_frame(
            session_id,
            ChannelId::Lease,
            MessageType::new(410),
            Bytes::from(payload.finish()),
        )
        .await;
    for _ in 0..1_000 {
        if !inbox_mailbox.is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let response = receive_frame(&inbox_mailbox, "invalid Lease LIST response");

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert_eq!(
        decode_domain_error_code(response.payload.as_ref()),
        crate::protocol::error_codes::lease::ERR_INVALID_LIST_PATTERN
    );
}

#[test]
fn should_reject_lease_exact_route_with_extra_segment_before_authorization() {
    // Arrange
    let over_long = encode_lease_subscribe("lease://acme/locks/db/extra");

    // Act
    let route = extract_auth_route_for_domain(DispatchDomain::Lease, 403, over_long.as_ref());

    // Assert
    assert!(
        route.is_err(),
        "over-long exact lease route must be rejected, not truncated"
    );
}

#[tokio::test]
async fn should_authorize_lease_double_star_alias_against_equivalent_literal_star_grant_at_ingress()
{
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 614;
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("lease", domain_mailbox.clone());
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/614")),
        inbox_mailbox,
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        session_id,
        TransportKind::Tcp,
        family,
        &["lease://*/*/*#read"],
    );
    ingress.on_open(session).await.unwrap();
    let subscribe_payload = encode_lease_subscribe("lease://**");

    // Act
    let decision = ingress
        .on_frame(
            session_id,
            ChannelId::Lease,
            MessageType::new(407),
            subscribe_payload,
        )
        .await;

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert!(
        domain_mailbox.receiver().try_recv().is_ok(),
        "expected the wildcard SUBSCRIBE to reach the Lease domain sink, not be denied"
    );
}

#[tokio::test]
async fn should_still_deny_lease_double_star_alias_outside_a_narrower_grant_at_ingress() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 615;
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("lease", domain_mailbox.clone());
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/615")),
        inbox_mailbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        session_id,
        TransportKind::Tcp,
        family,
        &["lease://acme/*/*#read"],
    );
    ingress.on_open(session).await.unwrap();
    let subscribe_payload = encode_lease_subscribe("lease://**");

    // Act
    let decision = ingress
        .on_frame(
            session_id,
            ChannelId::Lease,
            MessageType::new(407),
            subscribe_payload,
        )
        .await;
    let denied_frame = receive_frame(&inbox_mailbox, "lease unauthorized response");

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert_eq!(
        decode_domain_error_code(denied_frame.payload.as_ref()),
        crate::protocol::error_codes::lease::ERR_UNAUTHORIZED
    );
    assert!(domain_mailbox.receiver().try_recv().is_err());
}
