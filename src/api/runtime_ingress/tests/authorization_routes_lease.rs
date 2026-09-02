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

async fn malformed_authenticated_lease_observation(msg_type: u16) -> (IngressDecision, u16) {
    let family = RouteFamily::new(1);
    let session_id = 600 + u64::from(msg_type);
    let router = Arc::new(crate::runtime::Router::new());
    let lease_sink = Arc::new(crate::domains::lease::sink::LeaseDomainSink::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    ));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("lease", lease_sink);
    router.register(
        RouteAddress::new(family, Route::new(format!("inbox://session/{session_id}"))),
        inbox_mailbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session =
        make_authenticated_session_info(session_id, TransportKind::Tcp, family, &["lease://**#*"]);
    ingress.on_open(session).await.unwrap();
    let malformed = "lease://acme/lock*/db";
    let payload = if msg_type == 410 {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(malformed);
        encoder.put_u8(0);
        encoder.put_u32(0);
        Bytes::from(encoder.finish())
    } else {
        encode_lease_subscribe(malformed)
    };
    let decision = ingress
        .on_frame(
            session_id,
            ChannelId::Lease,
            MessageType::new(msg_type),
            payload,
        )
        .await;
    let response = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if let Ok(envelope) = inbox_mailbox.receiver().try_recv() {
                break envelope
                    .payload::<FrameContext>()
                    .expect("Lease response frame")
                    .clone();
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("typed Lease selector error");
    (
        decision,
        decode_domain_error_code(response.payload.as_ref()),
    )
}

async fn malformed_authenticated_lease_operation(
    msg_type: u16,
    payload: Bytes,
) -> (IngressDecision, u16) {
    let family = RouteFamily::new(1);
    let session_id = 700 + u64::from(msg_type);
    let router = Arc::new(crate::runtime::Router::new());
    let lease_sink = Arc::new(crate::domains::lease::sink::LeaseDomainSink::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    ));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("lease", lease_sink);
    router.register(
        RouteAddress::new(family, Route::new(format!("inbox://session/{session_id}"))),
        inbox_mailbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session =
        make_authenticated_session_info(session_id, TransportKind::Tcp, family, &["lease://**#*"]);
    ingress.on_open(session).await.unwrap();

    let decision = ingress
        .on_frame(
            session_id,
            ChannelId::Lease,
            MessageType::new(msg_type),
            payload,
        )
        .await;
    let response = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if !inbox_mailbox.receiver().is_empty() {
                break receive_frame(&inbox_mailbox, "typed malformed Lease operation response");
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("typed malformed Lease operation response");
    (
        decision,
        decode_domain_error_code(response.payload.as_ref()),
    )
}

#[tokio::test]
async fn should_return_typed_error_for_authenticated_malformed_lease_operations() {
    // Arrange
    let malformed = "lease://acme/locks/resource/extra";
    let mut renew = PayloadEncoder::new();
    renew.put_string(malformed);
    renew.put_string("owner");
    renew.put_u64(1);
    renew.put_u64(30);
    let mut release = PayloadEncoder::new();
    release.put_string(malformed);
    release.put_string("owner");
    release.put_u64(1);
    let mut query = PayloadEncoder::new();
    query.put_string(malformed);
    let requests = [
        (400, encode_lease_acquire(malformed, "owner", 30)),
        (401, Bytes::from(renew.finish())),
        (402, Bytes::from(release.finish())),
        (403, Bytes::from(query.finish())),
    ];

    for (msg_type, payload) in requests {
        // Act
        let (decision, code) = malformed_authenticated_lease_operation(msg_type, payload).await;

        // Assert
        assert_eq!(decision, IngressDecision::Accept);
        assert_eq!(
            code,
            crate::protocol::error_codes::lease::ERR_BAD_REQUEST,
            "message type {msg_type}"
        );
    }
}

#[tokio::test]
async fn should_return_typed_error_for_authenticated_malformed_lease_subscribe() {
    // Arrange
    let expected = crate::protocol::error_codes::lease::ERR_INVALID_SUBSCRIPTION_ROUTE;

    // Act
    let (decision, code) = malformed_authenticated_lease_observation(407).await;

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert_eq!(code, expected);
}

#[tokio::test]
async fn should_return_typed_error_for_authenticated_malformed_lease_unsubscribe() {
    // Arrange
    let expected = crate::protocol::error_codes::lease::ERR_INVALID_SUBSCRIPTION_ROUTE;

    // Act
    let (decision, code) = malformed_authenticated_lease_observation(408).await;

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert_eq!(code, expected);
}

#[tokio::test]
async fn should_return_typed_error_for_authenticated_malformed_lease_list() {
    // Arrange
    let expected = crate::protocol::error_codes::lease::ERR_INVALID_LIST_PATTERN;

    // Act
    let (decision, code) = malformed_authenticated_lease_observation(410).await;

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert_eq!(code, expected);
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
