use super::*;

#[test]
fn should_derive_canonical_routes_for_scheme_less_domain_payloads() {
    // Arrange
    let ingress = RuntimeIngress::new(false);
    let mut session = make_session_info(91, TransportKind::Tcp);
    session.route_family = crate::runtime::routing::RouteFamily::new(1);

    let mut queue_payload = Vec::new();
    let queue_wire_route = b"realm/area/tasks/receive";
    queue_payload.extend_from_slice(
        &u32::try_from(queue_wire_route.len())
            .expect("queue route length fits in u32")
            .to_be_bytes(),
    );
    queue_payload.extend_from_slice(queue_wire_route);
    queue_payload.extend_from_slice(&(1_u32).to_be_bytes());
    queue_payload.extend_from_slice(b"x");
    queue_payload.push(0);

    // Act
    let queue_route = ingress
        .derive_route_for_frame(
            &session,
            crate::protocol::tlv::MessageType::new(200),
            &Bytes::from(queue_payload),
        )
        .unwrap()
        .unwrap();

    let pattern = b"notice://patterns/*";
    let mut notice_payload = Vec::new();
    notice_payload.extend_from_slice(
        &u32::try_from(pattern.len())
            .expect("notice pattern length fits in u32")
            .to_be_bytes(),
    );
    notice_payload.extend_from_slice(pattern);
    let notice_route = ingress
        .derive_route_for_frame(
            &session,
            crate::protocol::tlv::MessageType::new(501),
            &Bytes::from(notice_payload),
        )
        .unwrap()
        .unwrap();

    // Assert
    assert_eq!(queue_route.as_str(), "queue://realm/area/tasks");
    assert_eq!(notice_route.as_str(), "notice://patterns/*");

    let stream_name = b"realm/area/stream-data/read";
    let mut stream_payload = Vec::new();
    stream_payload.extend_from_slice(
        &u32::try_from(stream_name.len())
            .expect("stream route length fits in u32")
            .to_be_bytes(),
    );
    stream_payload.extend_from_slice(stream_name);
    stream_payload.extend_from_slice(&0_u64.to_be_bytes());
    stream_payload.extend_from_slice(&1000_u64.to_be_bytes());
    stream_payload.push(0);
    let stream_route = ingress
        .derive_route_for_frame(
            &session,
            crate::protocol::tlv::MessageType::new(604),
            &Bytes::from(stream_payload),
        )
        .unwrap()
        .unwrap();
    assert_eq!(stream_route.as_str(), "stream://realm/area/stream-data");
}

#[test]
fn should_map_notice_authorization_policies() {
    // Arrange
    let publish = auth_spec(500);
    let subscribe = auth_spec(501);
    let unsubscribe = auth_spec(502);

    // Act
    let publish_policy = publish.policy;
    let subscribe_policy = subscribe.policy;
    let unsubscribe_policy = unsubscribe.policy;

    // Assert
    assert_eq!(publish.domain, DispatchDomain::Notice);
    assert_eq!(
        publish_policy,
        AuthorizationPolicy::RouteScoped(Access::Write)
    );
    assert_eq!(subscribe.domain, DispatchDomain::Notice);
    assert_eq!(
        subscribe_policy,
        AuthorizationPolicy::RouteScoped(Access::Read)
    );
    assert_eq!(unsubscribe.domain, DispatchDomain::Notice);
    assert_eq!(unsubscribe_policy, AuthorizationPolicy::SessionOwned);
}

#[test]
fn should_map_rpc_authorization_policies() {
    // Arrange
    let register = auth_spec(300);
    let unregister = auth_spec(301);
    let call = auth_spec(302);
    let response = auth_spec(303);
    let ack = crate::api::runtime_ingress::domain_registry::IngressDomainRegistry::dispatch_spec_for_msg_type(
        MessageType::new(304),
    );

    // Act
    let register_policy = register.policy;
    let unregister_policy = unregister.policy;
    let call_policy = call.policy;

    // Assert
    assert_eq!(register.domain, DispatchDomain::Rpc);
    assert_eq!(
        register_policy,
        AuthorizationPolicy::RouteScoped(Access::All)
    );
    assert_eq!(
        unregister_policy,
        AuthorizationPolicy::RouteScoped(Access::All)
    );
    assert_eq!(call_policy, AuthorizationPolicy::RouteScoped(Access::Write));
    assert_eq!(response.policy, AuthorizationPolicy::SessionOwned);
    assert!(matches!(
        ack,
        Err("invalid message type: unsupported rpc operation")
    ));
}

#[test]
fn should_map_kv_authorization_policies() {
    // Arrange
    let route = "kv://acme/app/users";
    let read_only_frame = crate::benchkit::build_kv_begin(route, 0, 0);
    let read_write_frame = crate::benchkit::build_kv_begin(route, 1, 0);
    let (_, read_only_payload) = crate::benchkit::extract_single_tlv_field(&read_only_frame);
    let (_, read_write_payload) = crate::benchkit::extract_single_tlv_field(&read_write_frame);

    // Act
    let (read_only_targets, read_only_access) = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Kv,
        MessageType::new(100),
        read_only_payload.as_ref(),
        auth_spec(100).policy,
    )
    .expect("resolve read-only begin auth");
    let (read_write_targets, read_write_access) = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Kv,
        MessageType::new(100),
        read_write_payload.as_ref(),
        auth_spec(100).policy,
    )
    .expect("resolve read-write begin auth");
    let (put_targets, _) = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Kv,
        MessageType::new(104),
        b"",
        auth_spec(104).policy,
    )
    .expect("resolve put auth");

    // Assert
    assert_eq!(read_only_access, Access::Read);
    assert_eq!(read_only_targets.span_target(), (route, 1));
    assert_eq!(read_write_access, Access::Write);
    assert_eq!(read_write_targets.span_target(), (route, 1));
    assert_eq!(put_targets.span_target(), ("<session-owned>", 1));
    assert_eq!(
        auth_spec(109).policy,
        AuthorizationPolicy::RouteScoped(Access::Read)
    );
}

#[test]
fn should_require_explicit_wildcard_policy_for_schedule_list_authorization() {
    // Arrange
    let payload = [];

    // Act
    let wildcard = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Schedule,
        MessageType::new(702),
        &payload,
        AuthorizationPolicy::WildcardScoped(Access::Read),
    )
    .expect("resolve schedule list wildcard auth");
    let missing_route = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Schedule,
        MessageType::new(702),
        &payload,
        AuthorizationPolicy::RouteScoped(Access::Read),
    );

    // Assert
    assert_eq!(wildcard.0.span_target(), ("schedule://**", 1));
    assert_eq!(wildcard.1, Access::Read);
    assert!(missing_route.is_err());
}

#[test]
fn should_authorize_wildcard_read_selectors_as_pattern_coverage() {
    // Arrange
    let queue_payload = encode_queue_reserve("queue://*/cats/*", 30, 2);
    let stream_frame = crate::benchkit::build_stream_read("stream://acme/*/*", 0);
    let (_, stream_payload) = crate::benchkit::extract_single_tlv_field(&stream_frame);

    // Act
    let queue = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Queue,
        MessageType::new(202),
        queue_payload.as_ref(),
        auth_spec(202).policy,
    )
    .expect("resolve wildcard queue reserve authorization");
    let stream = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Stream,
        MessageType::new(604),
        stream_payload.as_ref(),
        auth_spec(604).policy,
    )
    .expect("resolve wildcard stream read authorization");

    // Assert
    assert!(matches!(queue.0, AuthorizationTargets::Registration(_)));
    assert_eq!(queue.1, Access::Write);
    assert!(matches!(stream.0, AuthorizationTargets::Registration(_)));
    assert_eq!(stream.1, Access::Read);
}

#[test]
fn should_authorize_concrete_queue_reserve_plus_stream_read_as_single_routes() {
    // Arrange
    let queue_payload = encode_queue_reserve("queue://acme/cats/orders", 30, 2);
    let stream_frame = crate::benchkit::build_stream_read("stream://acme/cats/orders", 0);
    let (_, stream_payload) = crate::benchkit::extract_single_tlv_field(&stream_frame);

    // Act
    let queue = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Queue,
        MessageType::new(202),
        queue_payload.as_ref(),
        auth_spec(202).policy,
    )
    .expect("resolve concrete queue reserve authorization");
    let stream = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Stream,
        MessageType::new(604),
        stream_payload.as_ref(),
        auth_spec(604).policy,
    )
    .expect("resolve concrete stream read authorization");
    let queue_route =
        extract_auth_route_for_domain(DispatchDomain::Queue, 202, queue_payload.as_ref())
            .expect("extract concrete queue reserve route")
            .expect("queue reserve route");
    let stream_route =
        extract_auth_route_for_domain(DispatchDomain::Stream, 604, stream_payload.as_ref())
            .expect("extract concrete stream read route")
            .expect("stream read route");

    // Assert
    assert!(matches!(queue.0, AuthorizationTargets::Single(_)));
    assert_eq!(queue.1, Access::Write);
    assert!(matches!(stream.0, AuthorizationTargets::Single(_)));
    assert_eq!(stream.1, Access::Read);
    assert_eq!(queue_route, "queue://acme/cats/orders");
    assert_eq!(stream_route, "stream://acme/cats/orders");
}

#[test]
fn should_canonicalize_domain_identity_routes_for_authorization() {
    // Arrange
    let kv_begin = crate::benchkit::build_kv_begin("kv://acme/app/users/extra", 0, 0);
    let (_, kv_payload) = crate::benchkit::extract_single_tlv_field(&kv_begin);
    let queue_payload = encode_queue_send("queue://acme/app/jobs/process", b"job");
    let lease_payload = encode_lease_subscribe("lease://acme/locks/db");
    let stream_begin = crate::benchkit::build_stream_begin("stream://acme/logs/events/append");
    let (_, stream_payload) = crate::benchkit::extract_single_tlv_field(&stream_begin);

    // Act
    let kv_result = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Kv,
        MessageType::new(100),
        kv_payload.as_ref(),
        auth_spec(100).policy,
    );
    let queue_targets = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Queue,
        MessageType::new(200),
        queue_payload.as_ref(),
        auth_spec(200).policy,
    )
    .expect("resolve queue auth");
    let lease_targets = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Lease,
        MessageType::new(407),
        lease_payload.as_ref(),
        auth_spec(407).policy,
    )
    .expect("resolve lease auth");
    let stream_targets = RuntimeIngress::resolve_authorization_targets(
        DispatchDomain::Stream,
        MessageType::new(600),
        stream_payload.as_ref(),
        auth_spec(600).policy,
    )
    .expect("resolve stream auth");

    // Assert
    assert!(kv_result.is_err());
    assert_eq!(queue_targets.0.span_target(), ("queue://acme/app/jobs", 1));
    assert_eq!(lease_targets.0.span_target(), ("lease://acme/locks/db", 1));
    assert_eq!(
        stream_targets.0.span_target(),
        ("stream://acme/logs/events", 1)
    );
}

#[test]
fn should_keep_authorization_policies_for_unaffected_domains() {
    // Arrange
    let queue_send = auth_spec(200);
    let queue_watch = auth_spec(207);
    let lease_acquire = auth_spec(400);
    let lease_query = auth_spec(403);
    let stream_begin = auth_spec(600);
    let stream_append = auth_spec(601);
    let stream_read = auth_spec(604);
    let schedule_create = auth_spec(700);
    let schedule_list = auth_spec(702);
    let schedule_batch = auth_spec(706);

    // Act
    let policies = [
        queue_send.policy,
        queue_watch.policy,
        lease_acquire.policy,
        lease_query.policy,
        stream_begin.policy,
        stream_append.policy,
        stream_read.policy,
        schedule_create.policy,
        schedule_list.policy,
        schedule_batch.policy,
    ];

    // Assert
    assert_eq!(queue_send.domain, DispatchDomain::Queue);
    assert_eq!(policies[0], AuthorizationPolicy::RouteScoped(Access::Write));
    assert_eq!(policies[1], AuthorizationPolicy::RouteScoped(Access::Read));
    assert_eq!(lease_acquire.domain, DispatchDomain::Lease);
    assert_eq!(policies[2], AuthorizationPolicy::RouteScoped(Access::Write));
    assert_eq!(policies[3], AuthorizationPolicy::RouteScoped(Access::Read));
    assert_eq!(stream_begin.domain, DispatchDomain::Stream);
    assert_eq!(policies[4], AuthorizationPolicy::RouteScoped(Access::Write));
    assert_eq!(policies[5], AuthorizationPolicy::SessionOwned);
    assert_eq!(policies[6], AuthorizationPolicy::RouteScoped(Access::Read));
    assert_eq!(schedule_create.domain, DispatchDomain::Schedule);
    assert_eq!(policies[7], AuthorizationPolicy::RouteScoped(Access::Write));
    assert_eq!(
        policies[8],
        AuthorizationPolicy::WildcardScoped(Access::Read)
    );
    assert_eq!(
        policies[9],
        AuthorizationPolicy::MultiRouteScoped(Access::Write)
    );
}

#[tokio::test]
async fn should_return_notice_unauthorized_response_without_closing_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 501;
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("notice", domain_mailbox.clone());
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/501")),
        inbox_mailbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        session_id,
        TransportKind::Tcp,
        family,
        &["notice://prod/orders/**#read"],
    );
    ingress.on_open(session).await.unwrap();

    // Act
    let subscribe_decision = ingress
        .on_frame(
            session_id,
            ChannelId::Sub,
            MessageType::new(501),
            encode_notice_subscribe("notice://prod/orders/**"),
        )
        .await;
    let subscribe_frame = receive_frame(&domain_mailbox, "notice subscribe dispatch");
    let publish_decision = ingress
        .on_frame(
            session_id,
            ChannelId::Pub,
            MessageType::new(500),
            encode_notice_publish("notice://prod/orders/create", b"hello"),
        )
        .await;
    let unauthorized_frame = receive_frame(&inbox_mailbox, "notice unauthorized response");

    // Assert
    assert_eq!(subscribe_decision, IngressDecision::Accept);
    assert_eq!(subscribe_frame.msg_type, MessageType::new(501));
    assert_eq!(publish_decision, IngressDecision::Accept);
    assert_eq!(unauthorized_frame.msg_type, MessageType::new(500));
    assert_eq!(unauthorized_frame.channel_id, ChannelId::Pub);
    assert_eq!(
        decode_domain_error_code(unauthorized_frame.payload.as_ref()),
        crate::protocol::error_codes::notice::ERR_UNAUTHORIZED
    );
    assert_eq!(ingress.session_count(), 1);
    assert!(domain_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_require_all_for_rpc_worker_registration_at_ingress() {
    // Arrange
    let family = RouteFamily::new(1);
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let write_inbox = Arc::new(Mailbox::new(8));
    let all_inbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("rpc", domain_mailbox.clone());
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/610")),
        write_inbox.clone(),
    );
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/611")),
        all_inbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let write_session = make_authenticated_session_info(
        610,
        TransportKind::Tcp,
        family,
        &["rpc://acme/tasks/**#write"],
    );
    let all_session = make_authenticated_session_info(
        611,
        TransportKind::Tcp,
        family,
        &["rpc://acme/tasks/**#*"],
    );
    ingress.on_open(write_session).await.unwrap();
    ingress.on_open(all_session).await.unwrap();

    // Act
    let register_frame = crate::benchkit::build_rpc_subscribe("rpc://acme/tasks/worker");
    let (_, register_payload) = crate::benchkit::extract_single_tlv_field(&register_frame);
    let denied_decision = ingress
        .on_frame(
            610,
            ChannelId::Rpc,
            MessageType::new(300),
            register_payload.clone(),
        )
        .await;
    let denied_frame = receive_frame(&write_inbox, "rpc unauthorized response");
    let allowed_decision = ingress
        .on_frame(611, ChannelId::Rpc, MessageType::new(300), register_payload)
        .await;
    let allowed_frame = receive_frame(&domain_mailbox, "rpc register dispatch");

    // Assert
    assert_eq!(denied_decision, IngressDecision::Accept);
    assert_eq!(
        decode_domain_error_code(denied_frame.payload.as_ref()),
        crate::protocol::error_codes::rpc::ERR_UNAUTHORIZED
    );
    assert_eq!(allowed_decision, IngressDecision::Accept);
    assert_eq!(allowed_frame.msg_type, MessageType::new(300));
    assert!(all_inbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_reject_rpc_registration_exceeding_permission_match_set_at_ingress() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 612;
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("rpc", domain_mailbox.clone());
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/612")),
        inbox_mailbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        session_id,
        TransportKind::Tcp,
        family,
        &["rpc://acme/orders/*/*#*"],
    );
    ingress.on_open(session).await.unwrap();
    let register_frame = crate::benchkit::build_rpc_subscribe("rpc://acme/orders/**/created");
    let (_, register_payload) = crate::benchkit::extract_single_tlv_field(&register_frame);

    // Act
    let decision = ingress
        .on_frame(
            session_id,
            ChannelId::Rpc,
            MessageType::new(300),
            register_payload,
        )
        .await;
    let denied_frame = receive_frame(&inbox_mailbox, "rpc unauthorized response");

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert_eq!(
        decode_domain_error_code(denied_frame.payload.as_ref()),
        crate::protocol::error_codes::rpc::ERR_UNAUTHORIZED
    );
    assert!(domain_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_close_auth_required_session_before_dispatch_given_unparseable_registration_route() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 613;
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("notice", domain_mailbox.clone());
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        session_id,
        TransportKind::Tcp,
        family,
        &["notice://acme/allowed/**#read"],
    );
    ingress.on_open(session).await.unwrap();
    let route = b"acme/for*bidden";
    let mut payload = Vec::with_capacity(4 + route.len());
    payload.extend_from_slice(
        &u32::try_from(route.len())
            .expect("route length fits in u32")
            .to_be_bytes(),
    );
    payload.extend_from_slice(route);

    // Act
    let decision = ingress
        .on_frame(
            session_id,
            ChannelId::Sub,
            MessageType::new(501),
            Bytes::from(payload),
        )
        .await;

    // Assert
    assert!(matches!(
        decision,
        IngressDecision::Close(reason) if reason.contains("authorization parse failed")
    ));
    assert!(domain_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_return_terminal_rpc_response_for_unauthorized_call_at_ingress() {
    // Arrange
    let family = RouteFamily::new(1);
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let caller_inbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("rpc", domain_mailbox.clone());
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/612")),
        caller_inbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        612,
        TransportKind::Tcp,
        family,
        &["rpc://acme/tasks/worker#read"],
    );
    ingress.on_open(session).await.unwrap();
    let request_frame = crate::benchkit::build_rpc_request("rpc://acme/tasks/worker", b"run");
    let (_, request_payload) = crate::benchkit::extract_single_tlv_field(&request_frame);
    let expected_correlation_id =
        crate::protocol::rpc_codec::extract_request_correlation_id(&request_payload)
            .expect("request correlation id");

    // Act
    let denied_decision = ingress
        .on_frame(612, ChannelId::Rpc, MessageType::new(302), request_payload)
        .await;
    let denied_frame = receive_frame(&caller_inbox, "rpc call unauthorized response");
    let response = parse_rpc_response_frame(&denied_frame);

    // Assert
    assert_eq!(denied_decision, IngressDecision::Accept);
    assert_eq!(denied_frame.msg_type, MessageType::new(303));
    assert_eq!(response.correlation_id, expected_correlation_id);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);
    assert_eq!(
        decode_domain_error_code(&response.body),
        crate::protocol::error_codes::rpc::ERR_UNAUTHORIZED
    );
    assert!(domain_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_authorize_kv_begin_by_mode_while_keeping_tx_ops_session_owned_at_ingress() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 620;
    let route = "kv://acme/app/users";
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("kv", domain_mailbox.clone());
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/620")),
        inbox_mailbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        session_id,
        TransportKind::Tcp,
        family,
        &["kv://acme/app/users#read"],
    );
    ingress.on_open(session).await.unwrap();

    // Act
    let read_only_begin = crate::benchkit::build_kv_begin(route, 0, 0);
    let (_, read_only_payload) = crate::benchkit::extract_single_tlv_field(&read_only_begin);
    let read_only_decision = ingress
        .on_frame(
            session_id,
            ChannelId::Pub,
            MessageType::new(100),
            read_only_payload,
        )
        .await;
    let read_only_frame = receive_frame(&domain_mailbox, "kv read-only begin dispatch");

    let read_write_begin = crate::benchkit::build_kv_begin(route, 1, 0);
    let (_, read_write_payload) = crate::benchkit::extract_single_tlv_field(&read_write_begin);
    let read_write_decision = ingress
        .on_frame(
            session_id,
            ChannelId::Pub,
            MessageType::new(100),
            read_write_payload,
        )
        .await;
    let denied_frame = receive_frame(&inbox_mailbox, "kv unauthorized response");

    let put_frame = crate::benchkit::build_kv_put(7, route, b"name", b"ada");
    let (_, put_payload) = crate::benchkit::extract_single_tlv_field(&put_frame);
    let put_decision = ingress
        .on_frame(
            session_id,
            ChannelId::Pub,
            MessageType::new(104),
            put_payload,
        )
        .await;
    let put_dispatch = receive_frame(&domain_mailbox, "kv put dispatch");

    let commit_frame = crate::benchkit::build_kv_commit(7, route);
    let (_, commit_payload) = crate::benchkit::extract_single_tlv_field(&commit_frame);
    let commit_decision = ingress
        .on_frame(
            session_id,
            ChannelId::Pub,
            MessageType::new(101),
            commit_payload,
        )
        .await;
    let commit_dispatch = receive_frame(&domain_mailbox, "kv commit dispatch");

    let rollback_frame = crate::benchkit::build_kv_rollback(7, route);
    let (_, rollback_payload) = crate::benchkit::extract_single_tlv_field(&rollback_frame);
    let rollback_decision = ingress
        .on_frame(
            session_id,
            ChannelId::Pub,
            MessageType::new(102),
            rollback_payload,
        )
        .await;
    let rollback_dispatch = receive_frame(&domain_mailbox, "kv rollback dispatch");

    // Assert
    assert_eq!(read_only_decision, IngressDecision::Accept);
    assert_eq!(read_only_frame.msg_type, MessageType::new(100));
    assert_eq!(read_write_decision, IngressDecision::Accept);
    assert_eq!(
        decode_domain_error_code(denied_frame.payload.as_ref()),
        crate::protocol::error_codes::kv::ERR_UNAUTHORIZED
    );
    assert_eq!(put_decision, IngressDecision::Accept);
    assert_eq!(put_dispatch.msg_type, MessageType::new(104));
    assert_eq!(commit_decision, IngressDecision::Accept);
    assert_eq!(commit_dispatch.msg_type, MessageType::new(101));
    assert_eq!(rollback_decision, IngressDecision::Accept);
    assert_eq!(rollback_dispatch.msg_type, MessageType::new(102));
}

#[tokio::test]
async fn should_require_exact_kv_realm_area_and_resource_at_ingress() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 622;
    let exact_route = "kv://acme/app/users";
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("kv", domain_mailbox.clone());
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/622")),
        inbox_mailbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        session_id,
        TransportKind::Tcp,
        family,
        &["kv://acme/app/users#write"],
    );
    ingress.on_open(session).await.unwrap();

    // Act
    let exact_begin = crate::benchkit::build_kv_begin(exact_route, 1, 0);
    let (_, exact_payload) = crate::benchkit::extract_single_tlv_field(&exact_begin);
    let exact_decision = ingress
        .on_frame(
            session_id,
            ChannelId::Pub,
            MessageType::new(100),
            exact_payload,
        )
        .await;
    let exact_dispatch = receive_frame(&domain_mailbox, "exact KV begin dispatch");

    let mismatched_routes = [
        "kv://other/app/users",
        "kv://acme/other/users",
        "kv://acme/app/other",
    ];
    let mut denial_codes = Vec::new();
    for route in mismatched_routes {
        let begin = crate::benchkit::build_kv_begin(route, 1, 0);
        let (_, payload) = crate::benchkit::extract_single_tlv_field(&begin);
        let decision = ingress
            .on_frame(session_id, ChannelId::Pub, MessageType::new(100), payload)
            .await;
        assert_eq!(decision, IngressDecision::Accept);
        let denial = receive_frame(&inbox_mailbox, "mismatched KV begin denial");
        denial_codes.push(decode_domain_error_code(denial.payload.as_ref()));
    }

    // Assert
    assert_eq!(exact_decision, IngressDecision::Accept);
    assert_eq!(exact_dispatch.msg_type, MessageType::new(100));
    assert_eq!(
        denial_codes,
        vec![crate::protocol::error_codes::kv::ERR_UNAUTHORIZED; 3]
    );
    assert!(domain_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_deny_expired_session_owned_frame_without_closing_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 621;
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let inbox_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("kv", domain_mailbox.clone());
    router.register(
        RouteAddress::new(family, Route::new("inbox://session/621")),
        inbox_mailbox.clone(),
    );
    let ingress = runtime_ingress_with_jwks_auth().with_router(router);
    let session = make_authenticated_session_info(
        session_id,
        TransportKind::Tcp,
        family,
        &["kv://acme/app/users#*"],
    );
    ingress.on_open(session).await.unwrap();
    install_expired_session_actor(&ingress, session_id, &["kv://acme/app/users#*"]);

    // Act
    let put_frame = crate::benchkit::build_kv_put(7, "kv://acme/app/users", b"name", b"ada");
    let (_, put_payload) = crate::benchkit::extract_single_tlv_field(&put_frame);
    let decision = ingress
        .on_frame(
            session_id,
            ChannelId::Pub,
            MessageType::new(104),
            put_payload,
        )
        .await;
    let denied_frame = receive_frame(&inbox_mailbox, "expired session-owned denial");

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    assert_eq!(denied_frame.channel_id, ChannelId::Pub);
    assert_eq!(denied_frame.msg_type, MessageType::new(104));
    assert_eq!(
        decode_domain_error_code(denied_frame.payload.as_ref()),
        crate::protocol::error_codes::kv::ERR_UNAUTHORIZED
    );
    assert_eq!(ingress.session_count(), 1);
    assert!(domain_mailbox.receiver().try_recv().is_err());
}

#[test]
fn should_reject_unassigned_notice_message_types() {
    // Arrange
    let msg_type = crate::protocol::tlv::MessageType::new(505);

    // Act
    let result = RuntimeIngress::domain_dispatch_for_msg_type(msg_type);

    // Assert
    assert!(result.is_err());
}
