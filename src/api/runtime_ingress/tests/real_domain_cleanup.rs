use super::*;

fn parse_rpc_request(
    session_id: u64,
    family: RouteFamily,
    request_payload: &Bytes,
    request_msg_type: u16,
) -> crate::domains::rpc::protocol::RpcRequest {
    let request_ctx = FrameContext::new(
        session_id,
        ChannelId::Rpc,
        MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );

    match crate::protocol::rpc_codec::parse_request(&request_ctx, request_payload, family)
        .expect("parse rpc request")
    {
        crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
        other => panic!("expected rpc request, found {other:?}"),
    }
}

fn assert_rpc_disconnect_response(caller_mailbox: &Mailbox, correlation_id: uuid::Uuid) {
    let request_ack = caller_mailbox
        .receiver()
        .try_recv()
        .expect("rpc request ack")
        .into_payload::<FrameContext>()
        .expect("rpc request ack frame");
    assert_eq!(request_ack.msg_type.as_u16(), 302);
    assert_eq!(request_ack.payload[0], 0);

    let disconnect_error = caller_mailbox
        .receiver()
        .try_recv()
        .expect("rpc disconnect error")
        .into_payload::<FrameContext>()
        .expect("rpc disconnect error frame");
    assert_eq!(disconnect_error.msg_type.as_u16(), 303);

    let error_response = parse_rpc_response_frame(&disconnect_error);
    assert_eq!(error_response.correlation_id, correlation_id);
    assert_eq!(error_response.seq, 0);
    assert!(error_response.stream_end);

    let (error_code, _) =
        crate::protocol::rpc_codec::decode_error_body(error_response.body.as_ref())
            .expect("decode rpc disconnect error body");
    assert_eq!(
        error_code,
        crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND
    );
}

fn receive_frame(mailbox: &Mailbox, label: &str) -> FrameContext {
    mailbox
        .receiver()
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("{label}"))
        .into_payload::<FrameContext>()
        .unwrap_or_else(|| panic!("{label} frame"))
}

fn wait_for_kv_active_transaction_count(kv_sink: &KvDomainSink, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if kv_sink.active_transaction_count() == expected {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(kv_sink.active_transaction_count(), expected);
}

fn wait_for_lease_count(lease_sink: &LeaseDomainSink, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if lease_sink.lease_count() == expected {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(lease_sink.lease_count(), expected);
}

fn wait_for_lease_subscription_count(lease_sink: &LeaseDomainSink, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if lease_sink.subscription_count() == expected {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(lease_sink.subscription_count(), expected);
}

fn wait_for_schedule_count(schedule_sink: &ScheduleDomainSink, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if schedule_sink.schedule_count() == expected {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(schedule_sink.schedule_count(), expected);
}

fn wait_for_schedule_subscription_count(schedule_sink: &ScheduleDomainSink, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if schedule_sink.subscription_count() == expected {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(schedule_sink.subscription_count(), expected);
}

fn wait_for_admin_lease_count(
    admin_read_model: &crate::control::admin::read_model::AdminReadModel,
    expected: usize,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if admin_read_model.leases(None).len() == expected {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(admin_read_model.leases(None).len(), expected);
}

#[tokio::test]
async fn should_cleanup_real_rpc_pending_request_on_close() {
    // Arrange
    let family = RouteFamily::new(93);
    let caller_session_id = 1;
    let worker_session_id = 42;
    let rpc_route = "rpc://acme/system/resource/operation";
    let rpc_address = RouteAddress::new(family, Route::new(rpc_route));
    let caller_address = RouteAddress::new(family, Route::new("inbox://session/1"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/42"));

    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let rpc_sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone()));
    let caller_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));

    router.register(caller_address.clone(), caller_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    router.register_domain_pattern("rpc", rpc_sink.clone());
    register_fallback_cleanup_domains(&router, DispatchDomain::Rpc);

    let ingress = make_cleanup_ingress(router, admin_read_model);
    let mut worker_session = make_session_info(worker_session_id, TransportKind::WebSocket);
    worker_session.route_family = family;
    ingress.on_open(worker_session).await.unwrap();

    let register_frame = crate::benchkit::build_rpc_subscribe(rpc_route);
    let (register_msg_type, register_payload) =
        crate::benchkit::extract_single_tlv_field(&register_frame);
    rpc_sink
        .deliver(Envelope::from_route(
            worker_address,
            rpc_address.clone(),
            FrameContext::new(
                worker_session_id,
                ChannelId::Rpc,
                MessageType::new(register_msg_type),
                register_payload,
                family,
            ),
        ))
        .expect("register rpc worker");
    let _register_ack = worker_mailbox
        .receiver()
        .try_recv()
        .expect("rpc worker register response");

    let request_frame = crate::benchkit::build_rpc_request(rpc_route, b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        caller_session_id,
        ChannelId::Rpc,
        MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request = parse_rpc_request(
        caller_session_id,
        family,
        &request_payload,
        request_msg_type,
    );

    rpc_sink
        .deliver(Envelope::from_route(
            caller_address,
            rpc_address,
            request_ctx,
        ))
        .expect("deliver rpc request");

    assert_eq!(rpc_sink.worker_count(), 1);
    assert_eq!(rpc_sink.pending_request_count(), 1);
    let _worker_request = worker_mailbox
        .receiver()
        .try_recv()
        .expect("worker request delivery");

    // Act
    ingress
        .on_close(worker_session_id, CloseReason::ClientClose)
        .await;

    // Assert
    assert_eq!(rpc_sink.worker_count(), 0);
    assert_eq!(rpc_sink.pending_request_count(), 0);

    assert_rpc_disconnect_response(&caller_mailbox, request.correlation_id);
}

#[tokio::test]
async fn should_cleanup_real_lease_state_on_close() {
    // Arrange
    let family = RouteFamily::new(94);
    let session_id = 94;
    let lease_route = "lease://acme/locks/resource";
    let lease_address = RouteAddress::new(family, Route::new(lease_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/94"));

    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let lease_sink = Arc::new(LeaseDomainSink::new(
        router.clone(),
        admin_read_model.clone(),
    ));
    let subscriber_mailbox = Arc::new(Mailbox::new(8));

    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    router.register_domain_pattern("lease", lease_sink.clone());
    register_fallback_cleanup_domains(&router, DispatchDomain::Lease);

    let ingress = make_cleanup_ingress(router, admin_read_model.clone());
    let mut session = make_session_info(session_id, TransportKind::WebSocket);
    session.route_family = family;
    ingress.on_open(session).await.unwrap();

    lease_sink
        .deliver(Envelope::from_route(
            subscriber_address.clone(),
            lease_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(400),
                encode_lease_acquire(lease_route, "", 30),
                family,
            ),
        ))
        .expect("acquire lease");
    lease_sink
        .deliver(Envelope::from_route(
            subscriber_address,
            lease_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(407),
                encode_lease_subscribe(lease_route),
                family,
            ),
        ))
        .expect("subscribe lease route");

    wait_for_lease_count(&lease_sink, 1);
    wait_for_lease_subscription_count(&lease_sink, 1);
    wait_for_admin_lease_count(&admin_read_model, 1);
    assert_eq!(lease_sink.lease_count(), 1);
    assert_eq!(lease_sink.subscription_count(), 1);
    assert_eq!(admin_read_model.leases(None).len(), 1);
    drain_mailbox(&subscriber_mailbox);

    // Act
    ingress.on_close(session_id, CloseReason::ClientClose).await;
    lease_sink
        .deliver(Envelope::new(
            RouteAddress::new(family, Route::new("lease://events")),
            crate::runtime::DomainPublishEvent::new(
                family,
                Route::new(lease_route),
                Bytes::from_static(b"expired"),
            ),
        ))
        .expect("publish lease event after cleanup");

    // Assert
    wait_for_lease_count(&lease_sink, 0);
    wait_for_lease_subscription_count(&lease_sink, 0);
    wait_for_admin_lease_count(&admin_read_model, 0);
    assert_eq!(lease_sink.lease_count(), 0);
    assert_eq!(lease_sink.subscription_count(), 0);
    assert!(admin_read_model.leases(None).is_empty());
    assert!(subscriber_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_cleanup_real_schedule_subscription_on_close() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 95;
    let schedule_route = "schedule://acme/jobs/nightly/run";
    let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/95"));

    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let schedule_sink = Arc::new(ScheduleDomainSink::new(
        store,
        router.clone(),
        admin_read_model.clone(),
    ));
    let subscriber_mailbox = Arc::new(Mailbox::new(8));

    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    router.register_domain_pattern("schedule", schedule_sink.clone());
    register_fallback_cleanup_domains(&router, DispatchDomain::Schedule);

    let ingress = make_cleanup_ingress(router, admin_read_model);
    let mut session = make_session_info(session_id, TransportKind::WebSocket);
    session.route_family = family;
    ingress.on_open(session).await.unwrap();

    schedule_sink
        .deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(700),
                encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
                family,
            ),
        ))
        .expect("create schedule");
    let _create_ack = receive_frame(&subscriber_mailbox, "schedule create ack");
    wait_for_schedule_count(&schedule_sink, 1);

    schedule_sink
        .deliver(Envelope::from_route(
            subscriber_address,
            schedule_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(schedule_route),
                family,
            ),
        ))
        .expect("subscribe schedule");
    let _subscribe_ack = receive_frame(&subscriber_mailbox, "schedule subscribe ack");
    wait_for_schedule_subscription_count(&schedule_sink, 1);

    // Act
    ingress.on_close(session_id, CloseReason::ClientClose).await;

    // Assert
    wait_for_schedule_count(&schedule_sink, 1);
    wait_for_schedule_subscription_count(&schedule_sink, 0);
    assert!(subscriber_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_cleanup_real_stream_session_and_subscription_on_close() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 96;
    let stream_route = "stream://acme/logs/events";
    let stream_pattern = "stream://acme/logs/*";
    let source_address = RouteAddress::new(family, Route::new("inbox://session/96"));
    let stream_address = RouteAddress::new(family, Route::new(stream_route));

    let store = crate::benchkit::create_bench_store();
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let stream_sink = Arc::new(StreamDomainSink::new(
        store,
        router.clone(),
        admin_read_model.clone(),
    ));
    let source_mailbox = Arc::new(Mailbox::new(8));

    router.register(source_address.clone(), source_mailbox.clone());
    router.register_domain_pattern("stream", stream_sink.clone());
    register_fallback_cleanup_domains(&router, DispatchDomain::Stream);

    let ingress = make_cleanup_ingress(router, admin_read_model);
    let mut session = make_session_info(session_id, TransportKind::WebSocket);
    session.route_family = family;
    ingress.on_open(session).await.unwrap();

    let begin_frame = crate::benchkit::build_stream_begin(stream_route);
    let (begin_msg_type, begin_payload) = crate::benchkit::extract_single_tlv_field(&begin_frame);
    stream_sink
        .deliver(Envelope::from_route(
            source_address.clone(),
            stream_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Pub,
                MessageType::new(begin_msg_type),
                begin_payload,
                family,
            ),
        ))
        .expect("begin stream session");

    let begin_response = source_mailbox
        .receiver()
        .try_recv()
        .expect("stream begin response")
        .into_payload::<FrameContext>()
        .expect("stream begin response frame");
    let _stream_session_id =
        crate::benchkit::parse_stream_session_id(begin_response.payload.as_ref())
            .expect("stream session id");

    let subscribe_frame = crate::benchkit::build_stream_subscribe(stream_pattern);
    let (subscribe_msg_type, subscribe_payload) =
        crate::benchkit::extract_single_tlv_field(&subscribe_frame);
    stream_sink
        .deliver(Envelope::from_route(
            source_address,
            stream_address,
            FrameContext::new(
                session_id,
                ChannelId::Pub,
                MessageType::new(subscribe_msg_type),
                subscribe_payload,
                family,
            ),
        ))
        .expect("subscribe stream pattern");
    let _subscribe_response = source_mailbox
        .receiver()
        .try_recv()
        .expect("stream subscribe response");

    assert_eq!(stream_sink.append_session_count(), 1);
    assert_eq!(stream_sink.subscription_count(), 1);

    // Act
    ingress.on_close(session_id, CloseReason::ClientClose).await;

    // Assert
    assert_eq!(stream_sink.append_session_count(), 0);
    assert_eq!(stream_sink.subscription_count(), 0);
    assert!(source_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_cleanup_real_kv_transaction_on_close() {
    // Arrange
    let family = RouteFamily::new(1);
    let first_session_id = 97;
    let second_session_id = 98;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let first_address = RouteAddress::new(family, Route::new("inbox://session/97"));
    let second_address = RouteAddress::new(family, Route::new("inbox://session/98"));

    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let kv_sink = Arc::new(KvDomainSink::new(
        store,
        router.clone(),
        admin_read_model.clone(),
    ));
    let first_mailbox = Arc::new(Mailbox::new(8));
    let second_mailbox = Arc::new(Mailbox::new(8));

    router.register(first_address.clone(), first_mailbox.clone());
    router.register(second_address.clone(), second_mailbox.clone());
    router.register_domain_pattern("kv", kv_sink.clone());
    register_fallback_cleanup_domains(&router, DispatchDomain::Kv);

    let ingress = make_cleanup_ingress(router, admin_read_model);
    let mut first_session = make_session_info(first_session_id, TransportKind::WebSocket);
    first_session.route_family = family;
    ingress.on_open(first_session).await.unwrap();

    let first_begin = crate::benchkit::build_kv_begin(kv_route, 1, 0);
    let (first_begin_msg_type, first_begin_payload) =
        crate::benchkit::extract_single_tlv_field(&first_begin);
    kv_sink
        .deliver(Envelope::from_route(
            first_address,
            kv_address.clone(),
            FrameContext::new(
                first_session_id,
                ChannelId::Pub,
                MessageType::new(first_begin_msg_type),
                first_begin_payload,
                family,
            ),
        ))
        .expect("begin first KV transaction");

    let first_begin_response = receive_frame(&first_mailbox, "first begin response");
    let first_tx_id = crate::benchkit::parse_kv_tx_id(first_begin_response.payload.as_ref())
        .expect("first tx id");

    assert_eq!(first_begin_response.payload[0], 0);
    assert!(first_tx_id > 0);
    wait_for_kv_active_transaction_count(&kv_sink, 1);

    // Act
    ingress
        .on_close(first_session_id, CloseReason::ClientClose)
        .await;

    // Assert
    wait_for_kv_active_transaction_count(&kv_sink, 0);

    let second_begin = crate::benchkit::build_kv_begin(kv_route, 1, 0);
    let (second_begin_msg_type, second_begin_payload) =
        crate::benchkit::extract_single_tlv_field(&second_begin);
    kv_sink
        .deliver(Envelope::from_route(
            second_address,
            kv_address,
            FrameContext::new(
                second_session_id,
                ChannelId::Pub,
                MessageType::new(second_begin_msg_type),
                second_begin_payload,
                family,
            ),
        ))
        .expect("begin second KV transaction");

    let second_begin_response = receive_frame(&second_mailbox, "second begin response");
    let second_tx_id = crate::benchkit::parse_kv_tx_id(second_begin_response.payload.as_ref())
        .expect("second tx id");

    assert_eq!(second_begin_response.payload[0], 0);
    assert!(second_tx_id > 0);
    wait_for_kv_active_transaction_count(&kv_sink, 1);
    assert!(first_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_reject_non_connect_before_auth() {
    let ingress = runtime_ingress_with_jwks_auth();
    let session = make_session_info(4, TransportKind::WebSocket);
    ingress.on_open(session).await.unwrap();

    let decision = ingress
        .on_frame(
            4,
            ChannelId::Pub,
            crate::protocol::tlv::MessageType::new(100),
            Bytes::from("payload"),
        )
        .await;

    assert!(matches!(decision, IngressDecision::Close(_)));
}

#[tokio::test]
async fn should_reject_control_non_connect_before_auth() {
    let ingress = runtime_ingress_with_jwks_auth();
    let session = make_session_info(5, TransportKind::WebSocket);
    ingress.on_open(session).await.unwrap();

    // Control message with wrong type
    let decision = ingress
        .on_frame(
            5,
            ChannelId::Control,
            crate::protocol::tlv::MessageType::new(2),
            Bytes::from("payload"),
        )
        .await;

    assert!(matches!(decision, IngressDecision::Close(_)));
}

#[test]
fn should_retrieve_session_info() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth();
    let session = make_session_info(42, TransportKind::Tcp);

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session.clone()).await.unwrap();
    });
    let retrieved = ingress.get_session(42);

    // Assert
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().session_id, 42);
}

#[test]
fn should_set_permissions_on_connect_with_valid_token() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
    let session = make_session_info(50, TransportKind::Tcp);

    let payload = serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#read"]
    });
    let jwt = signed_jwks_jwt(payload);

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session.clone()).await.unwrap();
        let decision = ingress
            .on_frame(
                50,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt.clone()),
            )
            .await;

        // Assert
        assert_eq!(decision, IngressDecision::Accept);
    });

    // Assert: permissions snapshot updated
    let retrieved = ingress.get_session(50).unwrap();
    assert!(retrieved.permissions_snapshot.allows(
        &crate::runtime::routing::Route::new("notice://prod/orders/create"),
        crate::auth::Access::Read
    ));
}

#[test]
fn should_set_permissions_on_connect_for_auth0_shape() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth()
        .with_auth_claims_config(crate::auth::AuthClaimsConfig::new(
            "org_id",
            None,
            crate::auth::DEFAULT_ROLE_CLAIM,
        ))
        .with_route_family_resolver(crate::auth::RouteFamilyResolverConfig::from_mappings(
            "org_id",
            [("org_acme", 2)],
        ))
        .with_route_families(&[1, 2]);
    let session = make_session_info(57, TransportKind::Tcp);
    let jwt = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "auth0|user-1",
        "exp": 9_999_999_999_u64,
        "org_id": "org_acme",
        "permissions": ["notice://prod/orders/**#read"]
    }));

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        assert_eq!(
            ingress
                .on_frame(
                    57,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await,
            IngressDecision::Accept
        );
    });

    // Assert
    let retrieved = ingress.get_session(57).unwrap();
    assert_eq!(retrieved.route_family.id(), 2);
    assert!(retrieved.permissions_snapshot.allows(
        &crate::runtime::routing::Route::new("notice://prod/orders/1"),
        crate::auth::Access::Read
    ));
}

#[test]
fn should_set_permissions_on_connect_for_entra_delegated_shape() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth()
        .with_route_family_map(&[("entra-tenant-1", 2)])
        .with_route_families(&[1, 2]);
    let session = make_session_info(58, TransportKind::Tcp);
    let jwt = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "entra-user-1",
        "exp": 9_999_999_999_u64,
        "tid": "entra-tenant-1",
        "scp": "notice.read"
    }));

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        assert_eq!(
            ingress
                .on_frame(
                    58,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await,
            IngressDecision::Accept
        );
    });

    // Assert
    let retrieved = ingress.get_session(58).unwrap();
    assert_eq!(retrieved.route_family.id(), 2);
    assert!(retrieved.permissions_snapshot.allows(
        &crate::runtime::routing::Route::new("notice://prod/orders/1"),
        crate::auth::Access::Read
    ));
}

#[test]
fn should_set_permissions_on_connect_for_entra_app_only_shape() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth()
        .with_route_family_map(&[("entra-tenant-2", 2)])
        .with_route_families(&[1, 2]);
    let session = make_session_info(59, TransportKind::Tcp);
    let jwt = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "service-principal-1",
        "exp": 9_999_999_999_u64,
        "tid": "entra-tenant-2",
        "roles": ["queue.read"]
    }));

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        assert_eq!(
            ingress
                .on_frame(
                    59,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await,
            IngressDecision::Accept
        );
    });

    // Assert
    let retrieved = ingress.get_session(59).unwrap();
    assert_eq!(retrieved.route_family.id(), 2);
    assert!(retrieved.permissions_snapshot.allows(
        &crate::runtime::routing::Route::new("queue://prod/orders/1"),
        crate::auth::Access::Read
    ));
}

#[test]
fn should_set_permissions_on_connect_for_cognito_shape() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth()
        .with_auth_claims_config(crate::auth::AuthClaimsConfig::new(
            "custom:tenant_id",
            None,
            crate::auth::DEFAULT_ROLE_CLAIM,
        ))
        .with_route_family_resolver(crate::auth::RouteFamilyResolverConfig::from_mappings(
            "custom:tenant_id",
            [("acme-prod", 2)],
        ))
        .with_route_families(&[1, 2]);
    let session = make_session_info(64, TransportKind::Tcp);
    let jwt = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "cognito-user-1",
        "exp": 9_999_999_999_u64,
        "custom:tenant_id": "acme-prod",
        "scope": "fitz/kv.read"
    }));

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        assert_eq!(
            ingress
                .on_frame(
                    64,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await,
            IngressDecision::Accept
        );
    });

    // Assert
    let retrieved = ingress.get_session(64).unwrap();
    assert_eq!(retrieved.route_family.id(), 2);
    assert!(retrieved.permissions_snapshot.allows(
        &crate::runtime::routing::Route::new("kv://prod/orders/1"),
        crate::auth::Access::Read
    ));
}

#[test]
fn should_set_permissions_on_connect_for_okta_shape() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth()
        .with_auth_claims_config(crate::auth::AuthClaimsConfig::new(
            "https://fitz.example.com/identity",
            Some("https://fitz.example.com/claims".to_string()),
            crate::auth::DEFAULT_ROLE_CLAIM,
        ))
        .with_route_family_resolver(crate::auth::RouteFamilyResolverConfig::from_mappings(
            "https://fitz.example.com/identity",
            [("okta-acme", 2)],
        ))
        .with_route_families(&[1, 2]);
    let session = make_session_info(65, TransportKind::Tcp);
    let jwt = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "okta-user-1",
        "exp": 9_999_999_999_u64,
        "https://fitz.example.com/identity": "okta-acme",
        "https://fitz.example.com/claims": {
            "permissions": ["notice://prod/orders/**#write"]
        }
    }));

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        assert_eq!(
            ingress
                .on_frame(
                    65,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt),
                )
                .await,
            IngressDecision::Accept
        );
    });

    // Assert
    let retrieved = ingress.get_session(65).unwrap();
    assert_eq!(retrieved.route_family.id(), 2);
    assert!(retrieved.permissions_snapshot.allows(
        &crate::runtime::routing::Route::new("notice://prod/orders/1"),
        crate::auth::Access::Write
    ));
}

#[test]
fn should_assign_route_families_from_verified_claims() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth()
        .with_route_families(&[1, 2])
        .with_route_family_map(&[("tenant-a", 2), ("tenant-b", 1)]);
    let session_a = make_session_info(52, TransportKind::Tcp);
    let session_b = make_session_info(53, TransportKind::Tcp);

    let jwt_a = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:a",
        "exp": 9_999_999_999_u64,
        "tid": "tenant-a",
        "permissions": ["notice://tenant-a/**#read"]
    }));
    let jwt_b = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:b",
        "exp": 9_999_999_999_u64,
        "tid": "tenant-b",
        "permissions": ["notice://tenant-b/**#read"]
    }));

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session_a).await.unwrap();
        ingress.on_open(session_b).await.unwrap();

        assert_eq!(
            ingress
                .on_frame(
                    52,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt_a),
                )
                .await,
            IngressDecision::Accept
        );
        assert_eq!(
            ingress
                .on_frame(
                    53,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt_b),
                )
                .await,
            IngressDecision::Accept
        );
    });

    // Assert
    let session_a = ingress.get_session(52).unwrap();
    let session_b = ingress.get_session(53).unwrap();
    assert_ne!(session_a.route_family, session_b.route_family);
    assert_eq!(session_a.route_family.id(), 2);
    assert_eq!(session_b.route_family.id(), 1);
}
