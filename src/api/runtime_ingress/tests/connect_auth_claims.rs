use super::*;

struct BackpressuredSink;

impl MailboxSink for BackpressuredSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(DeliveryError::MailboxFull {
            capacity: 1,
            current_len: 1,
        })
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_reject_connect_with_unprovisioned_resolved_route_family() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth()
        .with_route_families(&[1])
        .with_route_family_map(&[("acme-prod", 2)]);
    let session = make_session_info(55, TransportKind::Tcp);
    let jwt = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:55",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#read"]
    }));

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    let decision = rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        ingress
            .on_frame(
                55,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt),
            )
            .await
    });

    // Assert
    assert!(matches!(decision, IngressDecision::Close(_)));
}

#[test]
fn should_reject_connect_with_unmapped_identity_claim() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("mapped", 1)]);
    let session = make_session_info(56, TransportKind::Tcp);
    let jwt = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:56",
        "exp": 9_999_999_999_u64,
        "tid": "unmapped",
        "permissions": ["notice://prod/orders/**#read"]
    }));

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    let decision = rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        ingress
            .on_frame(
                56,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt),
            )
            .await
    });

    // Assert
    assert!(matches!(decision, IngressDecision::Close(_)));
}

#[test]
fn should_preserve_resolved_route_families_when_sessions_reconnect_in_reverse_order() {
    // Arrange
    let jwt_for = |subject: &str| {
        signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": subject,
            "exp": 9_999_999_999_u64,
            "tid": subject,
            "permissions": ["kv://shared/data/item#read"]
        }))
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let first = runtime_ingress_with_jwks_auth()
        .with_route_families(&[1, 2])
        .with_route_family_map(&[("tenant-a", 1), ("tenant-b", 2)]);
    let second = runtime_ingress_with_jwks_auth()
        .with_route_families(&[1, 2])
        .with_route_family_map(&[("tenant-a", 1), ("tenant-b", 2)]);

    // Act
    rt.block_on(async {
        first
            .on_open(make_session_info(60, TransportKind::Tcp))
            .await
            .unwrap();
        first
            .on_open(make_session_info(61, TransportKind::Tcp))
            .await
            .unwrap();
        assert_eq!(
            first
                .on_frame(
                    60,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt_for("tenant-a")),
                )
                .await,
            IngressDecision::Accept
        );
        assert_eq!(
            first
                .on_frame(
                    61,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt_for("tenant-b")),
                )
                .await,
            IngressDecision::Accept
        );

        second
            .on_open(make_session_info(62, TransportKind::Tcp))
            .await
            .unwrap();
        second
            .on_open(make_session_info(63, TransportKind::Tcp))
            .await
            .unwrap();
        assert_eq!(
            second
                .on_frame(
                    62,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt_for("tenant-b")),
                )
                .await,
            IngressDecision::Accept
        );
        assert_eq!(
            second
                .on_frame(
                    63,
                    ChannelId::Control,
                    crate::protocol::tlv::MessageType::CONNECT,
                    Bytes::from(jwt_for("tenant-a")),
                )
                .await,
            IngressDecision::Accept
        );
    });

    // Assert
    assert_eq!(first.get_session(60).unwrap().route_family.id(), 1);
    assert_eq!(first.get_session(61).unwrap().route_family.id(), 2);
    assert_eq!(second.get_session(62).unwrap().route_family.id(), 2);
    assert_eq!(second.get_session(63).unwrap().route_family.id(), 1);
}

#[test]
fn should_reject_connect_with_malformed_permissions() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
    let session = make_session_info(51, TransportKind::Tcp);

    let payload = serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["badperm#oops"]
    });
    let jwt = signed_jwks_jwt(payload);

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session.clone()).await.unwrap();
        let decision = ingress
            .on_frame(
                51,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt.clone()),
            )
            .await;

        // Assert
        assert!(matches!(decision, IngressDecision::Close(_)));
    });
}

#[test]
fn should_reject_connect_when_issuer_cannot_derive_jwks() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth();
    let session = make_session_info(54, TransportKind::Tcp);
    let jwt = signed_jwks_jwt(serde_json::json!({
        "iss": "not-a-valid-issuer",
        "aud": "fitz-broker",
        "sub": "user:54",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#read"]
    }));

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        let decision = ingress
            .on_frame(
                54,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt),
            )
            .await;

        // Assert
        assert!(matches!(decision, IngressDecision::Close(_)));
    });
}

#[test]
fn should_set_permissions_on_connect_with_issuer_valid_signature() {
    // Arrange
    use base64::Engine;
    use jsonwebtoken::{EncodingKey, Header};
    let issuer = "https://idp.example/jwks-valid";
    let jwks_url = "https://idp.example/jwks-valid/.well-known/jwks.json";

    let ingress = runtime_ingress_with_jwks_auth()
        .with_auth_config(crate::auth::AuthConfig::jwks(
            vec!["fitz-broker".to_string()],
            vec![crate::auth::JwksIssuerConfig {
                issuer: issuer.to_string(),
                jwks_url: jwks_url.to_string(),
            }],
        ))
        .with_route_family_map(&[("acme-prod", 1)]);
    let session = make_session_info(80, TransportKind::Tcp);

    // Build a signed HS256 token and cache a matching oct key under the issuer's derived JWKS URL
    let payload = serde_json::json!({
        "iss": issuer,
        "aud": "fitz-broker",
        "sub": "user:80",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#write"]
    });

    let secret = b"supersecretkey".to_vec();
    let header = Header::new(jsonwebtoken::Algorithm::HS256);
    let jwt = jsonwebtoken::encode(
        &header,
        &payload,
        &EncodingKey::from_secret(secret.as_slice()),
    )
    .unwrap();

    // Cache JWKS for the derived URL
    let k_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&secret);
    let jwks =
        serde_json::json!({ "keys": [ { "kty": "oct", "kid": "", "k": k_b64 } ] }).to_string();
    crate::auth::cache_jwks_from_json(jwks_url, &jwks).unwrap();

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session.clone()).await.unwrap();
        let decision = ingress
            .on_frame(
                80,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt.clone()),
            )
            .await;

        // Assert
        assert_eq!(decision, IngressDecision::Accept);
    });

    // Assert: actor authorizes write
    let actor_ref = ingress.session_actors.get(&80).unwrap();
    let actor = actor_ref.value();
    assert!(actor.authorize(
        &crate::runtime::routing::Route::new("notice://prod/orders/create"),
        crate::auth::Access::Write
    ));
}

#[tokio::test]
async fn should_not_block_unrelated_sessions_while_jwks_fetch_is_pending() {
    // Arrange
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use tokio::sync::oneshot;
    use tokio::time::{sleep, timeout, Duration};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("https://{}", listener.local_addr().unwrap());
    let jwks_url = format!("{issuer}/jwks");
    let secret = b"delayed-secret";
    let (release_response_tx, release_response_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        release_response_rx.await.unwrap();
    });
    let ingress = Arc::new(
        runtime_ingress_with_jwks_auth()
            .with_auth_config(crate::auth::AuthConfig::jwks(
                vec!["fitz-broker".to_string()],
                vec![crate::auth::JwksIssuerConfig {
                    issuer: issuer.clone(),
                    jwks_url,
                }],
            ))
            .with_route_family_map(&[("acme-prod", 1)]),
    );
    ingress
        .on_open(make_session_info(82, TransportKind::Tcp))
        .await
        .unwrap();
    ingress
        .on_open(make_session_info(83, TransportKind::Tcp))
        .await
        .unwrap();
    let mut active_session = make_session_info(84, TransportKind::Tcp);
    active_session.authenticated = true;
    active_session.route_family = crate::runtime::routing::RouteFamily::new(1);
    ingress.on_open(active_session).await.unwrap();
    let jwt = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({
            "iss": issuer,
            "aud": "fitz-broker",
            "sub": "user:82",
            "exp": 9_999_999_999_u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#write"]
        }),
        &EncodingKey::from_secret(secret),
    )
    .unwrap();

    // Act
    let pending_ingress = Arc::clone(&ingress);
    let connect = tokio::spawn(async move {
        pending_ingress
            .on_frame(
                82,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt),
            )
            .await
    });
    sleep(Duration::from_millis(100)).await;
    timeout(
        Duration::from_millis(100),
        ingress.on_close(83, CloseReason::ClientClose),
    )
    .await
    .expect("unrelated close should not wait for JWKS");
    let frame_decision = timeout(
        Duration::from_millis(100),
        ingress.on_frame(
            84,
            ChannelId::Control,
            crate::protocol::tlv::MessageType::new(2),
            Bytes::new(),
        ),
    )
    .await
    .expect("unrelated frame should not wait for JWKS");

    // Assert
    assert!(!connect.is_finished());
    assert!(ingress.get_session(83).is_none());
    assert_eq!(frame_decision, IngressDecision::Accept);
    release_response_tx.send(()).unwrap();
    assert!(matches!(
        connect.await.unwrap(),
        IngressDecision::Close(reason) if reason.contains("connect failed")
    ));
    server.await.unwrap();
}

#[test]
fn should_reject_connect_with_issuer_invalid_signature() {
    // Arrange
    use base64::Engine;
    use jsonwebtoken::{EncodingKey, Header};
    let issuer = "https://idp.example/jwks-invalid";
    let jwks_url = "https://idp.example/jwks-invalid/.well-known/jwks.json";

    let ingress = runtime_ingress_with_jwks_auth().with_auth_config(crate::auth::AuthConfig::jwks(
        vec!["fitz-broker".to_string()],
        vec![crate::auth::JwksIssuerConfig {
            issuer: issuer.to_string(),
            jwks_url: jwks_url.to_string(),
        }],
    ));
    let session = make_session_info(81, TransportKind::Tcp);

    // Create a token signed with a secret NOT in the JWKS cache
    let payload = serde_json::json!({
        "iss": issuer,
        "aud": "fitz-broker",
        "sub": "user:81",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#write"]
    });

    let signing_secret = b"othersecret";
    let header = Header::new(jsonwebtoken::Algorithm::HS256);
    let jwt =
        jsonwebtoken::encode(&header, &payload, &EncodingKey::from_secret(signing_secret)).unwrap();

    // Cache a different secret under the JWKS URL
    let k_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"supersecretkey");
    let jwks =
        serde_json::json!({ "keys": [ { "kty": "oct", "kid": "", "k": k_b64 } ] }).to_string();
    crate::auth::cache_jwks_from_json(jwks_url, &jwks).unwrap();

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session.clone()).await.unwrap();
        let decision = ingress
            .on_frame(
                81,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt.clone()),
            )
            .await;

        // Assert
        assert!(matches!(decision, IngressDecision::Close(_)));
    });
}

#[test]
fn should_create_session_actor_on_open() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth();
    let session = make_session_info(60, TransportKind::Tcp);

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session.clone()).await.unwrap();
    });

    // Assert: Actor should exist but have no permissions
    assert!(ingress.session_actors.contains_key(&60));
    let actor_ref = ingress.session_actors.get(&60).unwrap();
    let actor = actor_ref.value();
    assert!(!actor.authorize(
        &crate::runtime::routing::Route::new("notice://prod/orders/create"),
        crate::auth::Access::Write
    ));
}

#[test]
fn should_update_session_actor_on_connect() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
    let session = make_session_info(61, TransportKind::Tcp);

    let payload = serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:42",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["notice://prod/orders/**#write"]
    });
    let jwt = signed_jwks_jwt(payload);

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session.clone()).await.unwrap();
        let decision = ingress
            .on_frame(
                61,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt.clone()),
            )
            .await;

        // Assert
        assert_eq!(decision, IngressDecision::Accept);
    });

    // Actor should now allow write on the route
    let actor_ref = ingress.session_actors.get(&61).unwrap();
    let actor = actor_ref.value();
    assert!(actor.authorize(
        &crate::runtime::routing::Route::new("notice://prod/orders/create"),
        crate::auth::Access::Write
    ));
}

#[test]
fn should_allow_stream_followup_after_begin_without_global_stream_write_permission() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
    let session = make_session_info(62, TransportKind::Tcp);

    let payload = serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "user:62",
        "exp": 9_999_999_999_u64,
        "tid": "acme-prod",
        "permissions": ["stream://acme/logs/**#write"]
    });
    let jwt = signed_jwks_jwt(payload);

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session.clone()).await.unwrap();

        let connect_decision = ingress
            .on_frame(
                62,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt),
            )
            .await;
        assert_eq!(connect_decision, IngressDecision::Accept);

        let begin_frame = crate::benchkit::build_stream_begin("stream://acme/logs/events/append");
        let (begin_msg_type, begin_payload) =
            crate::benchkit::extract_single_tlv_field(&begin_frame);
        let begin_decision = ingress
            .on_frame(
                62,
                ChannelId::Pub,
                crate::protocol::tlv::MessageType::new(begin_msg_type),
                begin_payload,
            )
            .await;
        assert_eq!(begin_decision, IngressDecision::Accept);

        let append_frame = crate::benchkit::build_stream_append(7, 0, b"event-1");
        let (append_msg_type, append_payload) =
            crate::benchkit::extract_single_tlv_field(&append_frame);
        let append_decision = ingress
            .on_frame(
                62,
                ChannelId::Pub,
                crate::protocol::tlv::MessageType::new(append_msg_type),
                append_payload,
            )
            .await;
        assert_eq!(append_decision, IngressDecision::Accept);

        let commit_frame = crate::benchkit::build_stream_commit(7, 1);
        let (commit_msg_type, commit_payload) =
            crate::benchkit::extract_single_tlv_field(&commit_frame);
        let commit_decision = ingress
            .on_frame(
                62,
                ChannelId::Pub,
                crate::protocol::tlv::MessageType::new(commit_msg_type),
                commit_payload,
            )
            .await;
        assert_eq!(commit_decision, IngressDecision::Accept);
    });

    // Assert
    assert_eq!(ingress.session_count(), 1);
}

struct BackpressureCapturingInbox;

impl MailboxSink for BackpressureCapturingInbox {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_surface_router_backpressure_in_ingress_decision() {
    // Arrange
    let metrics = crate::observability::metrics();
    let backpressure_before = metrics.counter_get(obs::METRIC_ROUTER_BACKPRESSURE);

    let router = Arc::new(crate::runtime::Router::new());
    router.register_domain_pattern("kv", Arc::new(BackpressuredSink));
    // A live session has an inbox; the rejection frame is written to it.
    router.register(
        crate::runtime::routing::RouteAddress::new(
            RouteFamily::new(1),
            crate::runtime::routing::Route::new("inbox://session/90"),
        ),
        Arc::new(BackpressureCapturingInbox) as Arc<dyn MailboxSink>,
    );

    let ingress = RuntimeIngress::new(false).with_router(router);
    let session = make_session_info(90, TransportKind::Tcp);

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        let frame = crate::benchkit::transport::build_kv_begin("kv://test/app/users", 1, 0);
        let payload = Bytes::from(frame[3..].to_vec());

        let decision = ingress
            .on_frame(
                90,
                ChannelId::Pub,
                crate::protocol::tlv::MessageType::new(100),
                payload,
            )
            .await;

        // Assert
        // The command was never enqueued, so the frame is rejected and the
        // session kept; closing would strip the client of the one signal that
        // makes a retry safe.
        assert_eq!(decision, IngressDecision::Accept);
        assert!(
            metrics.counter_get(obs::METRIC_ROUTER_BACKPRESSURE) > backpressure_before,
            "expected router backpressure metric to increase"
        );
    });
}

#[test]
fn should_list_sessions() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Act
    rt.block_on(async {
        for i in 1..=3 {
            let session = make_session_info(i, TransportKind::WebSocket);
            ingress.on_open(session).await.unwrap();
        }
    });

    // Assert
    assert_eq!(ingress.session_count(), 3);
}

#[tokio::test]
async fn should_allow_anonymous_access_when_auth_not_required() {
    // Arrange - Create ingress with auth_required=false
    let ingress = RuntimeIngress::new(false);
    let session = make_session_info(1, TransportKind::WebSocket);
    ingress.on_open(session).await.unwrap();

    // Build a CONNECT frame with empty JWT
    let jwt = "eyJhbGciOiJub25lIn0.e30."; // header.payload.sig (all empty)

    // Act - Send CONNECT frame without valid JWT
    let result = ingress
        .on_frame(
            1,
            ChannelId::Control,
            crate::protocol::tlv::MessageType::CONNECT,
            Bytes::from(jwt),
        )
        .await;

    // Assert - Should accept (anonymous mode)
    assert!(
        matches!(result, IngressDecision::Accept),
        "Expected Accept in anonymous mode, got {result:?}"
    );

    // Verify session has full permissions
    let session_actor = ingress.session_actors.get(&1).unwrap();
    let perms = &session_actor.permissions;
    assert_eq!(ingress.get_session(1).unwrap().route_family.id(), 1);

    // Should have access to all domains
    assert!(perms.allows(&Route::new("kv://test/area/resource"), Access::Write));
    assert!(perms.allows(&Route::new("notice://test/area/resource"), Access::Write));
    assert!(perms.allows(&Route::new("rpc://test/area/resource"), Access::Write));
}

#[test]
fn should_canonicalize_scheme_less_domain_routes_for_authorization() {
    // Arrange
    // Act
    let queue_route = RuntimeIngress::canonicalize_domain_route(
        DispatchDomain::Queue,
        &Route::new("realm/area/tasks/receive"),
    )
    .expect("canonical queue route");
    let notice_route = RuntimeIngress::canonicalize_domain_route(
        DispatchDomain::Notice,
        &Route::new("patterns/*"),
    )
    .expect("canonical notice route");
    let stream_route = RuntimeIngress::canonicalize_domain_route(
        DispatchDomain::Stream,
        &Route::new("realm/area/stream-data/append"),
    )
    .expect("canonical stream route");
    let existing_notice_route = RuntimeIngress::canonicalize_domain_route(
        DispatchDomain::Notice,
        &Route::new("notice://test/notifications/**"),
    )
    .expect("canonical existing notice route");

    // Assert
    assert_eq!(queue_route.as_str(), "queue://realm/area/tasks");
    assert_eq!(notice_route.as_str(), "notice://patterns/*");
    assert_eq!(stream_route.as_str(), "stream://realm/area/stream-data");
    assert_eq!(
        existing_notice_route.as_str(),
        "notice://test/notifications/**"
    );
}

#[test]
fn should_canonicalize_stream_double_star_aliases_to_their_expanded_shape() {
    // routing-design.md §8.1 blesses `stream://**` and `stream://{realm}/**`,
    // and §11.2 requires each to authorize as the same language as its expanded
    // spelling. The generic realm/area/resource canonicalization rejected both
    // outright, so the aliases were unreachable once authorization ran.

    // Arrange
    let cases = [
        ("stream://**", "stream://*/*/*"),
        ("stream://acme/**", "stream://acme/*/*"),
        ("stream://acme/*/*", "stream://acme/*/*"),
        ("stream://acme/events/orders", "stream://acme/events/orders"),
        ("stream://*/events/*", "stream://*/events/*"),
    ];

    // Act
    let canonical = cases.map(|(input, _)| {
        RuntimeIngress::canonicalize_domain_route(DispatchDomain::Stream, &Route::new(input))
            .unwrap_or_else(|error| panic!("expected {input} to canonicalize: {error}"))
    });

    // Assert
    for ((input, expected), actual) in cases.iter().zip(&canonical) {
        assert_eq!(actual.as_str(), *expected, "canonical form of {input}");
    }
}

#[test]
fn should_reject_stream_routes_outside_the_selector_grammar() {
    // Deeper routes previously canonicalized by silently dropping trailing
    // segments, so authorization ran against a different route than the client
    // sent. The shared grammar rejects them instead.

    // Arrange
    let invalid = [
        "stream://acme/events",
        "stream://acme//orders",
        // Noncanonical `**` spellings the domain sink rejects must not be
        // authorized as if they were a blessed selector.
        "stream://acme/**/orders",
        "stream://*/**",
        "stream://**/orders",
        "stream://acme/event*/orders",
    ];

    // Act
    let results = invalid.map(|route| {
        RuntimeIngress::canonicalize_domain_route(DispatchDomain::Stream, &Route::new(route))
    });

    // Assert
    for (route, result) in invalid.iter().zip(&results) {
        assert!(result.is_err(), "expected {route} to be rejected");
    }
}

#[test]
fn should_reject_qualified_route_with_a_different_domain_scheme() {
    // Arrange
    let route = crate::runtime::routing::Route::new("queue://test/area/resource");

    // Act
    let result = RuntimeIngress::canonicalize_domain_route(DispatchDomain::Notice, &route);

    // Assert
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .contains("notice message route must use notice://"));
}

#[test]
fn should_bound_full_connect_failure_diagnostics_per_window() {
    // Arrange
    // Full CONNECT-failure diagnostics are large (a SHA-256, a JSON
    // parse of the attacker-supplied payload, and up to six bounded claim
    // views) and are emitted for entirely unauthenticated peers, so an
    // attacker looping CONNECT must not be able to drive unbounded
    // ERROR-level log volume.
    let budget =
        crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsBudget::default();

    // Act
    let first_window = (0..50).map(|_| budget.acquire(1_000)).collect::<Vec<_>>();
    let next_window = budget.acquire(2_000);

    // Assert
    let full_count = first_window
        .iter()
        .filter(|grant| {
            matches!(
                grant,
                crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsGrant::Full
            )
        })
        .count();
    assert!(
        (1..=5).contains(&full_count),
        "expected between 1 and 5 full diagnostics per window, got {full_count}"
    );
    assert!(
        matches!(
            first_window.last(),
            Some(crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsGrant::Suppressed { .. })
        ),
        "failures past the window budget must be suppressed"
    );
    assert!(
        matches!(
            next_window,
            crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsGrant::Full
        ),
        "a new window must allow full diagnostics again"
    );
    // The suppressed failures are still accounted for, not silently dropped.
    let suppressed = first_window
        .iter()
        .filter(|grant| matches!(grant, crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsGrant::Suppressed { .. }))
        .count();
    assert_eq!(full_count + suppressed, 50);
}

#[test]
fn should_hold_connect_diagnostics_bound_under_concurrent_window_rollover() {
    // Arrange
    // An attacker chooses the concurrency, so the per-window bound
    // must hold when many CONNECT failures land on a window boundary at once
    // - not just when they arrive one at a time.
    use crate::api::runtime_ingress::session_authenticator::{
        ConnectDiagnosticsBudget, ConnectDiagnosticsGrant,
    };

    for round in 0..200_u64 {
        let budget = Arc::new(ConnectDiagnosticsBudget::default());
        let full_grants = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(16));
        // Every thread sees the same stale-window boundary instant.
        let now_millis = 5_000 + round;

        // Act
        let handles = (0..16)
            .map(|_| {
                let budget = budget.clone();
                let full_grants = full_grants.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    if matches!(budget.acquire(now_millis), ConnectDiagnosticsGrant::Full) {
                        full_grants.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        let granted = full_grants.load(Ordering::Relaxed);
        assert!(
            granted <= 5,
            "round {round}: {granted} full diagnostics granted in one window, bound is 5"
        );
    }
}
