use super::*;

fn kv_begin_payload(mode: u8, durability: u8, trailing: &[u8]) -> Bytes {
    let frame = crate::benchkit::build_kv_begin("kv://acme/app/users", mode, durability);
    let (_, payload) = crate::benchkit::extract_single_tlv_field(&frame);
    let mut payload = payload.to_vec();
    payload.extend_from_slice(trailing);
    Bytes::from(payload)
}

#[test]
fn should_accept_kv_begin_authorization_exactly_when_the_kv_codec_accepts_it() {
    // Arrange
    let cases = [
        ("read-only buffered", kv_begin_payload(0, 0, &[])),
        ("read-write sync", kv_begin_payload(1, 1, &[])),
        ("trailing byte", kv_begin_payload(1, 0, &[0])),
        ("unknown mode", kv_begin_payload(2, 0, &[])),
        ("unknown durability", kv_begin_payload(0, 2, &[])),
    ];

    for (name, payload) in cases {
        // Act
        let ingress = RuntimeIngress::resolve_authorization_targets(
            DispatchDomain::Kv,
            MessageType::new(100),
            payload.as_ref(),
            auth_spec(100).policy,
        );
        let codec =
            crate::protocol::kv_codec::parse_request(100, RouteFamily::new(1), payload.as_ref());

        // Assert
        assert_eq!(
            ingress.is_ok(),
            codec.is_ok(),
            "{name}: ingress authorization and the KV codec must agree on BEGIN validity"
        );
    }
}

#[tokio::test]
async fn should_close_before_dispatch_given_kv_begin_with_trailing_data() {
    // Arrange
    let session_id = 614;
    let router = Arc::new(crate::runtime::Router::new());
    let domain_mailbox = Arc::new(Mailbox::new(8));
    router.register_domain_pattern("kv", domain_mailbox.clone());
    let ingress = RuntimeIngress::new(false).with_router(router);
    ingress
        .on_open(make_session_info(session_id, TransportKind::Tcp))
        .await
        .unwrap();

    // Act
    let decision = ingress
        .on_frame(
            session_id,
            ChannelId::Pub,
            MessageType::new(100),
            kv_begin_payload(1, 0, &[0]),
            None,
        )
        .await;

    // Assert
    assert!(matches!(
        decision,
        IngressDecision::Close(reason) if reason.contains("authorization parse failed")
    ));
    assert!(domain_mailbox.receiver().try_recv().is_err());
}
