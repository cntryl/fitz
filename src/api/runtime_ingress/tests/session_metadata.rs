use super::*;

#[test]
fn should_include_client_reported_service_name_in_active_sessions() {
    // Arrange
    let session_id = 57;
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let ingress = runtime_ingress_with_jwks_auth()
        .with_route_families(&[1])
        .with_route_family_map(&[("acme", 1)])
        .with_admin_read_model(admin_read_model.clone());
    let jwt = signed_jwks_jwt(serde_json::json!({
        "iss": "",
        "aud": "fitz-broker",
        "sub": "service-principal-57",
        "exp": 9_999_999_999_u64,
        "tid": "acme",
        "permissions": ["notice://acme/orders/**#read"]
    }));
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Act
    let (connect, service_name_was_omitted_before_report, metadata) = rt.block_on(async {
        ingress
            .on_open(make_session_info(session_id, TransportKind::Tcp))
            .await
            .unwrap();
        let connect = ingress
            .on_frame(
                session_id,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt),
                None,
            )
            .await;
        let service_name_was_omitted_before_report =
            serde_json::to_value(&admin_read_model.sessions()[0])
                .expect("serialize session without a reported name")
                .get("service_name")
                .is_none();
        let metadata = ingress
            .on_frame(
                session_id,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::SESSION_METADATA,
                Bytes::from(crate::protocol::session_metadata::encode_service_name(
                    "orders-worker",
                )),
                None,
            )
            .await;
        (connect, service_name_was_omitted_before_report, metadata)
    });

    // Assert
    assert_eq!(connect, IngressDecision::Accept);
    assert!(service_name_was_omitted_before_report);
    assert_eq!(metadata, IngressDecision::Accept);
    assert_eq!(
        admin_read_model.sessions()[0].service_name.as_deref(),
        Some("orders-worker")
    );
}
