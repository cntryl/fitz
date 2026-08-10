use super::*;

#[test]
fn should_route_domain_frame_before_late_event_handler_plus_preserve_original_payload() {
    #[derive(Clone)]
    struct TrackingMailbox {
        delivered: Arc<std::sync::atomic::AtomicBool>,
        mailbox: Arc<Mailbox>,
    }

    impl MailboxSink for TrackingMailbox {
        fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            self.delivered.store(true, Ordering::SeqCst);
            self.mailbox.deliver(envelope)
        }

        fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            self.delivered.store(true, Ordering::SeqCst);
            self.mailbox.deliver_high_priority(envelope)
        }
    }

    // Arrange
    let router = Arc::new(crate::runtime::Router::new());
    let domain_delivered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let domain_mailbox = Arc::new(Mailbox::new(8));
    let tracking_mailbox = Arc::new(TrackingMailbox {
        delivered: domain_delivered.clone(),
        mailbox: domain_mailbox.clone(),
    });
    router.register_domain_pattern("notice", tracking_mailbox);

    let handler_frames = Arc::new(Mutex::new(Vec::<(bool, Bytes)>::new()));
    let handler_frames_clone = handler_frames.clone();
    let domain_delivered_clone = domain_delivered.clone();
    let ingress = runtime_ingress_with_jwks_auth()
        .with_route_family_map(&[("acme-prod", 1)])
        .with_router(router)
        .with_event_handler(move |event| {
            if let SessionEvent::Frame(frame) = event {
                handler_frames_clone.lock().unwrap().push((
                    domain_delivered_clone.load(Ordering::SeqCst),
                    frame.payload.clone(),
                ));
            }
        });
    let session = make_session_info(613, TransportKind::WebSocket);
    let payload = encode_notice_subscribe("notice://prod/orders/*");

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    let decision = rt.block_on(async {
        ingress.on_open(session).await.unwrap();

        let jwt = signed_jwks_jwt(serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:613",
            "exp": 9_999_999_999_u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#read"]
        }));
        let _ = ingress
            .on_frame(
                613,
                ChannelId::Control,
                MessageType::CONNECT,
                Bytes::from(jwt),
            )
            .await;

        handler_frames.lock().unwrap().clear();
        domain_delivered.store(false, Ordering::SeqCst);

        ingress
            .on_frame(613, ChannelId::Sub, MessageType::new(501), payload.clone())
            .await
    });

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    let envelope = domain_mailbox
        .receiver()
        .try_recv()
        .expect("expected notice domain dispatch");
    let request = envelope
        .payload::<crate::domains::notice::NoticeClientRequest>()
        .expect("expected notice request payload");
    assert_eq!(request.meta.session_id, 613);
    assert_eq!(request.meta.channel, crate::runtime::ClientChannel::Sub);
    assert_eq!(request.meta.message_type, 501);
    assert_eq!(request.meta.route_family, RouteFamily::new(1));

    let frames = handler_frames.lock().unwrap().clone();
    assert_eq!(frames, vec![(true, payload)]);
}
