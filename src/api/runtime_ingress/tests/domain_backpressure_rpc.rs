use super::*;

/// Rejects the first RPC delivery as backpressured and records each attempt's receive stamp.
#[derive(Default)]
struct ReceiveStampingRpcSink {
    received_at: Mutex<Vec<std::time::Instant>>,
}

impl MailboxSink for ReceiveStampingRpcSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let ingress = envelope
            .payload::<crate::domains::rpc::protocol::RpcClientRequestIngress>()
            .expect("RPC request ingress");
        let mut attempts = self.received_at.lock().unwrap();
        attempts.push(ingress.received_at);
        if attempts.len() == 1 {
            return Err(DeliveryError::MailboxFull {
                capacity: 1,
                current_len: 1,
            });
        }
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_keep_rpc_receive_stamp_across_backpressure_retry() {
    // Arrange
    let rt = tokio::runtime::Runtime::new().unwrap();
    let case = domain_ingress_cases()
        .into_iter()
        .find(|case| case.domain == "rpc")
        .expect("RPC request case");
    let router = Arc::new(crate::runtime::Router::new());
    let sink = Arc::new(ReceiveStampingRpcSink::default());
    router.register_domain_pattern("rpc", sink.clone());
    let ingress = RuntimeIngress::new(false).with_router(router);
    let session_id = 9_100;

    // Act
    let decision = rt.block_on(async {
        ingress
            .on_open(make_session_info(session_id, TransportKind::Tcp))
            .await
            .unwrap();
        ingress
            .on_frame(
                session_id,
                case.channel_id,
                crate::protocol::tlv::MessageType::new(case.msg_type),
                case.payload,
                None,
            )
            .await
    });

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    let attempts = sink.received_at.lock().unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[1], attempts[0],
        "a retry must not restart the RPC deadline clock"
    );
}

#[tokio::test(start_paused = true)]
async fn should_preserve_transport_rpc_stamp_across_dispatch_paths() {
    for transport in [TransportKind::Tcp, TransportKind::WebSocket] {
        for shared_payload in [false, true] {
            // Arrange
            let case = domain_ingress_cases()
                .into_iter()
                .find(|case| case.domain == "rpc")
                .expect("RPC request case");
            let router = Arc::new(crate::runtime::Router::new());
            let sink = Arc::new(ReceiveStampingRpcSink::default());
            router.register_domain_pattern("rpc", sink.clone());
            let mut ingress = RuntimeIngress::new(false).with_router(router);
            if shared_payload {
                ingress = ingress.with_event_handler(|_| {});
            }
            let session_id = 9_300;
            let received_at = std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(2))
                .unwrap();
            ingress
                .on_open(make_session_info(session_id, transport))
                .await
                .unwrap();
            if shared_payload {
                // Complete authentication first so the request takes the late
                // event-handler branch that borrows the original payload.
                let connect = ingress
                    .on_frame(
                        session_id,
                        ChannelId::Control,
                        crate::protocol::tlv::MessageType::CONNECT,
                        connect_payload(""),
                        None,
                    )
                    .await;
                assert_eq!(connect, IngressDecision::Accept);
            }

            // Act
            let decision = ingress
                .on_frame_received_at(
                    session_id,
                    case.channel_id,
                    crate::protocol::tlv::MessageType::new(case.msg_type),
                    case.payload,
                    crate::protocol::wire2::header::HeaderLabels::none(),
                    received_at,
                )
                .await;

            // Assert
            assert_eq!(decision, IngressDecision::Accept);
            assert_eq!(*sink.received_at.lock().unwrap(), vec![received_at; 2]);
        }
    }
}
