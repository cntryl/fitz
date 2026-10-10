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
