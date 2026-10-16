use super::*;
use std::time::{Duration, Instant};

#[tokio::test(start_paused = true)]
async fn should_give_non_rpc_retries_a_fresh_admission_budget_after_transport_queueing() {
    for transport in [TransportKind::Tcp, TransportKind::WebSocket] {
        // Arrange
        let case = domain_ingress_cases()
            .into_iter()
            .find(|case| case.domain == "notice")
            .expect("non-RPC request case");
        let router = Arc::new(crate::runtime::Router::new());
        let sink = Arc::new(TransientBackpressuredSink::new(2));
        router.register_domain_pattern(case.domain, sink.clone());
        let ingress = RuntimeIngress::new(false).with_router(router);
        let session_id = 9_200;
        let received_at = Instant::now().checked_sub(Duration::from_secs(2)).unwrap();
        ingress
            .on_open(make_session_info(session_id, transport))
            .await
            .unwrap();

        // Act
        let decision = ingress
            .on_frame_received_at(
                session_id,
                case.channel_id,
                crate::protocol::tlv::MessageType::new(case.msg_type),
                case.payload,
                None,
                received_at,
            )
            .await;

        // Assert
        assert_eq!(decision, IngressDecision::Accept);
        assert_eq!(sink.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(sink.failures_remaining.load(Ordering::SeqCst), 0);
    }
}
