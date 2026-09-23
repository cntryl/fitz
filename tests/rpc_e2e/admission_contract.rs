use super::common::*;
use fitz::runtime::{DeliveryError, Envelope, MailboxSink};

#[derive(Clone, Copy)]
enum AdmissionFailure {
    Unauthorized,
    Backpressure,
    Timeout,
    Unavailable,
    DomainRejection,
}

impl AdmissionFailure {
    fn expected(self) -> (u16, &'static str) {
        match self {
            Self::Unauthorized => (
                fitz::protocol::error_codes::rpc::ERR_UNAUTHORIZED,
                "unauthorized: permission denied",
            ),
            Self::Backpressure => (
                fitz::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
                "domain at capacity: request was not accepted, retry with backoff",
            ),
            Self::Timeout => (
                fitz::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
                "domain timeout: request outcome unknown, do not blindly retry",
            ),
            Self::Unavailable => (
                fitz::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
                "domain unavailable: request could not be completed",
            ),
            Self::DomainRejection => (
                fitz::protocol::error_codes::rpc::ERR_ROUTE_NOT_REGISTERED,
                "No workers registered for route",
            ),
        }
    }

    fn delivery_error(self) -> Option<DeliveryError> {
        match self {
            Self::Backpressure => Some(DeliveryError::MailboxFull {
                capacity: 1,
                current_len: 1,
            }),
            Self::Timeout => Some(DeliveryError::Timeout),
            Self::Unavailable => Some(DeliveryError::ActorStopped),
            Self::Unauthorized | Self::DomainRejection => None,
        }
    }
}

struct FailingRpcSink(DeliveryError);

impl MailboxSink for FailingRpcSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Err(self.0.clone())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

async fn exercise_failure<C: RpcConnector + FrameReceivingConnector>(failure: AdmissionFailure) {
    let server = if matches!(failure, AdmissionFailure::Unauthorized) {
        TestServer::start_with_auth(true)
            .await
            .expect("start auth server")
    } else {
        TestServer::start().await.expect("start server")
    };
    if let Some(error) = failure.delivery_error() {
        server
            .runtime
            .router()
            .register_domain_pattern("rpc", std::sync::Arc::new(FailingRpcSink(error)));
    }
    let mut caller = C::connect(&server).await.expect("connect caller");
    if matches!(failure, AdmissionFailure::Unauthorized) {
        let connect = fitz::testkit::transport::build_connect_frame(
            "test-realm",
            &fitz::testkit::transport::generate_test_jwt("test-realm"),
        );
        caller
            .send_frame(&connect)
            .await
            .expect("authenticate caller");
        server
            .wait_for_authenticated_sessions(1)
            .await
            .expect("auth readiness");
    }
    let route = if matches!(failure, AdmissionFailure::Unauthorized) {
        "rpc://denied/tasks/worker"
    } else {
        "rpc://test/tasks/worker"
    };
    let uncorrelated_request = build_rpc_request(route, "run", b"body");
    let (_, payload) = TlvFrameParser::new(&uncorrelated_request)
        .next_field()
        .expect("RPC request field");
    let correlation_id =
        fitz::protocol::rpc_codec::extract_request_correlation_id(&payload).expect("request UUID");
    let mut request_builder = TlvFrameBuilder::new();
    request_builder.encode_field(2, &42_u64.to_be_bytes());
    request_builder.encode_field(302, &payload);
    let request = request_builder.build();

    caller.send_frame(&request).await.expect("send RPC request");
    let terminal = caller
        .recv_frame(2_000)
        .await
        .expect("terminal RPC response");

    let mut response_parser = TlvFrameParser::new(&terminal);
    assert_eq!(response_parser.next_field().expect("response field").0, 303);
    assert!(
        response_parser.next_field().is_none(),
        "RPC terminal reply must not echo optional frame correlation"
    );
    let response = parse_rpc_response_delivery(&terminal).expect("decode terminal 303 by UUID");
    assert_eq!(response.msg_type, 303);
    assert_eq!(response.correlation_id, correlation_id);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);
    let (code, message) = fitz::protocol::rpc_codec::decode_error_body(&response.body)
        .expect("decode RPC terminal error body");
    assert_eq!((code, message.as_str()), failure.expected());
    assert!(
        caller.recv_frame(50).await.is_err(),
        "a REQUEST failure must have exactly one terminal frame and no success ACK"
    );
    server.shutdown().await.expect("stop server");
}

macro_rules! rpc_failure_transport_tests {
    ($tcp:ident, $ws:ident, $failure:expr) => {
        #[tokio::test]
        #[serial]
        async fn $tcp() {
            // Arrange
            let failure = $failure;
            // Act
            exercise_failure::<TcpRpcConnector>(failure).await;
            // Assert
            // The shared exercise checks the complete terminal contract.
        }

        #[tokio::test]
        #[serial]
        async fn $ws() {
            // Arrange
            let failure = $failure;
            // Act
            exercise_failure::<WsRpcConnector>(failure).await;
            // Assert
            // The shared exercise checks the complete terminal contract.
        }
    };
}

rpc_failure_transport_tests!(
    should_decode_unauthorized_rpc_failure_tcp,
    should_decode_unauthorized_rpc_failure_ws,
    AdmissionFailure::Unauthorized
);
rpc_failure_transport_tests!(
    should_decode_pre_enqueue_rpc_backpressure_tcp,
    should_decode_pre_enqueue_rpc_backpressure_ws,
    AdmissionFailure::Backpressure
);
rpc_failure_transport_tests!(
    should_decode_enqueued_rpc_timeout_tcp,
    should_decode_enqueued_rpc_timeout_ws,
    AdmissionFailure::Timeout
);
rpc_failure_transport_tests!(
    should_decode_unavailable_rpc_domain_tcp,
    should_decode_unavailable_rpc_domain_ws,
    AdmissionFailure::Unavailable
);
rpc_failure_transport_tests!(
    should_decode_domain_rpc_rejection_tcp,
    should_decode_domain_rpc_rejection_ws,
    AdmissionFailure::DomainRejection
);

async fn exercise_concurrent_failures<C: RpcConnector + FrameReceivingConnector>() {
    let server = TestServer::start().await.expect("start server");
    server.runtime.router().register_domain_pattern(
        "rpc",
        std::sync::Arc::new(FailingRpcSink(DeliveryError::MailboxFull {
            capacity: 1,
            current_len: 1,
        })),
    );
    let mut caller = C::connect(&server).await.expect("connect caller");
    let mut pending = std::collections::HashSet::new();
    for _ in 0..3 {
        let request = build_rpc_request("rpc://test/tasks/worker", "run", b"body");
        let (_, payload) = TlvFrameParser::new(&request)
            .next_field()
            .expect("RPC request field");
        let request_id = fitz::protocol::rpc_codec::extract_request_correlation_id(&payload)
            .expect("request UUID");
        assert!(pending.insert(request_id));
        caller.send_frame(&request).await.expect("send RPC request");
    }

    for _ in 0..3 {
        let terminal = caller.recv_frame(2_000).await.expect("terminal response");
        let response = parse_rpc_response_delivery(&terminal).expect("decode terminal 303");
        assert!(pending.remove(&response.correlation_id));
        assert_eq!(response.seq, 0);
        assert!(response.stream_end);
        let (code, _) = fitz::protocol::rpc_codec::decode_error_body(&response.body)
            .expect("decode error body");
        assert_eq!(code, fitz::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE);
    }
    assert!(pending.is_empty());
    assert!(
        caller.recv_frame(50).await.is_err(),
        "no extra ACK or reply"
    );
    server.shutdown().await.expect("stop server");
}

#[tokio::test]
#[serial]
async fn should_demultiplex_concurrent_rpc_rejections_by_uuid_tcp() {
    // Arrange
    // Act
    exercise_concurrent_failures::<TcpRpcConnector>().await;
    // Assert
    // The shared exercise checks the UUID set and exact terminal count.
}

#[tokio::test]
#[serial]
async fn should_demultiplex_concurrent_rpc_rejections_by_uuid_ws() {
    // Arrange
    // Act
    exercise_concurrent_failures::<WsRpcConnector>().await;
    // Assert
    // The shared exercise checks the UUID set and exact terminal count.
}
