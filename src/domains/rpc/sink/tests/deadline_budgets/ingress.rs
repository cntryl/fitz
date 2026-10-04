use super::*;
use crate::domains::rpc::protocol::{RpcClientRequest, RpcClientRequestIngress, RpcMessage};

struct IngressHarness {
    sink: RpcDomain,
    caller: Arc<parking_lot::Mutex<Vec<FrameContext>>>,
    worker: Arc<parking_lot::Mutex<Vec<FrameContext>>>,
}

impl IngressHarness {
    fn new() -> Self {
        let router = Arc::new(Router::new());
        let sink = RpcDomain::new(
            router.clone(),
            crate::control::admin::read_model::AdminReadModel::new(),
        );
        let family = RouteFamily::new(1);
        let route = request(uuid::Uuid::new_v4()).route;
        let caller = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let worker = Arc::new(parking_lot::Mutex::new(Vec::new()));
        router.register(
            session_inbox_address(family, 7),
            Arc::new(CaptureRpcFrameSink {
                frames: caller.clone(),
            }),
        );
        router.register(
            session_inbox_address(family, 42),
            Arc::new(CaptureRpcFrameSink {
                frames: worker.clone(),
            }),
        );
        sink.register_registration_for_tests(
            test_rpc_worker(family, &route, 42).with_cancellation_support(true),
        );
        Self {
            sink,
            caller,
            worker,
        }
    }

    fn deliver(&self, budget_ms: u32, received_at: Instant) -> uuid::Uuid {
        let request = request(uuid::Uuid::new_v4());
        let id = request.correlation_id;
        let family = request.family_id;
        let destination = RouteAddress::new(family, request.route.clone());
        let mut raw_payload = crate::protocol::rpc_codec::encode_request_into(
            &request,
            &mut crate::protocol::payload_codec::PayloadEncoder::new(),
        );
        raw_payload.extend([1, 1]);
        raw_payload.extend(budget_ms.to_be_bytes());
        let ingress = RpcClientRequestIngress {
            request: RpcClientRequest::new_with_payload(
                crate::runtime::ClientFrameMeta::new(
                    7,
                    crate::runtime::ClientChannel::Rpc,
                    302,
                    family,
                ),
                Ok(RpcMessage::Request(request)),
                raw_payload.into(),
            ),
            received_at,
        };
        self.sink
            .deliver(Envelope::from_route(
                session_inbox_address(family, 7),
                destination,
                ingress,
            ))
            .expect("deliver stamped request");
        id
    }
}

#[test]
fn should_reject_budget_expired_in_actor_mailbox_before_claiming_capacity() {
    // Arrange
    let h = IngressHarness::new();
    let received_at = Instant::now()
        .checked_sub(Duration::from_secs(2))
        .expect("model elapsed ingress time");

    // Act
    let id = h.deliver(1_000, received_at);

    // Assert
    assert!(h.worker.lock().is_empty());
    assert_eq!(
        h.sink.config.global_pending_count.load(Ordering::Acquire),
        0
    );
    let caller = h.caller.lock();
    assert_eq!(caller.len(), 1);
    assert_rpc_terminal_code_error(
        &caller[0],
        id,
        crate::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
        RPC_BUDGET_EXPIRED_ERROR,
    );
}

#[test]
fn should_deduct_actor_mailbox_time_from_forwarded_worker_budget() {
    // Arrange
    let h = IngressHarness::new();
    let received_at = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .expect("model elapsed ingress time");

    // Act
    h.deliver(3_000, received_at);

    // Assert
    let worker = h.worker.lock();
    assert_eq!(worker.len(), 1);
    let budget =
        crate::protocol::rpc_codec::extract_request_remaining_budget_ms(&worker[0].payload)
            .expect("valid forwarded budget")
            .expect("supporting worker budget");
    assert!(
        budget <= 2_000,
        "actor queue time cannot renew the original budget"
    );
    assert!(h.caller.lock().is_empty());
}
