//! A timeout sweep whose queued redispatch discovers a disconnected worker.

use super::*;
use crate::domains::rpc::protocol::{RpcClientRequest, RpcClientRequestIngress, RpcMessage};

fn deliver_budgeted_request(
    sink: &RpcDomain,
    route: &Route,
    budget_ms: u32,
    received_at: Instant,
) -> uuid::Uuid {
    let family = RouteFamily::new(1);
    let request = crate::domains::rpc::protocol::RpcRequest::new(
        family,
        uuid::Uuid::new_v4(),
        route.clone(),
        Bytes::from_static(b"request"),
    );
    let id = request.correlation_id;
    let mut raw_payload = crate::protocol::rpc_codec::encode_request_into(
        &request,
        &mut crate::protocol::payload_codec::PayloadEncoder::new(),
    );
    raw_payload.extend([1, 1]);
    raw_payload.extend(budget_ms.to_be_bytes());
    let ingress = RpcClientRequestIngress {
        request: RpcClientRequest::new_with_payload(
            crate::runtime::ClientFrameMeta::new(
                1,
                crate::runtime::ClientChannel::Rpc,
                302,
                family,
            ),
            Ok(RpcMessage::Request(request)),
            raw_payload.into(),
        ),
        received_at,
    };
    sink.deliver(Envelope::from_route(
        session_inbox_address(family, 1),
        RouteAddress::new(family, route.clone()),
        ingress,
    ))
    .expect("deliver budgeted request");
    id
}

#[test]
fn should_survive_sweep_when_redispatch_cleans_up_worker_with_collected_cancellation() {
    // Arrange
    // Worker 42 serves a cancellation-aware route and a legacy route. In one
    // sweep the aware call times out (cancellation collected, entry kept), the
    // legacy call times out (credit freed), and the queued legacy call is then
    // redispatched to worker 42, whose inbox is already gone. That failed
    // dispatch cleans up worker 42, removing the cancelled call before the
    // sweep forwards its collected cancellation.
    let router = Arc::new(Router::new());
    let sink = RpcDomain::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let family = RouteFamily::new(1);
    let aware = Route::new("rpc://realm/area/aware/run");
    let legacy = Route::new("rpc://realm/area/legacy/run");
    let caller = Arc::new(CaptureRpcEndpoint::default());
    let worker_inbox = session_inbox_address(family, 42);
    router.register(session_inbox_address(family, 1), caller.clone());
    router.register(
        worker_inbox.clone(),
        Arc::new(CaptureRpcEndpoint::default()),
    );
    sink.register_registration_for_tests(
        test_rpc_worker(family, &aware, 42).with_cancellation_support(true),
    );
    sink.register_registration_for_tests(test_rpc_worker(family, &legacy, 42));
    let received_at = Instant::now();
    deliver_budgeted_request(&sink, &aware, 10, received_at);
    deliver_budgeted_request(&sink, &legacy, 10, received_at);
    deliver_budgeted_request(&sink, &legacy, 60_000, received_at);
    router.unregister(&worker_inbox);

    // Act
    sink.expire_timed_out_requests_at(received_at + Duration::from_secs(1));
    let pending_after_sweep = sink.pending_request_count();
    let health = sink.family_health_snapshot();

    // Assert
    assert_eq!(health.panic_count, 0);
    assert_eq!(health.failed_families, []);
    assert_eq!(pending_after_sweep, 0);
    assert_eq!(caller.frames.lock().len(), 3);
}
