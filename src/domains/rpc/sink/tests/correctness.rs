use super::*;
use crate::runtime::Mailbox;
use bytes::Bytes;

fn new_correctness_rpc_sink(router: Arc<Router>) -> RpcDomainSink {
    RpcDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    )
}

fn receive_rpc_error(mailbox: &Mailbox, label: &str) -> (u16, String) {
    let frame = mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("{label}"))
        .into_payload::<FrameContext>()
        .expect("RPC response frame");
    crate::dispatch::protocol::rpc_codec::decode_error_body(&frame.payload).expect("RPC error body")
}

#[test]
fn should_reject_rpc_request_when_decoded_family_differs_from_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let other_family = RouteFamily::new(2);
    let source = session_inbox_address(family, 7);
    let destination = RouteAddress::new(family, Route::new("rpc://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_rpc_sink(router);
    let correlation_id = uuid::Uuid::new_v4();
    let request = crate::domains::rpc::RpcClientRequest::new(
        crate::runtime::ClientFrameMeta::new(7, crate::runtime::ClientChannel::Rpc, 302, family),
        Ok(crate::domains::rpc::RpcMessage::Request(
            crate::domains::rpc::RpcRequest::new(
                other_family,
                correlation_id,
                Route::new("rpc://acme/system/resource/operation"),
                Bytes::from_static(b"request"),
            ),
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver decoded-family mismatch");
    let (code, message) = receive_rpc_error(&mailbox, "RPC family mismatch response");

    // Assert
    assert_eq!(
        code,
        crate::dispatch::protocol::error_codes::rpc::ERR_BACKEND_ERROR
    );
    assert_eq!(message, "route family mismatch");
    assert_eq!(sink.pending_request_count(), 0);
}

#[test]
fn should_reject_rpc_worker_registration_when_worker_family_differs_from_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let other_family = RouteFamily::new(2);
    let source = session_inbox_address(family, 42);
    let destination = RouteAddress::new(family, Route::new("rpc://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_rpc_sink(router);
    let request = crate::domains::rpc::RpcClientRequest::new(
        crate::runtime::ClientFrameMeta::new(42, crate::runtime::ClientChannel::Rpc, 300, family),
        Ok(crate::domains::rpc::RpcMessage::RegisterWorker {
            worker_addr: RouteAddress::new(
                other_family,
                Route::new("rpc://acme/system/resource/operation"),
            ),
            max_concurrent: 1,
        }),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver mismatched worker registration");
    let (code, message) = receive_rpc_error(&mailbox, "RPC worker family mismatch response");

    // Assert
    assert_eq!(
        code,
        crate::dispatch::protocol::error_codes::rpc::ERR_BACKEND_ERROR
    );
    assert_eq!(message, "route family mismatch");
    assert_eq!(sink.worker_count(), 0);
}
