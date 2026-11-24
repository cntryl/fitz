use bytes::Bytes;
use fitz::domains::rpc::RpcRequest;
use fitz::runtime::routing::{Route, RouteFamily};
use uuid::Uuid;

#[test]
fn should_preserve_public_rpc_request_struct_literal() {
    // Arrange
    let request = RpcRequest {
        family_id: RouteFamily::new(7),
        correlation_id: Uuid::nil(),
        route: Route::new("rpc://example/jobs/orders/process"),
        body: Bytes::from_static(b"payload"),
    };

    // Act
    let public_fields = (
        request.family_id,
        request.correlation_id,
        request.route.as_str(),
        request.body.as_ref(),
    );

    // Assert
    assert_eq!(
        public_fields,
        (
            RouteFamily::new(7),
            Uuid::nil(),
            "rpc://example/jobs/orders/process",
            b"payload".as_slice()
        )
    );
}
