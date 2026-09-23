use super::*;

#[test]
fn should_classify_failed_store_transaction_as_kv_rows_backend_error() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let sink = KvDomain::new(
        store,
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let request = AdminKvRowsRequest {
        route_family: RouteFamily::new(2),
        realm: "prod",
        area: "app",
        resource: "users",
        starts_with: b"user:",
        cursor: None,
        limit: 1,
    };

    // Act
    let result = sink.admin_scan_committed_rows(&request);

    // Assert
    assert!(matches!(result, Err(AdminKvRowsError::Backend(_))));
}
