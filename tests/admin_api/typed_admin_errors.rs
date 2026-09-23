use super::common::*;

#[tokio::test]
#[serial]
async fn should_reject_out_of_prefix_kv_cursor_as_bad_request() {
    // Arrange
    let (runtime, _store) = queue_runtime_with_domains();
    let cookie = login_cookie(runtime.clone()).await;
    let request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/kv/realms/prod/areas/app/resources/users/rows?starts_with=user%3A&cursor=b3JkZXI6MQ==")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(request, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial]
async fn should_report_unavailable_kv_admin_domain_as_service_unavailable() {
    // Arrange
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;
    let request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/kv/realms/prod/areas/app/resources/users/rows?starts_with=user%3A")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(request, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}
