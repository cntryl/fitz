//! Golden contract for admin Schedule run-now HTTP responses.

use super::common::*;
use fitz::testkit::golden::{assert_golden, mask_json_string};
use hyper::header::COOKIE;
use hyper::Method;
use serial_test::serial;
use std::fmt::Write as _;

const RUN_NOW_PATHS: &[&str] = &[
    "all:prod/areas/jobs/resources/billing/operations/send",
    "prod/areas/jobs/resources/billing/operations/send",
    "prod/areas/jobs/resources/billing/operations/missing",
    "prod/areas/jobs/resources/bill%2Fing/operations/send",
    "prod/areas/jobs/resources/%2A/operations/send",
    "prod/areas/jobs/resources/bill%zzing/operations/send",
    "prod/areas/jobs/resources/bill%FFing/operations/send",
    "prod/areas/jobs/resources/%20/operations/send",
];

#[tokio::test]
#[serial]
async fn should_keep_schedule_run_now_responses_stable() {
    // Arrange
    let (runtime, store, schedule) = schedule_runtime_with_domains();
    seed_pending_schedule_claim(store);
    schedule.preload_schedules().expect("preload schedules");
    let cookie = login_cookie(runtime.clone()).await;
    let mut actual = String::new();

    // Act
    for entry in RUN_NOW_PATHS {
        let (family, path) = entry.split_once(':').unwrap_or(("1", entry));
        let req = hyper::http::Request::builder()
            .method(Method::POST)
            .uri(format!("/api/v1/{family}/schedule/realms/{path}/run"))
            .header(COOKIE, cookie.clone())
            .header("host", "localhost")
            .header("origin", "http://localhost")
            .body(Body::default())
            .unwrap();
        let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
            .await
            .unwrap();
        let status = response.status();
        let body = body::to_bytes(response.into_body()).await.unwrap();
        let _ = writeln!(
            actual,
            "{family} {path} => {} {}",
            status.as_u16(),
            mask_json_string(&String::from_utf8_lossy(&body), "triggered_at")
        );
    }

    // Assert
    assert_golden("admin_schedule_run_now", &actual);
}
