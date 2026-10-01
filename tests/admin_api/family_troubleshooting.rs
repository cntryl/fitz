use super::common::*;

fn dead_lettered_queue(family: u64) -> QueueInfo {
    QueueInfo {
        family,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        subscriptions_active: 0,
        messages_ready: 1,
        messages_delayed: 2,
        messages_inflight: 3,
        messages_dead_lettered: 100,
        messages_total: 106,
        oldest_message_age_seconds: 9,
        oldest_backlog_age_seconds: 600,
        backlog_age_buckets: QueueAgeBuckets::default(),
        delay_age_buckets: QueueAgeBuckets::default(),
        enqueue_success_total: 0,
        complete_success_total: 0,
        in_rate_per_second: 0.0,
        out_rate_per_second: 0.0,
        status: "backlogged".to_string(),
    }
}

async fn family_troubleshooting(runtime: Arc<Runtime>, family: u64) -> serde_json::Value {
    let cookie = login_cookie(runtime.clone()).await;
    let response = fitz::api::admin::handlers::handle_request(
        hyper::http::Request::builder()
            .method(Method::GET)
            .uri(format!("/api/v1/{family}/troubleshooting"))
            .header(COOKIE, cookie)
            .body(Body::default())
            .unwrap(),
        runtime,
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&body::to_bytes(response.into_body()).await.unwrap()).unwrap()
}

#[tokio::test]
#[serial]
async fn should_report_family_attributable_hotspot_in_family_troubleshooting() {
    // Arrange
    let runtime = test_runtime();
    runtime
        .admin_read_model()
        .replace_queues(vec![dead_lettered_queue(1)]);

    // Act
    let payload = family_troubleshooting(runtime, 1).await;

    // Assert
    assert_eq!(payload["incident_summary"]["status"], "stalled");
    assert_eq!(payload["top_bottleneck"]["domain"], "queue");
    assert_eq!(payload["top_bottleneck"]["family"], 1);
}

#[tokio::test]
#[serial]
async fn should_exclude_other_family_hotspots_from_family_troubleshooting() {
    // Arrange
    let runtime = test_runtime();
    runtime
        .admin_read_model()
        .replace_queues(vec![dead_lettered_queue(1)]);

    // Act
    let payload = family_troubleshooting(runtime, 2).await;

    // Assert
    assert_eq!(payload["incident_summary"]["status"], "healthy");
    assert!(payload["top_bottleneck"].is_null());
    assert!(payload["hotspots"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[serial]
async fn should_not_claim_full_confidence_when_broker_wide_signals_are_excluded() {
    // Arrange
    let runtime = test_runtime();

    // Act
    let payload = family_troubleshooting(runtime, 1).await;

    // Assert
    let confidence = payload["incident_summary"]["confidence"].as_f64().unwrap();
    assert!(confidence < 1.0, "family confidence was {confidence}");
}
