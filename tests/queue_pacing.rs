#[path = "../benches/stress_support/queue_pressure/pacing.rs"]
#[allow(dead_code)]
mod pacing;

use pacing::Pacer;
use std::time::{Duration, Instant};

#[tokio::test]
async fn should_enforce_minimum_pause_before_a_reserve_permit() {
    // Arrange
    let mut pacer = Pacer::new();
    let acknowledged = Instant::now();
    let minimum = Duration::from_millis(5);
    // Act
    let permitted = pacer.wait_after(acknowledged, minimum).await.unwrap();
    let worker_wake = pacer.last_worker_wake().unwrap();
    pacer.shutdown().await.unwrap();
    // Assert
    assert!(permitted.duration_since(acknowledged) >= minimum);
    assert!(worker_wake.duration_since(acknowledged) >= minimum);
    assert!(permitted >= worker_wake);
}

#[tokio::test]
async fn should_cancel_a_long_pause_and_join_the_worker() {
    // Arrange
    let mut pacer = Pacer::new();
    // Act
    let cancelled = tokio::time::timeout(
        Duration::from_millis(5),
        pacer.wait_after(Instant::now(), Duration::from_secs(60)),
    )
    .await;
    let cleanup = tokio::time::timeout(Duration::from_secs(1), pacer.shutdown()).await;
    // Assert
    assert!(cancelled.is_err());
    cleanup.unwrap().unwrap();
}

#[tokio::test]
async fn should_bound_outstanding_requests_after_cancelled_wait() {
    // Arrange
    let mut pacer = Pacer::new();
    let _ = tokio::time::timeout(
        Duration::from_millis(5),
        pacer.wait_after(Instant::now(), Duration::from_secs(60)),
    )
    .await;
    // Act
    let second = pacer
        .wait_after(Instant::now(), Duration::from_millis(5))
        .await;
    pacer.shutdown().await.unwrap();
    // Assert
    assert!(second.unwrap_err().contains("outstanding"));
}

#[tokio::test]
async fn should_accept_another_request_only_after_the_previous_permit() {
    // Arrange
    let mut pacer = Pacer::new();
    let minimum = Duration::from_millis(5);
    let first_ack = Instant::now();
    pacer.wait_after(first_ack, minimum).await.unwrap();
    let second_ack = Instant::now();
    // Act
    let second_permit = pacer.wait_after(second_ack, minimum).await.unwrap();
    pacer.shutdown().await.unwrap();
    // Assert
    assert!(second_permit.duration_since(second_ack) >= minimum);
}
