use serde::Serialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const QUERY_BUDGET: Duration = Duration::from_millis(100);

#[derive(Clone, Serialize)]
pub(super) struct Phase {
    pub observations: u64,
    pub total_ns: u64,
    pub buckets: [u64; 9],
}

#[derive(Clone, Serialize)]
pub(super) struct Snapshot {
    pub query_elapsed_ns: u128,
    pub query_error: Option<String>,
    pub runtime: Option<cntryl_midge::RuntimeMetricsSnapshot>,
    pub ack_phases: BTreeMap<&'static str, Phase>,
}

impl Snapshot {
    pub async fn capture(metrics: cntryl_midge::EngineMetrics) -> Self {
        Self::capture_with_budget(metrics, QUERY_BUDGET).await
    }

    async fn capture_with_budget(metrics: cntryl_midge::EngineMetrics, budget: Duration) -> Self {
        let ack_phases = phase_snapshot();
        let started = Instant::now();
        let result = if budget.is_zero() {
            Err("snapshot deadline is zero".into())
        } else {
            let task = tokio::task::spawn_blocking(move || {
                metrics.get_runtime_metrics_with_timeout(budget)
            });
            match tokio::time::timeout(budget, task).await {
                Ok(Ok(result)) => result.map_err(|error| error.to_string()),
                Ok(Err(error)) => Err(error.to_string()),
                Err(_) => Err("snapshot caller deadline exceeded; pressure unavailable".into()),
            }
        };
        let (runtime, query_error) = match result {
            Ok(runtime) => (Some(runtime), None),
            Err(error) => (None, Some(error)),
        };
        Self {
            query_elapsed_ns: started.elapsed().as_nanos(),
            query_error,
            runtime,
            ack_phases,
        }
    }
}

fn phase_snapshot() -> BTreeMap<&'static str, Phase> {
    let metrics = fitz::observability::metrics();
    [
        (
            "admission",
            "fitz_queue_diagnostic_ack_admission_us",
            "fitz_queue_diagnostic_ack_admission_total_ns",
        ),
        (
            "commit",
            "fitz_queue_diagnostic_ack_commit_us",
            "fitz_queue_diagnostic_ack_commit_total_ns",
        ),
    ]
    .into_iter()
    .map(|(name, histogram, total)| {
        let buckets = metrics.histogram_get_buckets(histogram).unwrap_or([0; 9]);
        (
            name,
            Phase {
                observations: buckets.iter().sum(),
                total_ns: metrics.counter_get(total),
                buckets,
            },
        )
    })
    .collect()
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn should_capture_storage_progress_and_ack_phases_after_a_verified_pair() {
        use super::Snapshot;
        use crate::stress_support::fixture::{BrokerFixture, StorageDirectory, StorageProfile};
        use fitz::testkit::TestClient;
        use std::time::Duration;
        // Arrange
        let directory = StorageDirectory::new(StorageProfile::LocalDisk).unwrap();
        let fixture = BrokerFixture::start(StorageProfile::LocalDisk, directory)
            .await
            .unwrap();
        let metrics = fixture.storage_metrics();
        let mut client = TestClient::new(fixture.tcp_addr()).await.unwrap();
        let id = super::super::io::enqueue(&mut client, 0)
            .await
            .unwrap()
            .unwrap();
        let before = Snapshot::capture_with_budget(metrics.clone(), Duration::from_secs(1)).await;
        // Act
        let delivery = super::super::io::consume(&mut client).await.unwrap();
        let after = Snapshot::capture_with_budget(metrics, Duration::from_secs(1)).await;
        // Assert
        assert!(
            matches!(delivery, super::super::io::Delivery::Acknowledged { id: actual, .. } if actual == id)
        );
        assert!(
            after.runtime.as_ref().unwrap().current_sequence
                > before.runtime.as_ref().unwrap().current_sequence
        );
        for phase in ["admission", "commit"] {
            assert_eq!(
                after.ack_phases[phase].observations - before.ack_phases[phase].observations,
                1
            );
        }
        client.close().await.unwrap();
        let (server, directory) = fixture.into_parts();
        server.shutdown().await.unwrap();
        directory.release().unwrap();
    }

    #[tokio::test]
    async fn should_report_unavailable_pressure_when_the_snapshot_deadline_is_zero() {
        use super::Snapshot;
        use std::time::Duration;
        // Arrange
        let server = fitz::testkit::TestServer::start().await.unwrap();
        // Act
        let snapshot =
            Snapshot::capture_with_budget(server.storage_metrics(), Duration::ZERO).await;
        let json = serde_json::to_value(&snapshot).unwrap();
        // Assert
        assert!(json["runtime"].is_null());
        assert!(json["query_error"]
            .as_str()
            .unwrap()
            .contains("deadline is zero"));
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn should_exclude_enqueue_work_from_ack_phase_observations() {
        use super::Snapshot;
        use fitz::testkit::TestClient;
        use std::time::Duration;
        // Arrange
        let server = fitz::testkit::TestServer::start().await.unwrap();
        let mut client = TestClient::new(server.tcp_addr).await.unwrap();
        let before =
            Snapshot::capture_with_budget(server.storage_metrics(), Duration::from_secs(1)).await;
        // Act
        let id = super::super::io::enqueue(&mut client, 0).await.unwrap();
        let after =
            Snapshot::capture_with_budget(server.storage_metrics(), Duration::from_secs(1)).await;
        // Assert
        assert!(id.is_some());
        for phase in ["admission", "commit"] {
            assert_eq!(
                after.ack_phases[phase].observations,
                before.ack_phases[phase].observations
            );
        }
        client.close().await.unwrap();
        server.shutdown().await.unwrap();
    }
}
