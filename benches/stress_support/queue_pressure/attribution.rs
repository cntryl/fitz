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
    pub reserve_phases: BTreeMap<&'static str, Phase>,
}

impl Snapshot {
    pub async fn capture(metrics: cntryl_midge::EngineMetrics) -> Self {
        Self::capture_with_budget(metrics, QUERY_BUDGET).await
    }

    async fn capture_with_budget(metrics: cntryl_midge::EngineMetrics, budget: Duration) -> Self {
        let ack_phases = phase_snapshot(
            "ack",
            &[
                "admission",
                "commit",
                "preparation",
                "dispatch_waiting",
                "reply_delivery",
            ],
        );
        let reserve_phases = phase_snapshot(
            "reserve",
            &["hydration", "dispatch_waiting", "reply_delivery"],
        );
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
            reserve_phases,
        }
    }
}

fn phase_snapshot(operation: &str, phases: &[&'static str]) -> BTreeMap<&'static str, Phase> {
    let metrics = fitz::observability::metrics();
    phases
        .iter()
        .map(|&name| {
            let prefix = format!("fitz_queue_diagnostic_{operation}_{name}");
            let buckets = metrics
                .histogram_get_buckets(&format!("{prefix}_us"))
                .unwrap_or([0; 9]);
            (
                name,
                Phase {
                    observations: buckets.iter().sum(),
                    total_ns: metrics.counter_get(&format!("{prefix}_total_ns")),
                    buckets,
                },
            )
        })
        .collect()
}

#[derive(Clone, Serialize)]
pub(super) struct PhaseDelta {
    pub observations: Option<u64>,
    pub total_ns: Option<u64>,
    pub buckets: [Option<u64>; 9],
}

impl PhaseDelta {
    fn between(before: &Phase, after: &Phase) -> Self {
        Self {
            observations: after.observations.checked_sub(before.observations),
            total_ns: after.total_ns.checked_sub(before.total_ns),
            buckets: std::array::from_fn(|index| {
                after.buckets[index].checked_sub(before.buckets[index])
            }),
        }
    }
}

#[derive(Clone, Serialize)]
pub(super) struct Delta {
    pub ack_phases: BTreeMap<&'static str, PhaseDelta>,
    pub reserve_phases: BTreeMap<&'static str, PhaseDelta>,
    pub runtime_counters: Option<BTreeMap<String, Option<u64>>>,
}

impl Delta {
    pub fn between(before: &Snapshot, after: &Snapshot) -> Self {
        let phases = |before: &BTreeMap<&'static str, Phase>,
                      after: &BTreeMap<&'static str, Phase>| {
            before
                .iter()
                .filter_map(|(&name, first)| {
                    after
                        .get(name)
                        .map(|last| (name, PhaseDelta::between(first, last)))
                })
                .collect()
        };
        Self {
            ack_phases: phases(&before.ack_phases, &after.ack_phases),
            reserve_phases: phases(&before.reserve_phases, &after.reserve_phases),
            runtime_counters: before
                .runtime
                .as_ref()
                .zip(after.runtime.as_ref())
                .and_then(|(first, last)| {
                    let first = serde_json::to_value(first).ok()?;
                    let last = serde_json::to_value(last).ok()?;
                    Some(
                        [
                            "current_sequence",
                            "wal_append_count",
                            "wal_flush_count",
                            "wal_fsync_count",
                            "wal_append_ns_total",
                            "wal_fsync_ns_total",
                            "flush_build_count",
                            "flush_build_ns_total",
                            "flush_publish_count",
                            "flush_publish_ns_total",
                            "compactions_run",
                            "compaction_bytes_rewritten",
                            "write_stalls_total",
                            "write_stall_ns_total",
                            "sst_data_blocks_read_total",
                        ]
                        .into_iter()
                        .map(|name| {
                            let delta = first[name]
                                .as_u64()
                                .zip(last[name].as_u64())
                                .and_then(|(start, end)| end.checked_sub(start));
                            (name.to_owned(), delta)
                        })
                        .collect(),
                    )
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[serial_test::serial(queue_attribution)]
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
        for phase in [
            "admission",
            "commit",
            "preparation",
            "dispatch_waiting",
            "reply_delivery",
        ] {
            assert_eq!(
                after.ack_phases[phase].observations - before.ack_phases[phase].observations,
                1,
                "ACK {phase}"
            );
        }
        for phase in ["hydration", "dispatch_waiting", "reply_delivery"] {
            assert_eq!(
                after.reserve_phases[phase].observations
                    - before.reserve_phases[phase].observations,
                1,
                "RESERVE {phase}"
            );
        }
        let delta = super::Delta::between(&before, &after);
        assert_eq!(delta.ack_phases["admission"].observations, Some(1));
        assert_eq!(delta.reserve_phases["hydration"].observations, Some(1));
        assert!(delta.runtime_counters.as_ref().unwrap()["current_sequence"].unwrap() > 0);
        client.close().await.unwrap();
        let (server, directory) = fixture.into_parts();
        server.shutdown().await.unwrap();
        directory.release().unwrap();
    }

    #[tokio::test]
    #[serial_test::serial(queue_attribution)]
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
    #[serial_test::serial(queue_attribution)]
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
        for phase in [
            "admission",
            "commit",
            "preparation",
            "dispatch_waiting",
            "reply_delivery",
        ] {
            assert_eq!(
                after.ack_phases[phase].observations,
                before.ack_phases[phase].observations
            );
        }
        client.close().await.unwrap();
        server.shutdown().await.unwrap();
    }

    #[test]
    fn should_report_a_counter_reset_as_unavailable_instead_of_zero_cost() {
        // Arrange
        let before = super::Phase {
            observations: 5,
            total_ns: 50,
            buckets: [5; 9],
        };
        let after = super::Phase {
            observations: 1,
            total_ns: 10,
            buckets: [1; 9],
        };
        // Act
        let delta = super::PhaseDelta::between(&before, &after);
        // Assert
        assert_eq!(delta.observations, None);
        assert_eq!(delta.total_ns, None);
        assert_eq!(delta.buckets, [None; 9]);
    }
}
