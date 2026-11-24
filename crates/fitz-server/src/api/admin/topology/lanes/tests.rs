use super::kv_lane;
use crate::api::admin::stats;
use crate::api::admin::topology::types::{TopologyConnectionBuilder, TopologyState};

#[test]
fn should_not_mark_kv_topology_pressure_given_no_errors() {
    // Arrange
    let stats = stats::KvStats {
        transactions_active: 0,
        keys_total: 0,
        commits_failed_total: 0,
        invalid_transaction_rejects_total: 0,
        operations_per_second: 0.0,
        diagnostics: crate::api::admin::troubleshooting::DomainDiagnostics {
            snapshot: crate::api::admin::troubleshooting::kv_resource_diagnostics(0),
        },
    };
    let mut connections = TopologyConnectionBuilder::new(8);

    // Act
    let lane = kv_lane(&stats, &[], &mut connections);

    // Assert
    assert!(matches!(lane.state, TopologyState::Quiet));
    assert!(lane
        .counters
        .iter()
        .all(|counter| counter.key != "rollbacks"));
}
