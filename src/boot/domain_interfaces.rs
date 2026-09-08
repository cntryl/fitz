//! Narrow operational views over boot-owned domain handles.

use super::domains::{BrokerDomains, DomainHealthSnapshot};

pub(crate) trait DomainHealth: Send + Sync {
    fn health_snapshots(&self) -> Vec<DomainHealthSnapshot>;

    fn has_permanently_failed_domain(&self) -> bool {
        self.health_snapshots()
            .iter()
            .any(|snapshot| snapshot.restart_exhausted)
    }
}

pub(crate) trait DomainMaintenance: Send + Sync {
    fn queue_is_active(&self) -> bool;
    fn queue_sweep_runtime_state(&self);
    fn rpc_is_active(&self) -> bool;
    fn rpc_timeout_sweep_interval(&self) -> std::time::Duration;
    fn rpc_expire_timed_out_requests(&self);
    fn lease_is_active(&self) -> bool;
    fn lease_sweep_expired_state(&self);
    fn schedule_is_active(&self) -> bool;
    fn schedule_scan_due_schedules(&self);
    fn stream_is_active(&self) -> bool;
    fn stream_run_maintenance_slice(&self);
}

impl DomainHealth for BrokerDomains {
    fn health_snapshots(&self) -> Vec<DomainHealthSnapshot> {
        BrokerDomains::health_snapshots(self)
    }
}

impl DomainMaintenance for BrokerDomains {
    fn queue_is_active(&self) -> bool {
        BrokerDomains::queue_is_active(self)
    }
    fn queue_sweep_runtime_state(&self) {
        BrokerDomains::queue_sweep_runtime_state(self);
    }
    fn rpc_is_active(&self) -> bool {
        BrokerDomains::rpc_is_active(self)
    }
    fn rpc_timeout_sweep_interval(&self) -> std::time::Duration {
        BrokerDomains::rpc_timeout_sweep_interval(self)
    }
    fn rpc_expire_timed_out_requests(&self) {
        BrokerDomains::rpc_expire_timed_out_requests(self);
    }
    fn lease_is_active(&self) -> bool {
        BrokerDomains::lease_is_active(self)
    }
    fn lease_sweep_expired_state(&self) {
        BrokerDomains::lease_sweep_expired_state(self);
    }
    fn schedule_is_active(&self) -> bool {
        BrokerDomains::schedule_is_active(self)
    }
    fn schedule_scan_due_schedules(&self) {
        BrokerDomains::schedule_scan_due_schedules(self);
    }
    fn stream_is_active(&self) -> bool {
        BrokerDomains::stream_is_active(self)
    }
    fn stream_run_maintenance_slice(&self) {
        BrokerDomains::stream_run_maintenance_slice(self);
    }
}
