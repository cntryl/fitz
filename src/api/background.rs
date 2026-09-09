//! Tokio-owned background loops for synchronous domain maintenance.

use crate::boot::domain_interfaces::MaintenanceJob;
use crate::boot::domains::BrokerDomains;
use std::sync::Arc;

pub(crate) fn start_domain_background_tasks(domains: &Arc<BrokerDomains>) {
    for job in domains.maintenance_jobs() {
        start_maintenance_job(job);
    }
}

fn start_maintenance_job(job: MaintenanceJob) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!(
            job = job.name(),
            "Domain maintenance not started: no Tokio runtime available"
        );
        return;
    };
    handle.spawn(async move {
        if job.starts_immediately() {
            if !job.is_active() {
                return;
            }
            job.run();
        }
        loop {
            if !job.is_active() {
                break;
            }
            tokio::time::sleep(job.interval()).await;
            if !job.is_active() {
                break;
            }
            job.run();
        }
    });
}
