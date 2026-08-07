//! Tokio-owned background loops for synchronous domain maintenance.

use crate::boot::domains::DomainHandles;
use std::sync::{Arc, Weak};
use std::time::Duration;

pub fn start_domain_background_tasks(domains: &Arc<DomainHandles>) {
    start_queue_runtime_sweep(Arc::downgrade(domains));
    start_rpc_timeout_loop(Arc::downgrade(domains));
    start_lease_timeout_loop(Arc::downgrade(domains));
    start_schedule_tick_loop(Arc::downgrade(domains));
    start_stream_maintenance_loop(Arc::downgrade(domains));
}

/// Fail closed when a production actor panics. A domain actor owns
/// synchronous state that cannot be reconstructed transparently, so readiness
/// is withdrawn and the broker enters its normal drain lifecycle.
pub fn start_domain_health_monitor(runtime: crate::boot::Runtime, domains: Arc<DomainHandles>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("Domain health monitor not started: no Tokio runtime available");
        return;
    };
    handle.spawn(async move {
        loop {
            if runtime.is_shutting_down() {
                break;
            }
            if domains.has_permanently_failed_domain() {
                tracing::error!(
                    "A domain actor failed; withdrawing readiness and beginning broker drain"
                );
                runtime.mark_fatal_domain_failure();
                runtime.begin_drain();
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    });
}

fn start_queue_runtime_sweep(domains: Weak<DomainHandles>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("Queue runtime sweep not started: no Tokio runtime available");
        return;
    };
    handle.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(domains) = domains.upgrade() else {
                break;
            };
            if !domains.queue_is_active() {
                break;
            }
            domains.queue_sweep_runtime_state();
        }
    });
}

fn start_rpc_timeout_loop(domains: Weak<DomainHandles>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("RPC timeout loop not started: no Tokio runtime available");
        return;
    };
    handle.spawn(async move {
        loop {
            let sleep_for = {
                let Some(domain_handles) = domains.upgrade() else {
                    break;
                };
                if !domain_handles.rpc_is_active() {
                    break;
                }
                domain_handles.rpc_timeout_sweep_interval()
            };
            tokio::time::sleep(sleep_for).await;
            let Some(domain_handles) = domains.upgrade() else {
                break;
            };
            if !domain_handles.rpc_is_active() {
                break;
            }
            domain_handles.rpc_expire_timed_out_requests();
        }
    });
}

fn start_lease_timeout_loop(domains: Weak<DomainHandles>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("Lease timeout loop not started: no Tokio runtime available");
        return;
    };
    handle.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(domains) = domains.upgrade() else {
                break;
            };
            if !domains.lease_is_active() {
                break;
            }
            domains.lease_sweep_expired_state();
        }
    });
}

fn start_schedule_tick_loop(domains: Weak<DomainHandles>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("Schedule tick loop not started: no Tokio runtime available");
        return;
    };
    handle.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(domains) = domains.upgrade() else {
                break;
            };
            if !domains.schedule_is_active() {
                break;
            }
            domains.schedule_scan_due_schedules();
        }
    });
}

fn start_stream_maintenance_loop(domains: Weak<DomainHandles>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("Stream maintenance loop not started: no Tokio runtime available");
        return;
    };
    handle.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(domains) = domains.upgrade() else {
                break;
            };
            if !domains.stream_is_active() {
                break;
            }
            domains.stream_run_maintenance_slice();
        }
    });
}
