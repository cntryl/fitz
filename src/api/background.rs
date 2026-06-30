//! Tokio-owned background loops for synchronous domain maintenance.

use crate::boot::domains::{
    DomainHandles, LeaseDomainSink, QueueDomainSink, RpcDomainSink, ScheduleDomainSink,
};
use std::sync::Arc;
use std::time::Duration;

pub fn start_domain_background_tasks(domains: &DomainHandles) {
    start_queue_runtime_sweep(&domains.queue);
    start_rpc_timeout_loop(&domains.rpc);
    start_lease_timeout_loop(&domains.lease);
    start_schedule_tick_loop(&domains.schedule);
}

fn start_queue_runtime_sweep(sink: &Arc<QueueDomainSink>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("Queue runtime sweep not started: no Tokio runtime available");
        return;
    };
    let weak = Arc::downgrade(sink);
    handle.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(sink) = weak.upgrade() else {
                break;
            };
            if !sink.is_active() {
                break;
            }
            sink.sweep_runtime_state();
        }
    });
}

pub fn start_rpc_timeout_loop(sink: &Arc<RpcDomainSink>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("RPC timeout loop not started: no Tokio runtime available");
        return;
    };
    let weak = Arc::downgrade(sink);
    handle.spawn(async move {
        loop {
            let Some(sink) = weak.upgrade() else {
                break;
            };
            if !sink.is_active() {
                break;
            }
            tokio::time::sleep(sink.timeout_sweep_interval()).await;
            let Some(sink) = weak.upgrade() else {
                break;
            };
            if !sink.is_active() {
                break;
            }
            sink.expire_timed_out_requests();
        }
    });
}

fn start_lease_timeout_loop(sink: &Arc<LeaseDomainSink>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("Lease timeout loop not started: no Tokio runtime available");
        return;
    };
    let weak = Arc::downgrade(sink);
    handle.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(sink) = weak.upgrade() else {
                break;
            };
            if !sink.is_active() {
                break;
            }
            sink.sweep_expired_state();
        }
    });
}

fn start_schedule_tick_loop(sink: &Arc<ScheduleDomainSink>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("Schedule tick loop not started: no Tokio runtime available");
        return;
    };
    let weak = Arc::downgrade(sink);
    handle.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(sink) = weak.upgrade() else {
                break;
            };
            if !sink.is_active() {
                break;
            }
            sink.scan_due_schedules();
        }
    });
}
