#![allow(dead_code)] // Standalone Tier 4 targets use focused subsets of this shared API.

use cntryl_stress::StressContext;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageProfile {
    Memory,
    LocalDisk,
}

impl StorageProfile {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::LocalDisk => "local_disk",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportKind {
    Tcp,
    WebSocket,
}

impl TransportKind {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::WebSocket => "websocket",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LayerKind {
    Direct,
    Encoded,
    Tcp,
    WebSocket,
    TcpMultiClient,
    WebSocketMultiClient,
}

impl LayerKind {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Encoded => "encoded",
            Self::Tcp => "tcp",
            Self::WebSocket => "websocket",
            Self::TcpMultiClient | Self::WebSocketMultiClient => "multiclient",
        }
    }

    pub(crate) const fn transport(self) -> &'static str {
        match self {
            Self::Direct | Self::Encoded => "in_process",
            Self::Tcp | Self::TcpMultiClient => "tcp",
            Self::WebSocket | Self::WebSocketMultiClient => "websocket",
        }
    }
}

impl From<TransportKind> for LayerKind {
    fn from(value: TransportKind) -> Self {
        match value {
            TransportKind::Tcp => Self::Tcp,
            TransportKind::WebSocket => Self::WebSocket,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Tier4Dimensions<'a> {
    pub(crate) domain: &'a str,
    pub(crate) scenario: &'a str,
    pub(crate) storage_profile: StorageProfile,
    pub(crate) layer: LayerKind,
    pub(crate) write_mode: &'a str,
    pub(crate) payload_size: usize,
    pub(crate) history_depth: usize,
    pub(crate) read_limit: usize,
    pub(crate) read_scope: &'a str,
    pub(crate) route_count: usize,
    pub(crate) filter_selectivity: &'a str,
    pub(crate) client_count: usize,
    pub(crate) workload_mix: &'a str,
    pub(crate) completed_unit: &'a str,
    pub(crate) gate_class: &'a str,
}

pub(crate) fn tag_dimensions(ctx: &mut StressContext, dimensions: &Tier4Dimensions<'_>) {
    ctx.parameter("domain", dimensions.domain);
    ctx.parameter("scenario", dimensions.scenario);
    ctx.parameter("storage_profile", dimensions.storage_profile.label());
    ctx.parameter("layer", dimensions.layer.label());
    ctx.parameter("transport", dimensions.layer.transport());
    ctx.parameter("write_mode", dimensions.write_mode);
    ctx.parameter("payload_size", dimensions.payload_size);
    ctx.parameter("history_depth", dimensions.history_depth);
    ctx.parameter("read_limit", dimensions.read_limit);
    ctx.parameter("read_scope", dimensions.read_scope);
    ctx.parameter("route_count", dimensions.route_count);
    ctx.parameter("filter_selectivity", dimensions.filter_selectivity);
    ctx.parameter("client_count", dimensions.client_count);
    ctx.parameter("workload_mix", dimensions.workload_mix);
    ctx.parameter("completed_unit", dimensions.completed_unit);
    ctx.parameter("logical_unit", dimensions.completed_unit);
    ctx.parameter(
        "measurement_scope",
        format!(
            "{}_{}_{}_{}_{}",
            dimensions.domain,
            dimensions.storage_profile.label(),
            dimensions.layer.transport(),
            dimensions.layer.label(),
            dimensions.scenario
        ),
    );
    ctx.metadata("primary_metric", "throughput");
    ctx.metadata("target_class", dimensions.gate_class);
}

/// Run a fixed-duration Tier 4 sample and emit paired throughput and latency records.
///
/// The workload must add exactly one latency observation per completed logical operation.
/// This keeps correctness counters, normalization, and p50/p95/p99 observations on the
/// same unit across every domain.
pub(crate) fn measure_operations(
    ctx: &mut StressContext,
    measurement: &'static str,
    operations_per_iteration: u64,
    mut run_iteration: impl FnMut(&mut Vec<Duration>),
) -> u64 {
    assert!(
        operations_per_iteration > 0,
        "Tier 4 iterations must contain at least one logical operation"
    );

    let mut latencies = Vec::new();
    ctx.parameter("logical_operations_per_iteration", operations_per_iteration);
    let measured_started = Instant::now();
    let completed = ctx.measure_batch(measurement, operations_per_iteration, || {
        let before = latencies.len();
        run_iteration(&mut latencies);
        assert_eq!(
            latencies.len() - before,
            usize::try_from(operations_per_iteration)
                .expect("operations per iteration should fit usize"),
            "each completed logical operation needs one latency observation"
        );
    });
    let measured_duration = measured_started.elapsed();
    let observations =
        u64::try_from(latencies.len()).expect("latency observation count should fit u64");
    assert_eq!(
        completed, observations,
        "completed logical operations must equal latency observations"
    );

    let latency_measurement = format!("{measurement}_latency");
    ctx.metadata("paired_latency_record", &latency_measurement);
    ctx.metadata("measurement_kind", "throughput_and_ns_per_operation");
    ctx.record_external(&latency_measurement, measured_duration, completed);
    for latency in latencies {
        ctx.record_latency(latency);
    }
    ctx.metadata("paired_throughput_record", measurement);
    ctx.metadata("measurement_kind", "latency_distribution");
    ctx.metadata("latency_quantiles", "p50,p95,p99");
    ctx.metadata("primary_metric", "latency");
    ctx.metadata("target_class", "latency_report");

    completed
}

/// Run a diagnostic operation for every sample iteration without aborting the
/// enclosing benchmark when an integration transport fails.
pub(crate) fn measure_operations_best_effort(
    ctx: &mut StressContext,
    measurement: &'static str,
    mut run_iteration: impl FnMut() -> Result<Duration, String>,
) {
    let mut latencies = Vec::new();
    let mut failures = 0_u64;
    let mut first_failure = None;
    let attempted = ctx.measure_batch(measurement, 1, || match run_iteration() {
        Ok(latency) => latencies.push(latency),
        Err(error) => {
            failures = failures.saturating_add(1);
            if first_failure.is_none() {
                first_failure = Some(error);
            }
        }
    });

    let completed = u64::try_from(latencies.len()).unwrap_or(u64::MAX);
    let _ = ctx.correctness().attempted(completed).completed(completed);
    ctx.metadata("attempted_operations", attempted);

    ctx.metadata("measurement_kind", "best_effort_diagnostic");
    if failures == 0 {
        ctx.metadata("measurement_status", "complete");
    } else {
        ctx.metadata("measurement_status", "degraded");
        ctx.metadata("measurement_failures", failures);
        ctx.metadata(
            "first_measurement_failure",
            first_failure.as_deref().unwrap_or("unknown"),
        );
    }
    for latency in latencies {
        ctx.record_latency(latency);
    }
}
