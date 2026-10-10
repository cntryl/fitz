//! Prometheus metrics endpoint.

mod broker;
mod collector;
mod domains;
mod rendering;

use crate::api::http::{Body, Response};
use crate::boot::Runtime;
use hyper::StatusCode;
use num_traits::ToPrimitive;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Handle /metrics endpoint (Prometheus format)
pub fn handle_metrics(runtime: &Runtime) -> Response {
    let metrics = generate_prometheus_metrics(runtime);

    hyper::http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(Body::from(metrics))
        .unwrap()
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StructuredMetricsResponse {
    pub(crate) scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) family: Option<u64>,
    pub(crate) generated_at: u64,
    pub(crate) samples: Vec<StructuredMetricSample>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StructuredMetricSample {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) help: String,
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) value: f64,
}

/// Handle the authenticated structured metrics contract.
pub(crate) fn handle_structured_metrics(runtime: &Runtime, family: Option<u64>) -> Response {
    runtime.refresh_admin_snapshots();
    let mut samples = structured_samples(&generate_prometheus_metrics(runtime), family);
    if let Some(family) = family {
        samples.extend(family_attributable_samples(runtime, family));
        sort_samples(&mut samples);
    }
    super::json_response(StructuredMetricsResponse {
        scope: if family.is_some() { "family" } else { "all" },
        family,
        generated_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
        samples,
    })
}

/// Build the MCP metrics response with an explicit sample cap and truncation marker.
pub(crate) fn mcp_structured_metrics_value(
    runtime: &Runtime,
    family: Option<u64>,
    max_samples: usize,
) -> serde_json::Value {
    let snapshot = runtime.admin_read_model().bounded_snapshot(family, 128);
    let mut samples = projection_samples(&snapshot, family);
    sort_samples(&mut samples);
    let truncated = snapshot.truncated || samples.len() > max_samples;
    samples.truncate(max_samples);
    serde_json::json!({
        "scope": if family.is_some() { "family" } else { "all" },
        "route_family": family,
        "generated_at": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis().try_into().unwrap_or(u64::MAX),
        "samples": samples,
        "truncated": truncated,
        "limit": max_samples,
        "_meta": {
            "partial": truncated,
            "source": "shared admin projections",
            "collection_limit": snapshot.limit_per_collection,
            "unavailable": ["unattributed latency histories and domain counters"],
            "aggregation": if snapshot.truncated { "lower bounds from bounded observations" } else { "complete projection snapshot" },
        },
    })
}

/// Generate Prometheus-format metrics
fn generate_prometheus_metrics(runtime: &Runtime) -> String {
    let mut output = String::new();

    broker::append_broker_metrics(&mut output, runtime);
    collector::append_observability_metrics(&mut output);
    domains::append_domain_metrics(&mut output, runtime);
    crate::api::mcp::telemetry::append_prometheus_metrics(&mut output);

    output
}

fn structured_samples(metrics: &str, family: Option<u64>) -> Vec<StructuredMetricSample> {
    let mut help_by_name = BTreeMap::new();
    let mut kind_by_name = BTreeMap::new();
    let mut samples = Vec::new();

    for line in metrics.lines() {
        if let Some(metadata) = line.strip_prefix("# HELP ") {
            if let Some((name, help)) = metadata.split_once(' ') {
                help_by_name.insert(name.to_string(), help.to_string());
            }
            continue;
        }
        if let Some(metadata) = line.strip_prefix("# TYPE ") {
            if let Some((name, kind)) = metadata.split_once(' ') {
                kind_by_name.insert(name.to_string(), kind.to_string());
            }
            continue;
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((sample_head, raw_value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = raw_value.parse::<f64>() else {
            continue;
        };
        let (name, labels) = parse_sample_head(sample_head);
        if family.is_some_and(|requested| {
            labels
                .get("family")
                .and_then(|value| value.parse::<u64>().ok())
                != Some(requested)
        }) {
            continue;
        }
        let kind = kind_by_name
            .get(&name)
            .cloned()
            .unwrap_or_else(|| "gauge".to_string());
        let help = help_by_name
            .get(&name)
            .cloned()
            .unwrap_or_else(|| "Fitz metric".to_string());
        samples.push(StructuredMetricSample {
            name,
            kind,
            help,
            labels,
            value,
        });
    }

    sort_samples(&mut samples);
    samples
}

#[allow(clippy::too_many_lines)]
fn family_attributable_samples(runtime: &Runtime, family: u64) -> Vec<StructuredMetricSample> {
    let snapshot = runtime
        .admin_read_model()
        .bounded_snapshot(Some(family), usize::MAX);
    projection_samples(&snapshot, Some(family))
}

#[allow(clippy::too_many_lines)]
fn projection_samples(
    snapshot: &crate::control::admin::read_model::AdminSnapshot,
    family: Option<u64>,
) -> Vec<StructuredMetricSample> {
    let sample = |name: &str, kind: &str, help: &str, value: f64| StructuredMetricSample {
        name: name.to_string(),
        kind: kind.to_string(),
        help: help.to_string(),
        labels: family.map_or_else(BTreeMap::new, |family| {
            BTreeMap::from([(String::from("family"), family.to_string())])
        }),
        value,
    };
    let count = |value: usize| value.to_f64().unwrap_or(f64::MAX);
    let kv_transactions = snapshot.kv_transactions.len();
    let streams = &snapshot.streams;
    let notice_subscriptions = snapshot.notice_subscriptions.len();
    let notice_routes = &snapshot.notice_routes;
    let queues = &snapshot.queues;
    let rpc_workers = snapshot.rpc_workers.len();
    let rpc_pending = snapshot.rpc_pending.len();
    let leases = snapshot.leases.len();
    let schedules = snapshot.schedules.len();
    let sessions = snapshot.sessions.len();

    vec![
        sample(
            "fitz_sessions_total",
            "gauge",
            "Active sessions attributable to this route family",
            count(sessions),
        ),
        sample(
            "fitz_kv_transactions_active",
            "gauge",
            "Active KV transactions attributable to this route family",
            count(kv_transactions),
        ),
        sample(
            "fitz_stream_active",
            "gauge",
            "Active streams attributable to this route family",
            count(
                streams
                    .iter()
                    .filter(|stream| stream.committed_event_count > 0)
                    .count(),
            ),
        ),
        sample(
            "fitz_stream_append_sessions_active",
            "gauge",
            "Active stream append sessions attributable to this route family",
            metric_sum(streams.iter().map(|item| item.sessions_active)),
        ),
        sample(
            "fitz_notice_subscriptions_active",
            "gauge",
            "Active Notice subscriptions attributable to this route family",
            count(notice_subscriptions),
        ),
        sample(
            "fitz_notice_routes_active",
            "gauge",
            "Active Notice routes attributable to this route family",
            count(notice_routes.len()),
        ),
        sample(
            "fitz_notice_publishes_total",
            "counter",
            "Notice publishes attributable to this route family",
            notice_routes
                .iter()
                .map(|item| item.publishes_total)
                .sum::<u64>()
                .to_f64()
                .unwrap_or(f64::MAX),
        ),
        sample(
            "fitz_queue_messages_pending",
            "gauge",
            "Pending queue messages attributable to this route family",
            metric_sum(
                queues
                    .iter()
                    .map(|item| item.messages_ready + item.messages_delayed),
            ),
        ),
        sample(
            "fitz_queue_inflight_active",
            "gauge",
            "Active queue deliveries attributable to this route family",
            metric_sum(queues.iter().map(|item| item.messages_inflight)),
        ),
        sample(
            "fitz_queue_messages_dead_lettered",
            "gauge",
            "Dead-lettered queue messages attributable to this route family",
            metric_sum(queues.iter().map(|item| item.messages_dead_lettered)),
        ),
        sample(
            "fitz_rpc_workers_registered",
            "gauge",
            "Registered RPC workers attributable to this route family",
            count(rpc_workers),
        ),
        sample(
            "fitz_rpc_requests_pending",
            "gauge",
            "Pending RPC requests attributable to this route family",
            count(rpc_pending),
        ),
        sample(
            "fitz_lease_active",
            "gauge",
            "Active leases attributable to this route family",
            count(leases),
        ),
        sample(
            "fitz_schedule_active",
            "gauge",
            "Active schedules attributable to this route family",
            count(schedules),
        ),
    ]
}

fn metric_sum(values: impl Iterator<Item = usize>) -> f64 {
    values.sum::<usize>().to_f64().unwrap_or(f64::MAX)
}

fn sort_samples(samples: &mut [StructuredMetricSample]) {
    samples.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.labels.cmp(&right.labels))
    });
}

fn parse_sample_head(head: &str) -> (String, BTreeMap<String, String>) {
    let Some(open) = head.find('{') else {
        return (head.to_string(), BTreeMap::new());
    };
    let Some(close) = head.rfind('}') else {
        return (head.to_string(), BTreeMap::new());
    };
    (
        head[..open].to_string(),
        parse_labels(&head[open + 1..close]),
    )
}

fn parse_labels(raw: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    let mut cursor = 0;
    let bytes = raw.as_bytes();
    while cursor < bytes.len() {
        while cursor < bytes.len() && (bytes[cursor] == b',' || bytes[cursor].is_ascii_whitespace())
        {
            cursor += 1;
        }
        let key_start = cursor;
        while cursor < bytes.len() && bytes[cursor] != b'=' {
            cursor += 1;
        }
        if cursor == key_start || cursor >= bytes.len() {
            break;
        }
        let key = raw[key_start..cursor].trim();
        cursor += 1;
        if cursor >= bytes.len() || bytes[cursor] != b'"' {
            break;
        }
        cursor += 1;
        let mut value = String::new();
        while cursor < bytes.len() {
            match bytes[cursor] {
                b'"' => {
                    cursor += 1;
                    break;
                }
                b'\\' if cursor + 1 < bytes.len() => {
                    cursor += 1;
                    value.push(match bytes[cursor] {
                        b'n' => '\n',
                        b'"' => '"',
                        b'\\' => '\\',
                        other => char::from(other),
                    });
                }
                byte => value.push(char::from(byte)),
            }
            cursor += 1;
        }
        labels.insert(key.to_string(), value);
    }
    labels
}

#[cfg(test)]
mod tests;
