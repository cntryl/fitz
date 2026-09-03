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
    runtime.refresh_stream_admin_snapshot();
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

/// Generate Prometheus-format metrics
fn generate_prometheus_metrics(runtime: &Runtime) -> String {
    let mut output = String::new();

    broker::append_broker_metrics(&mut output, runtime);
    collector::append_observability_metrics(&mut output);
    domains::append_domain_metrics(&mut output, runtime);

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
    let read_model = runtime.admin_read_model();
    let family_label = || BTreeMap::from([(String::from("family"), family.to_string())]);
    let sample = |name: &str, kind: &str, help: &str, value: f64| StructuredMetricSample {
        name: name.to_string(),
        kind: kind.to_string(),
        help: help.to_string(),
        labels: family_label(),
        value,
    };
    let count = |value: usize| value.to_f64().unwrap_or(f64::MAX);

    let kv_transactions = read_model
        .kv_transactions(None)
        .into_iter()
        .filter(|item| item.route_family == family)
        .count();
    let streams = read_model
        .streams(None)
        .into_iter()
        .filter(|item| item.route_family == family)
        .collect::<Vec<_>>();
    let notice_subscriptions = read_model
        .notice_subscriptions(None, None)
        .into_iter()
        .filter(|item| item.route_family == family)
        .count();
    let notice_routes = read_model
        .notice_routes(None)
        .into_iter()
        .filter(|item| item.route_family == family)
        .collect::<Vec<_>>();
    let queues = read_model
        .queues(None)
        .into_iter()
        .filter(|item| item.family == family)
        .collect::<Vec<_>>();
    let rpc_workers = read_model
        .rpc_workers(None)
        .into_iter()
        .filter(|item| item.route_family == family)
        .count();
    let rpc_pending = read_model
        .rpc_pending(None)
        .into_iter()
        .filter(|item| item.route_family == family)
        .count();
    let leases = read_model.leases_for_route_family(family).len();
    let schedules = read_model.schedules_for_route_family(family).len();
    let sessions = read_model
        .sessions()
        .into_iter()
        .filter(|item| item.route_family == family)
        .count();

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
            count(streams.len()),
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
