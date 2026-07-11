/// Boot-time observability initialization
///
/// Handles:
/// - Tracing subscriber setup (text or JSON)
/// - OpenTelemetry OTLP exporter initialization
/// - Metrics collector creation and global setup
/// - Sampling rate configuration
use crate::observability::metrics::MetricsCollector;
use once_cell::sync::OnceCell;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{self as sdktrace, SdkTracerProvider},
    Resource,
};
use std::sync::Arc;
use std::time::Instant;
use tracing_subscriber::{fmt, prelude::*, util::SubscriberInitExt, EnvFilter};
use uuid::Uuid;

/// Global metrics collector (initialized once during boot)
static METRICS_COLLECTOR: OnceCell<Arc<MetricsCollector>> = OnceCell::new();
static HOT_PATH_METRICS_ENABLED: OnceCell<bool> = OnceCell::new();

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Get the global metrics collector.
///
/// If observability has not been initialized yet, this lazily creates
/// a default collector so metrics recording never panics.
pub fn metrics() -> Arc<MetricsCollector> {
    METRICS_COLLECTOR
        .get_or_init(|| Arc::new(MetricsCollector::new()))
        .clone()
}

fn metrics_ref() -> &'static Arc<MetricsCollector> {
    METRICS_COLLECTOR.get_or_init(|| Arc::new(MetricsCollector::new()))
}

/// Record a histogram observation in microseconds using the cached global collector.
pub fn histogram_observe_us(name: &str, value_us: u64) {
    metrics_ref().histogram_observe_us(name, value_us);
}

/// Increment a counter using the cached global collector.
pub fn counter_inc(name: &str) {
    metrics_ref().counter_inc(name);
}

/// Add to a counter using the cached global collector.
pub fn counter_add(name: &str, amount: u64) {
    metrics_ref().counter_add(name, amount);
}

/// Set a gauge using the cached global collector.
pub fn gauge_set(name: &str, value: u64) {
    metrics_ref().gauge_set(name, value);
}

/// Increment a gauge by 1 using the cached global collector.
pub fn gauge_inc(name: &str) {
    metrics_ref().gauge_inc(name);
}

/// Decrement a gauge by 1 using the cached global collector.
pub fn gauge_dec(name: &str) {
    metrics_ref().gauge_dec(name);
}

/// Whether extra hot-path attribution metrics are enabled.
///
/// Disabled by default because these measurements are expensive enough to distort
/// benchmark results. Enable explicitly with `FITZ_HOT_PATH_METRICS=true` when
/// collecting attribution data.
pub fn hot_path_metrics_enabled() -> bool {
    *HOT_PATH_METRICS_ENABLED.get_or_init(|| {
        std::env::var("FITZ_HOT_PATH_METRICS")
            .ok()
            .is_some_and(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
    })
}

/// Record a hot-path histogram only when explicit attribution is enabled.
pub fn hot_path_histogram_observe_us(name: &str, value_us: u64) {
    if hot_path_metrics_enabled() {
        histogram_observe_us(name, value_us);
    }
}

/// Increment a hot-path counter only when explicit attribution is enabled.
pub fn hot_path_counter_inc(name: &str) {
    if hot_path_metrics_enabled() {
        counter_inc(name);
    }
}

/// RAII helper that records a histogram in microseconds when dropped.
pub struct ScopedHistogramUs {
    name: &'static str,
    start: Option<Instant>,
}

impl ScopedHistogramUs {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            start: hot_path_metrics_enabled().then(Instant::now),
        }
    }
}

impl Drop for ScopedHistogramUs {
    fn drop(&mut self) {
        let Some(start) = self.start else {
            return;
        };

        let elapsed_us = u128_to_u64_saturating(start.elapsed().as_micros());
        histogram_observe_us(self.name, elapsed_us);
    }
}

/// Try to initialize observability, returning existing collector if already initialized.
///
/// This is safe to call multiple times (useful for tests).
/// Returns the existing collector if already initialized, or creates a new one.
///
/// # Errors
///
/// Returns an error if full observability initialization fails.
pub fn try_init_observability() -> Result<Arc<MetricsCollector>, Box<dyn std::error::Error>> {
    try_init_observability_with_defaults(None, None)
}

pub(crate) fn try_init_observability_with_defaults(
    default_log_level: Option<&str>,
    default_otel_enabled: Option<bool>,
) -> Result<Arc<MetricsCollector>, Box<dyn std::error::Error>> {
    // Check if already initialized
    if let Some(existing) = METRICS_COLLECTOR.get() {
        return Ok(existing.clone());
    }

    // Not initialized yet, call full init
    init_observability_with_defaults(default_log_level, default_otel_enabled)
}

pub(crate) fn try_init_bench_observability(
) -> Result<Arc<MetricsCollector>, Box<dyn std::error::Error>> {
    // Benchmarks should emit only stress output unless explicitly opted into logs.
    if let Some(existing) = METRICS_COLLECTOR.get() {
        return Ok(existing.clone());
    }

    init_observability_with_options(Some("off"), Some(false), true)
}

fn default_env_filter(log_level: &str) -> EnvFilter {
    if log_level == "off" {
        EnvFilter::new("off")
    } else {
        EnvFilter::new(format!("fitz={log_level},warn"))
    }
}

fn resolve_env_filter(ignore_env_overrides: bool, log_level: &str) -> EnvFilter {
    if ignore_env_overrides {
        return default_env_filter(log_level);
    }

    if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| default_env_filter(log_level))
    } else {
        default_env_filter(log_level)
    }
}

fn service_identity() -> (String, String) {
    let instance_id =
        std::env::var("FITZ_SERVICE_INSTANCE_ID").unwrap_or_else(|_| Uuid::new_v4().to_string());
    let environment =
        std::env::var("FITZ_DEPLOYMENT_ENVIRONMENT").unwrap_or_else(|_| "unknown".to_string());
    (instance_id, environment)
}

/// Initialize observability (tracing, metrics, OTEL).
///
/// # Environment Variables
///
/// - `FITZ_LOG_FORMAT` (text|json): Logging format. Default: text
/// - `FITZ_LOG_LEVEL` (off|trace|debug|info|warn): Log level. Default: info
/// - `OTEL_ENABLED` (true|false): Enable OTLP export. Default: true
/// - `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP collector endpoint. Default: <http://localhost:4317>
/// - `FITZ_METRICS_BIND_ADDR`: unauthenticated Prometheus bind address. Default: 127.0.0.1
/// - `FITZ_METRICS_PORT`: dedicated Prometheus listener port. Default: 9090
/// - `RUST_LOG`: Legacy env var for log filtering (takes precedence if set)
///
/// # Returns
///
/// Arc<MetricsCollector> that can be used throughout the application
///
/// # Errors
///
/// Returns an error if the OpenTelemetry exporter cannot be initialized.
pub fn init_observability() -> Result<Arc<MetricsCollector>, Box<dyn std::error::Error>> {
    init_observability_with_defaults(None, None)
}

fn init_observability_with_defaults(
    default_log_level: Option<&str>,
    default_otel_enabled: Option<bool>,
) -> Result<Arc<MetricsCollector>, Box<dyn std::error::Error>> {
    init_observability_with_options(default_log_level, default_otel_enabled, false)
}

fn init_observability_with_options(
    default_log_level: Option<&str>,
    default_otel_enabled: Option<bool>,
    ignore_env_overrides: bool,
) -> Result<Arc<MetricsCollector>, Box<dyn std::error::Error>> {
    // Detect logging format
    let log_format = std::env::var("FITZ_LOG_FORMAT")
        .unwrap_or_else(|_| "text".to_string())
        .to_lowercase();

    // Detect log level
    let log_level = if ignore_env_overrides {
        default_log_level.unwrap_or("info").to_string()
    } else {
        std::env::var("FITZ_LOG_LEVEL")
            .ok()
            .unwrap_or_else(|| default_log_level.unwrap_or("info").to_string())
    }
    .to_lowercase();

    // Build env filter (RUST_LOG takes precedence)
    let env_filter = resolve_env_filter(ignore_env_overrides, &log_level);

    // Derive service identity and environment metadata
    let (service_instance_id, deployment_environment) = service_identity();

    let fmt_layer = if log_format == "json" {
        fmt::layer()
            .json()
            .with_current_span(true)
            .with_target(true)
            .with_level(true)
            .boxed()
    } else {
        fmt::layer()
            .compact()
            .with_timer(fmt::time::SystemTime)
            .with_ansi(true)
            .with_writer(std::io::stderr)
            .with_target(true)
            .with_level(true)
            .with_file(false)
            .with_line_number(false)
            .boxed()
    };

    let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    // Initialize OpenTelemetry OTLP exporter when enabled.
    let otel_enabled = if ignore_env_overrides {
        default_otel_enabled.unwrap_or(true)
    } else {
        std::env::var("OTEL_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(default_otel_enabled.unwrap_or(true))
    };

    if otel_enabled {
        let resource = Resource::builder_empty()
            .with_attributes([
                KeyValue::new(
                    "service.name",
                    crate::observability::SERVICE_NAME.to_string(),
                ),
                KeyValue::new(
                    "service.version",
                    crate::observability::SERVICE_VERSION.to_string(),
                ),
                KeyValue::new("service.instance.id", service_instance_id.clone()),
                KeyValue::new("deployment.environment", deployment_environment.clone()),
            ])
            .build();

        global::set_text_map_propagator(TraceContextPropagator::new());

        let span_exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(otel_endpoint.clone())
            .build()
            .map_err(|e| format!("Failed to init OTEL exporter: {e}"))?;

        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_sampler(sdktrace::Sampler::TraceIdRatioBased(
                std::env::var("FITZ_OTEL_SAMPLE_RATIO")
                    .ok()
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or(1.0),
            ))
            .with_batch_exporter(span_exporter)
            .build();

        global::set_tracer_provider(tracer_provider);
        let tracer = global::tracer(crate::observability::SERVICE_NAME);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()
            .ok();

        tracing::info!("OpenTelemetry enabled, exporting to: {}", otel_endpoint);
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()
            .ok();

        tracing::info!("OpenTelemetry disabled");
    }

    let metrics_collector = METRICS_COLLECTOR
        .get_or_init(|| Arc::new(MetricsCollector::new()))
        .clone();

    Ok(metrics_collector)
}

#[cfg(test)]
mod tests {
    #[test]
    fn should_initialize_observability_once() {
        // Note: This test assumes the global is not yet initialized
        // In practice, you'd want to use a test harness that resets globals
        // between test runs.
    }
}
