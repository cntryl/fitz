/// Boot-time observability initialization
///
/// Handles:
/// - Tracing subscriber setup (text or JSON)
/// - OpenTelemetry OTLP exporter initialization
/// - Metrics collector creation and global setup
/// - Sampling rate configuration
use crate::observability::metrics::MetricsCollector;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use std::time::Instant;

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
        std::env::var("FITZ_HOT_PATH_METRICS").is_ok_and(|value| {
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

/// Access the initialized collector without creating it.
#[doc(hidden)]
pub fn initialized_metrics() -> Option<Arc<MetricsCollector>> {
    METRICS_COLLECTOR.get().cloned()
}
