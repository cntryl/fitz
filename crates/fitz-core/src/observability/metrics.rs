/// Synchronous, lock-free metrics collector for the Fitz runtime.
///
/// Uses `DashMap` for concurrent counter/gauge access without blocking.
/// Histograms use a simple Vec-based approach (preallocated buckets).
/// No async I/O; all updates are atomic or lock-free.
///
/// Safe to use from sync domain code.
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const HISTOGRAM_BUCKET_BOUNDS: [&str; 9] = [
    "1ms", "5ms", "10ms", "50ms", "100ms", "500ms", "1s", "5s", "+Inf",
];

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn append_metric_metadata(output: &mut String, name: &str, metric_type: &str, help: &str) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {metric_type}");
}

/// Metrics collector instance.
/// Clone-safe; backed by Arc<DashMap>.
#[derive(Clone)]
pub struct MetricsCollector {
    counters: Arc<DashMap<String, Arc<AtomicU64>>>,
    gauges: Arc<DashMap<String, Arc<AtomicU64>>>,
    histograms: Arc<DashMap<String, Arc<Histogram>>>,
}

/// Simple histogram with buckets (1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, +inf).
pub struct Histogram {
    buckets: [AtomicU64; 9], // 8 buckets + one for infinity
}

#[derive(Clone)]
pub struct DomainMetricSet {
    collector: MetricsCollector,
    requests_total: &'static str,
    success_total: &'static str,
    failure_total: &'static str,
    latency_ms: &'static str,
}

impl DomainMetricSet {
    #[must_use]
    pub fn new(
        collector: MetricsCollector,
        requests_total: &'static str,
        success_total: &'static str,
        failure_total: &'static str,
        latency_ms: &'static str,
    ) -> Self {
        Self {
            collector,
            requests_total,
            success_total,
            failure_total,
            latency_ms,
        }
    }

    #[must_use]
    pub fn record_request_start(&self) -> Instant {
        self.collector.counter_inc(self.requests_total);
        Instant::now()
    }

    pub fn record_success(&self, started_at: Instant) {
        self.collector.counter_inc(self.success_total);
        self.observe_latency_ms(started_at);
    }

    pub fn record_failure(&self, started_at: Instant) {
        self.collector.counter_inc(self.failure_total);
        self.observe_latency_ms(started_at);
    }

    pub fn counter_inc(&self, name: &str) {
        self.collector.counter_inc(name);
    }

    pub fn counter_add(&self, name: &str, amount: u64) {
        self.collector.counter_add(name, amount);
    }

    pub fn gauge_set(&self, name: &str, value: u64) {
        self.collector.gauge_set(name, value);
    }

    pub fn gauge_inc(&self, name: &str) {
        self.collector.gauge_inc(name);
    }

    pub fn gauge_dec(&self, name: &str) {
        self.collector.gauge_dec(name);
    }

    pub fn histogram_observe_us(&self, name: &str, value_us: u64) {
        self.collector.histogram_observe_us(name, value_us);
    }

    pub fn histogram_observe_ms(&self, name: &str, value_ms: u64) {
        self.collector.histogram_observe_ms(name, value_ms);
    }

    fn observe_latency_ms(&self, started_at: Instant) {
        let elapsed_ms = u128_to_u64_saturating(started_at.elapsed().as_millis());
        self.collector
            .histogram_observe_ms(self.latency_ms, elapsed_ms);
    }
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    /// Record a value in milliseconds.
    pub fn observe_ms(&self, value_ms: u64) {
        let bucket = match value_ms {
            0..=1 => 0,
            2..=5 => 1,
            6..=10 => 2,
            11..=50 => 3,
            51..=100 => 4,
            101..=500 => 5,
            501..=1000 => 6,
            1001..=5000 => 7,
            _ => 8,
        };
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// Record a value in microseconds (converts to milliseconds).
    pub fn observe_us(&self, value_us: u64) {
        let millis = value_us / 1000;
        self.observe_ms(millis);
    }

    /// Get all bucket counts (for Prometheus export).
    pub fn get_buckets(&self) -> [u64; 9] {
        [
            self.buckets[0].load(Ordering::Relaxed),
            self.buckets[1].load(Ordering::Relaxed),
            self.buckets[2].load(Ordering::Relaxed),
            self.buckets[3].load(Ordering::Relaxed),
            self.buckets[4].load(Ordering::Relaxed),
            self.buckets[5].load(Ordering::Relaxed),
            self.buckets[6].load(Ordering::Relaxed),
            self.buckets[7].load(Ordering::Relaxed),
            self.buckets[8].load(Ordering::Relaxed),
        ]
    }
}

impl MetricsCollector {
    /// Create a new metrics collector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
            gauges: Arc::new(DashMap::new()),
            histograms: Arc::new(DashMap::new()),
        }
    }

    /// Clear all recorded metrics.
    pub fn clear(&self) {
        self.counters.clear();
        self.gauges.clear();
        self.histograms.clear();
    }

    // ========================================================================
    // Counter Operations
    // ========================================================================

    /// Increment a counter by 1.
    pub fn counter_inc(&self, name: &str) {
        self.counter_add(name, 1);
    }

    /// Increment a counter by a specific value.
    pub fn counter_add(&self, name: &str, amount: u64) {
        if let Some(counter) = self.counters.get(name) {
            counter.fetch_add(amount, Ordering::Relaxed);
            return;
        }
        let counter = self
            .counters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        counter.fetch_add(amount, Ordering::Relaxed);
    }

    /// Get current counter value.
    #[must_use]
    pub fn counter_get(&self, name: &str) -> u64 {
        self.counters
            .get(name)
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    // ========================================================================
    // Gauge Operations (set to specific value, not increment)
    // ========================================================================

    /// Set a gauge to a specific value.
    pub fn gauge_set(&self, name: &str, value: u64) {
        if let Some(gauge) = self.gauges.get(name) {
            gauge.store(value, Ordering::Relaxed);
            return;
        }
        let gauge = self
            .gauges
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        gauge.store(value, Ordering::Relaxed);
    }

    /// Increment a gauge.
    pub fn gauge_inc(&self, name: &str) {
        if let Some(gauge) = self.gauges.get(name) {
            gauge.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let gauge = self
            .gauges
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        gauge.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement a gauge.
    pub fn gauge_dec(&self, name: &str) {
        if let Some(gauge) = self.gauges.get(name) {
            gauge.fetch_sub(1, Ordering::Relaxed);
            return;
        }
        let gauge = self
            .gauges
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        gauge.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current gauge value.
    #[must_use]
    pub fn gauge_get(&self, name: &str) -> u64 {
        self.gauges
            .get(name)
            .map_or(0, |g| g.load(Ordering::Relaxed))
    }

    // ========================================================================
    // Histogram Operations
    // ========================================================================

    /// Record an observation (in milliseconds) to a histogram.
    pub fn histogram_observe_ms(&self, name: &str, value_ms: u64) {
        if let Some(histogram) = self.histograms.get(name) {
            histogram.observe_ms(value_ms);
            return;
        }
        let histogram = self
            .histograms
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Histogram::new()))
            .clone();
        histogram.observe_ms(value_ms);
    }

    /// Record an observation (in microseconds) to a histogram.
    pub fn histogram_observe_us(&self, name: &str, value_us: u64) {
        if let Some(histogram) = self.histograms.get(name) {
            histogram.observe_us(value_us);
            return;
        }
        let histogram = self
            .histograms
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Histogram::new()))
            .clone();
        histogram.observe_us(value_us);
    }

    /// Get histogram bucket counts.
    #[must_use]
    pub fn histogram_get_buckets(&self, name: &str) -> Option<[u64; 9]> {
        self.histograms.get(name).map(|h| h.get_buckets())
    }

    // ========================================================================
    // Export/Snapshot Operations (for Prometheus)
    // ========================================================================

    /// Export all counters as a map (for Prometheus scrape).
    #[must_use]
    pub fn export_counters(&self) -> BTreeMap<String, u64> {
        let mut result = BTreeMap::new();
        for entry in self.counters.iter() {
            let name = entry.key().clone();
            let value = entry.value().load(Ordering::Relaxed);
            result.insert(name, value);
        }
        result
    }

    /// Export all gauges as a map (for Prometheus scrape).
    #[must_use]
    pub fn export_gauges(&self) -> BTreeMap<String, u64> {
        let mut result = BTreeMap::new();
        for entry in self.gauges.iter() {
            let name = entry.key().clone();
            let value = entry.value().load(Ordering::Relaxed);
            result.insert(name, value);
        }
        result
    }

    /// Export all histograms with their buckets (for Prometheus scrape).
    #[must_use]
    pub fn export_histograms(&self) -> BTreeMap<String, [u64; 9]> {
        let mut result = BTreeMap::new();
        for entry in self.histograms.iter() {
            let name = entry.key().clone();
            let buckets = entry.value().get_buckets();
            result.insert(name, buckets);
        }
        result
    }

    /// Generate Prometheus text format output.
    /// This is called by the `/metrics` HTTP endpoint.
    #[must_use]
    pub fn to_prometheus_text(&self) -> String {
        let mut output = String::new();

        for (name, value) in self.export_counters() {
            append_metric_metadata(&mut output, &name, "counter", "Fitz counter metric");
            let _ = writeln!(output, "{name} {value}");
            output.push('\n');
        }

        for (name, value) in self.export_gauges() {
            append_metric_metadata(&mut output, &name, "gauge", "Fitz gauge metric");
            let _ = writeln!(output, "{name} {value}");
            output.push('\n');
        }

        for (name, buckets) in self.export_histograms() {
            append_metric_metadata(&mut output, &name, "histogram", "Fitz histogram metric");
            let mut cumsum = 0u64;
            for (i, bucket_bound) in HISTOGRAM_BUCKET_BOUNDS.iter().enumerate() {
                cumsum += buckets[i];
                let _ = writeln!(output, "{name}{{le=\"{bucket_bound}\"}} {cumsum}");
            }
            let _ = writeln!(output, "{name}\u{005f}count {cumsum}");
            output.push('\n');
        }

        output
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_increment_counter() {
        // Arrange
        let mc = MetricsCollector::new();

        // Act
        mc.counter_inc("test_counter");
        mc.counter_inc("test_counter");

        // Assert
        assert_eq!(mc.counter_get("test_counter"), 2);
    }

    #[test]
    fn should_add_to_counter() {
        let mc = MetricsCollector::new();
        mc.counter_add("test_counter", 5);
        assert_eq!(mc.counter_get("test_counter"), 5);
    }

    #[test]
    fn should_set_gauge() {
        // Arrange
        let mc = MetricsCollector::new();

        // Act
        mc.gauge_set("test_gauge", 42);
        mc.gauge_set("test_gauge", 7);

        // Assert
        assert_eq!(mc.gauge_get("test_gauge"), 7);
    }

    #[test]
    fn should_increment_gauge() {
        // Arrange
        let mc = MetricsCollector::new();
        mc.gauge_set("test_gauge", 10);

        // Act
        mc.gauge_inc("test_gauge");

        // Assert
        assert_eq!(mc.gauge_get("test_gauge"), 11);
    }

    #[test]
    fn should_decrement_gauge() {
        // Arrange
        let mc = MetricsCollector::new();
        mc.gauge_set("test_gauge", 10);

        // Act
        mc.gauge_dec("test_gauge");

        // Assert
        assert_eq!(mc.gauge_get("test_gauge"), 9);
    }

    #[test]
    fn should_observe_histogram_ms() {
        // Arrange
        let mc = MetricsCollector::new();

        // Act
        mc.histogram_observe_ms("test_histogram", 1);
        mc.histogram_observe_ms("test_histogram", 5);
        mc.histogram_observe_ms("test_histogram", 100);

        // Assert
        let buckets = mc.histogram_get_buckets("test_histogram").unwrap();
        assert_eq!(buckets[0], 1); // 0-1ms bucket
        assert_eq!(buckets[1], 1); // 2-5ms bucket
        assert_eq!(buckets[4], 1); // 51-100ms bucket
    }

    #[test]
    fn should_observe_histogram_us() {
        // Arrange
        let mc = MetricsCollector::new();

        // Act
        mc.histogram_observe_us("test_histogram", 1000); // 1ms

        // Assert
        let buckets = mc.histogram_get_buckets("test_histogram").unwrap();
        assert_eq!(buckets[0], 1);
    }

    #[test]
    fn should_export_prometheus_text() {
        // Arrange
        let mc = MetricsCollector::new();
        mc.counter_add("test_counter", 42);
        mc.gauge_set("test_gauge", 10);
        mc.histogram_observe_ms("test_latency_ms", 1);

        // Act
        let text = mc.to_prometheus_text();

        // Assert
        assert!(text.contains("# TYPE test_counter counter"));
        assert!(text.contains("test_counter 42"));
        assert!(text.contains("# TYPE test_gauge gauge"));
        assert!(text.contains("test_gauge 10"));
        assert!(text.contains("# TYPE test_latency_ms histogram"));
        assert!(text.contains("test_latency_ms_count 1"));
    }
}
