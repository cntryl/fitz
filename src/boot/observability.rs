//! Compatibility exports for observability initialization.

pub use crate::observability::{
    ScopedHistogramUs, counter_add, counter_inc, gauge_dec, gauge_inc, gauge_set,
    histogram_observe_us, hot_path_counter_inc, hot_path_histogram_observe_us,
    hot_path_metrics_enabled, init_observability, metrics, try_init_observability,
};
