use serde::Serialize;
use std::time::Duration;

/// Fixed storage; bounds are powers of two nanoseconds, rounded up.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct Latencies {
    bins: Vec<u64>,
    pub(crate) count: u64,
    pub(crate) max_ns: u64,
}

impl Latencies {
    pub(crate) fn record(&mut self, elapsed: Duration) {
        if self.bins.is_empty() {
            self.bins.resize(65, 0);
        }
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let index = usize::try_from(nanos.saturating_sub(1).bit_width()).unwrap_or(64);
        self.bins[index] = self.bins[index].saturating_add(1);
        self.count = self.count.saturating_add(1);
        self.max_ns = self.max_ns.max(nanos);
    }

    pub(crate) fn percentile_upper_ns(&self, percent: u64) -> u64 {
        let target = self.count.saturating_mul(percent).div_ceil(100);
        if target == 0 {
            return 0;
        }
        let mut seen = 0_u64;
        for (index, count) in self.bins.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= target {
                return 1_u64
                    .checked_shl(u32::try_from(index).unwrap_or(64))
                    .unwrap_or(u64::MAX);
            }
        }
        self.max_ns
    }
}
