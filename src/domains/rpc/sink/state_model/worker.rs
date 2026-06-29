use super::*;

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
#[derive(Clone)]
pub(in crate::domains::rpc::sink) struct RpcWorker {
    pub(in crate::domains::rpc::sink) addr: RouteAddress,
    pub(in crate::domains::rpc::sink) inbox_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) session_id: u64,
    pub(in crate::domains::rpc::sink) registered_at: String,
    pub(in crate::domains::rpc::sink) requests_handled: u64,
    pub(in crate::domains::rpc::sink) total_latency_us: u64,
    pub(in crate::domains::rpc::sink) in_flight: usize,
    pub(in crate::domains::rpc::sink) max_concurrent: usize,
}

impl RpcWorker {
    pub(in crate::domains::rpc::sink) fn new(
        addr: RouteAddress,
        inbox_addr: RouteAddress,
        session_id: u64,
    ) -> Self {
        Self {
            addr,
            inbox_addr,
            session_id,
            registered_at: Utc::now().to_rfc3339(),
            requests_handled: 0,
            total_latency_us: 0,
            in_flight: 0,
            max_concurrent: 1,
        }
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn with_stats(
        addr: RouteAddress,
        inbox_addr: RouteAddress,
        session_id: u64,
        registered_at: impl Into<String>,
        requests_handled: u64,
        total_latency_us: u64,
    ) -> Self {
        Self {
            addr,
            inbox_addr,
            session_id,
            registered_at: registered_at.into(),
            requests_handled,
            total_latency_us,
            in_flight: 0,
            max_concurrent: 1,
        }
    }

    pub(in crate::domains::rpc::sink) fn is_available(&self) -> bool {
        self.in_flight < self.max_concurrent
    }

    pub(in crate::domains::rpc::sink) fn claim_slot(&mut self) {
        self.in_flight = self.in_flight.saturating_add(1);
    }

    pub(in crate::domains::rpc::sink) fn release_slot(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    pub(in crate::domains::rpc::sink) fn record_completion(&mut self, latency_us: u64) {
        self.requests_handled = self.requests_handled.saturating_add(1);
        self.total_latency_us = self.total_latency_us.saturating_add(latency_us);
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) fn average_latency_ms(&self) -> f64 {
        if self.requests_handled == 0 {
            return 0.0;
        }

        self.total_latency_us as f64 / 1000.0 / self.requests_handled as f64
    }
}
