use super::{DateTime, Route, RouteAddress, RouteFamily, Utc};
use crate::runtime::matcher::Pattern;

pub(in crate::domains::rpc::sink) type RpcRegistrationId = u64;

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
#[derive(Clone)]
pub(in crate::domains::rpc::sink) struct RpcWorker {
    pub(in crate::domains::rpc::sink) registration_id: RpcRegistrationId,
    pub(in crate::domains::rpc::sink) addr: RouteAddress,
    pub(in crate::domains::rpc::sink) inbox_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) session_id: u64,
    pub(in crate::domains::rpc::sink) registered_at: DateTime<Utc>,
    pub(in crate::domains::rpc::sink) requests_handled: u64,
    pub(in crate::domains::rpc::sink) total_latency_us: u64,
    pattern: Pattern,
    in_flight: usize,
    pub(in crate::domains::rpc::sink) max_concurrent: usize,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(in crate::domains::rpc::sink) struct RpcWorkerKey {
    pub(in crate::domains::rpc::sink) addr: RouteAddress,
    pub(in crate::domains::rpc::sink) session_id: u64,
}

pub(in crate::domains::rpc::sink) struct RpcWorkerDispatch {
    pub(in crate::domains::rpc::sink) registration_id: RpcRegistrationId,
    pub(in crate::domains::rpc::sink) addr: RouteAddress,
    pub(in crate::domains::rpc::sink) inbox_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) session_id: u64,
}

impl RpcWorkerKey {
    pub(in crate::domains::rpc::sink) fn from_parts(addr: &RouteAddress, session_id: u64) -> Self {
        Self {
            addr: addr.clone(),
            session_id,
        }
    }
}

impl RpcWorker {
    pub(in crate::domains::rpc::sink) fn new(
        addr: RouteAddress,
        inbox_addr: RouteAddress,
        session_id: u64,
        max_concurrent: usize,
    ) -> Self {
        debug_assert!(
            (1..=1024).contains(&max_concurrent),
            "RPC worker max_concurrent must be in 1..=1024"
        );
        let pattern = Pattern::new(addr.route().as_str());
        Self {
            registration_id: 0,
            addr,
            inbox_addr,
            session_id,
            registered_at: Utc::now(),
            requests_handled: 0,
            total_latency_us: 0,
            pattern,
            in_flight: 0,
            max_concurrent,
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
        let registered_at = registered_at.into();
        let registered_at = chrono::DateTime::parse_from_rfc3339(&registered_at)
            .expect("test RPC worker timestamp must be RFC3339")
            .with_timezone(&Utc);
        let pattern = Pattern::new(addr.route().as_str());
        Self {
            registration_id: 0,
            addr,
            inbox_addr,
            session_id,
            registered_at,
            requests_handled,
            total_latency_us,
            pattern,
            in_flight: 0,
            max_concurrent: 1,
        }
    }

    pub(in crate::domains::rpc::sink) fn assign_registration_id(
        &mut self,
        registration_id: RpcRegistrationId,
    ) {
        debug_assert_eq!(self.registration_id, 0);
        self.registration_id = registration_id;
    }

    pub(in crate::domains::rpc::sink) fn is_wildcard(&self) -> bool {
        self.pattern.is_wildcard()
    }

    pub(in crate::domains::rpc::sink) fn pattern(&self) -> &Pattern {
        &self.pattern
    }

    pub(in crate::domains::rpc::sink) fn matches(
        &self,
        family: RouteFamily,
        route: &Route,
    ) -> bool {
        *self.addr.family() == family && self.pattern.matches(route)
    }

    pub(in crate::domains::rpc::sink) fn is_available(&self) -> bool {
        self.in_flight < self.max_concurrent
    }

    pub(in crate::domains::rpc::sink) fn claim_slot(&mut self) {
        debug_assert!(self.is_available());
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
    pub(in crate::domains::rpc::sink) fn registered_at_rfc3339(&self) -> String {
        self.registered_at
            .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
    }

    pub(in crate::domains::rpc::sink) fn dispatch_view(&self) -> RpcWorkerDispatch {
        RpcWorkerDispatch {
            registration_id: self.registration_id,
            addr: self.addr.clone(),
            inbox_addr: self.inbox_addr.clone(),
            session_id: self.session_id,
        }
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) fn average_latency_ms(&self) -> f64 {
        if self.requests_handled == 0 {
            return 0.0;
        }

        let average_latency_us = self.total_latency_us / self.requests_handled;
        std::time::Duration::from_micros(average_latency_us).as_secs_f64() * 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "RPC worker max_concurrent must be in 1..=1024")]
    fn should_reject_zero_max_concurrent_at_domain_boundary() {
        // Arrange
        let family = RouteFamily::new(1);
        let worker = RouteAddress::new(family, Route::new("rpc://acme/jobs/run"));
        let inbox = RouteAddress::new(family, Route::new("inbox://session/1"));

        // Act
        let _worker = RpcWorker::new(worker, inbox, 1, 0);

        // Assert: construction must panic before an invalid worker can enter domain state.
    }
}
