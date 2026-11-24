//! Lease-specific benchmark fixtures.

use crate::domains::lease::LeaseDomain;
use crate::runtime::envelope::Envelope;
use crate::runtime::router::{DeliveryError, MailboxSink, Router};
use std::sync::Arc;

/// Typed Lease fixture for measuring the production expiry sweep and its
/// same-lane completion barrier without expanding the generic bench handle.
#[cfg(any(test, feature = "benchkit"))]
pub struct LeaseSweepFixture {
    sink: Arc<LeaseDomain>,
}

#[cfg(any(test, feature = "benchkit"))]
impl LeaseSweepFixture {
    #[must_use]
    pub fn new(router: Arc<Router>) -> Self {
        Self {
            sink: super::live_sink::create_raw_bench_lease_sink(router),
        }
    }

    /// Invoke Lease's normal sweep and wait for a count request on the same
    /// control lane. The returned count also verifies the fixture's live set.
    #[must_use]
    pub fn sweep_and_count(&self) -> usize {
        self.sink.sweep_expired_state();
        self.sink.lease_count()
    }

    #[must_use]
    pub fn live_count(&self) -> usize {
        self.sink.lease_count()
    }

    pub fn stop(&self) {
        self.sink.stop();
    }
}

#[cfg(any(test, feature = "benchkit"))]
impl MailboxSink for LeaseSweepFixture {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.sink.deliver(envelope)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.sink.deliver_high_priority(envelope)
    }
}

#[cfg(any(test, feature = "benchkit"))]
#[must_use]
pub fn create_bench_lease_sweep_fixture(router: Arc<Router>) -> LeaseSweepFixture {
    LeaseSweepFixture::new(router)
}
