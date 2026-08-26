//! Notice domain sink module wiring.
//!
//! See `state.rs` for what's owned (broker-local, session-scoped, never
//! durable), `admin_projection.rs` for the read-model mirror, and
//! `ingress.rs`/`subscriptions.rs`/`publish.rs`/`responses.rs`/`cleanup.rs`
//! for the request lifecycle.

use crate::domains::notice::NoticeMetrics;
use crate::domains::subscription_state::RoutedSubscriptionSet;
use crate::runtime::{DeliveryError, Envelope, MailboxSink};
use std::time::Instant;

mod actor_runtime;
mod admin_projection;
mod cleanup;
mod delivery_worker;
mod facade;
mod ingress;
mod mailbox_sink_impl;
mod model;
mod publish;
mod responses;
mod state;
mod subscriptions;
#[cfg(test)]
mod test_channels;
mod validation;

use actor_runtime::{NoticeDomainActor, NoticeDomainCommand};
use delivery_worker::{notice_delivery_worker, NoticeDeliveryJob, NOTICE_DELIVERY_HANDOFF_TIMEOUT};
use model::{
    notice_route_realm, NoticeDeliveryTarget, NoticeDeliveryTargets, NoticeMatchedRoutePatterns,
    NoticeRouteStats, NoticeRouteStatsKey, NoticeSubscription,
};
use state::NoticeDomainCore;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(test)]
use test_channels::{test_client_channel_from_protocol, test_protocol_channel_from_client};
use validation::subscription_limit_error;

pub use state::NoticeDomainSink;

impl NoticeDomainCore {
    fn counter_add(&self, name: &str, amount: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.counter_add(name, amount);
        } else {
            crate::observability::counter_add(name, amount);
        }
    }

    pub(super) fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(RoutedSubscriptionSet::subscription_count)
            .sum()
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
