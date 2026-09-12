// LAYER: API
//! The frames ingress synthesizes when a domain cannot answer for itself.
//!
//! Split out of `domain_frame_dispatcher.rs` for size. These three paths share
//! one property worth stating: each answers a request the domain never will, so
//! each must carry the request's correlation or the caller it belongs to waits
//! until its own timeout for a response that already arrived.

use super::domain_frame_dispatcher::{DomainErrorFrame, DomainFrameDispatcher};
use super::{DispatchDomain, IngressDecision};
use crate::observability as obs;
use std::time::Instant;
use tracing::{error, warn};

impl DomainFrameDispatcher {
    /// Answer a frame whose domain could not reply in time.
    ///
    /// The actor is alive but did not answer. That is the client's problem for
    /// this one request, not grounds to destroy a multiplexed session along
    /// with every other domain's in-flight work on it.
    ///
    /// The command was already enqueued and may still execute, so this reports
    /// an indeterminate outcome rather than a retryable rejection. Closing the
    /// session would not make this at-most-once either - the command keeps
    /// running and the client reconnects and retries with the same uncertainty
    /// - it would only add collateral damage.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn answer_indeterminate_dispatch(
        &self,
        session_id: u64,
        channel_id: crate::protocol::frame::ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        route_family: crate::runtime::routing::RouteFamily,
        domain: DispatchDomain,
        router: &crate::runtime::Router,
        correlation: Option<std::num::NonZeroU64>,
        error: &crate::runtime::router::RouteError,
        reply_claim: &crate::runtime::envelope::ReplyClaim,
    ) -> IngressDecision {
        obs::counter_inc(obs::METRIC_INGRESS_DOMAIN_DISPATCH_TIMEOUTS);
        // Every domain whose `deliver` can time out must be able to win this
        // race, not just Queue. Queue, Stream, RPC and Notice all block on an
        // actor reply, so each can have a terminal response in flight when the
        // dispatch deadline expires. Domains that never block simply never
        // contend for the claim, so taking it here is a no-op for them.
        if !reply_claim.try_claim() {
            warn!(
                session_id = session_id,
                domain = domain.as_str(),
                error = %error,
                outcome = "domain-response-won",
                "Ingress: domain dispatch timed out after its terminal response was claimed"
            );
            return IngressDecision::Accept;
        }
        warn!(
            session_id = session_id,
            domain = domain.as_str(),
            error = %error,
            outcome = Self::dispatch_timeout_outcome(),
            "Ingress: domain dispatch timed out; answering with an indeterminate outcome"
        );
        self.send_domain_error_frame(
            DomainErrorFrame {
                session_id,
                channel_id,
                msg_type,
                route_family,
                domain,
                router,
                correlation,
            },
            Self::indeterminate_error_code(domain),
            "domain timeout: request outcome unknown, do not blindly retry",
        )
        .map_or_else(|decision| decision, |()| IngressDecision::Accept)
    }

    /// Reject a frame whose domain mailbox stayed full past the retry budget.
    ///
    /// The command was never enqueued, which makes this the one failure a
    /// client can safely re-send. Answering with a rejection frame preserves
    /// that: returning `IngressDecision::Backpressure` instead closes the
    /// connection at the transport, which turns a clean retryable rejection
    /// into an unknown outcome the caller dare not retry.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn answer_exhausted_backpressure(
        &self,
        session_id: u64,
        channel_id: crate::protocol::frame::ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        route_family: crate::runtime::routing::RouteFamily,
        domain: DispatchDomain,
        router: &crate::runtime::Router,
        correlation: Option<std::num::NonZeroU64>,
        retries: u64,
        backpressure_started_at: Instant,
    ) -> IngressDecision {
        Self::record_backpressure_exhausted(backpressure_started_at);
        warn!(
            session_id = session_id,
            domain = domain.as_str(),
            retries = retries,
            waited_us = Self::elapsed_micros_u64(backpressure_started_at),
            "Ingress: domain dispatch backpressure"
        );
        self.send_domain_error_frame(
            DomainErrorFrame {
                session_id,
                channel_id,
                msg_type,
                route_family,
                domain,
                router,
                correlation,
            },
            Self::backpressure_error_code(domain),
            "domain at capacity: request was not accepted, retry with backoff",
        )
        .map_or_else(|decision| decision, |()| IngressDecision::Accept)
    }

    /// Answer a frame whose domain could not be reached at all.
    ///
    /// A dead actor, a panicked sink, an unroutable domain or a response that
    /// cannot be framed are all failures of THIS request. None is a client
    /// protocol violation, so none justifies destroying a multiplexed session
    /// and every other domain's in-flight work on it. Reported with a
    /// non-retryable code, since the command may have partially applied (the
    /// actor died holding it) or can never succeed.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn answer_unavailable_dispatch(
        &self,
        session_id: u64,
        channel_id: crate::protocol::frame::ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        route_family: crate::runtime::routing::RouteFamily,
        domain: DispatchDomain,
        router: &crate::runtime::Router,
        correlation: Option<std::num::NonZeroU64>,
        error: &crate::runtime::router::RouteError,
    ) -> IngressDecision {
        error!(
            session_id = session_id,
            domain = domain.as_str(),
            error = %error,
            "Ingress: router.route failed for domain dispatch"
        );
        self.send_domain_error_frame(
            DomainErrorFrame {
                session_id,
                channel_id,
                msg_type,
                route_family,
                domain,
                router,
                correlation,
            },
            Self::indeterminate_error_code(domain),
            "domain unavailable: request could not be completed",
        )
        .map_or_else(|decision| decision, |()| IngressDecision::Accept)
    }
}
