//! Shared ingress helpers for domain sinks.

use super::{DeliveryError, Envelope};
use std::sync::atomic::{AtomicBool, Ordering};

/// Reject delivery if the domain sink has been marked inactive.
pub(crate) fn ensure_actor_active(active: &AtomicBool) -> Result<(), DeliveryError> {
    if !active.load(Ordering::Relaxed) {
        return Err(DeliveryError::ActorStopped);
    }

    Ok(())
}

/// Trace-log an inbound envelope at debug level, tagged with the domain name.
///
/// `message` carries the domain-specific log text (e.g. `"Lease domain
/// sink: received envelope"`) so each caller keeps its own capitalization.
pub(crate) fn log_envelope_received(
    domain: &'static str,
    message: &'static str,
    envelope: &Envelope,
) {
    tracing::debug!(
        domain = domain,
        destination = %envelope.destination(),
        source = ?envelope.source(),
        "{}",
        message
    );
}
