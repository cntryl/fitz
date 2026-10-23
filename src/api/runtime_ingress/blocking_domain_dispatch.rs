use super::domain_frame_dispatcher::{DomainFrameDispatcher, BLOCKING_DOMAIN_DISPATCH_CONCURRENCY};
use super::DispatchDomain;
use crate::domains::queue::QUEUE_CLIENT_REPLY_TIMEOUT;
use std::time::Instant;
use tracing::warn;

impl DomainFrameDispatcher {
    pub(super) async fn route_client_domain(
        &self,
        router: &crate::runtime::Router,
        domain: DispatchDomain,
        mut envelope: crate::runtime::Envelope,
        started_at: Instant,
    ) -> Result<(), crate::runtime::router::RouteError> {
        let Some(permits) = self.blocking_handoff_permits(domain, &envelope) else {
            return router.route_to_domain(domain.as_str(), envelope);
        };

        let destination = envelope.destination().clone();
        let permit = match permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                return Err(crate::runtime::router::RouteError::DeliveryFailed(
                    destination,
                    crate::runtime::DeliveryError::MailboxFull {
                        capacity: BLOCKING_DOMAIN_DISPATCH_CONCURRENCY,
                        current_len: BLOCKING_DOMAIN_DISPATCH_CONCURRENCY,
                    },
                ));
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(crate::runtime::router::RouteError::DeliveryFailed(
                    destination,
                    crate::runtime::DeliveryError::ActorStopped,
                ));
            }
        };

        let queue_guard = if domain == DispatchDomain::Queue {
            let family_lock = self
                .queue_family_dispatch
                .entry(destination.family().id())
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
                .clone();
            let deadline = envelope
                .deadline()
                .map_or(started_at + QUEUE_CLIENT_REPLY_TIMEOUT, |deadline| {
                    deadline.min(started_at + QUEUE_CLIENT_REPLY_TIMEOUT)
                });
            envelope = envelope.with_deadline(deadline);
            match tokio::time::timeout_at(deadline.into(), family_lock.lock_owned()).await {
                Ok(guard) => Some(guard),
                Err(_) => {
                    return Err(crate::runtime::router::RouteError::DeliveryFailed(
                        destination,
                        crate::runtime::DeliveryError::MailboxFull {
                            capacity: 1,
                            current_len: 1,
                        },
                    ));
                }
            }
        } else {
            None
        };

        let blocking_router = self
            .router
            .as_ref()
            .expect("domain dispatch requires an attached router")
            .clone();
        match tokio::task::spawn_blocking(move || {
            // Queue bounds pending waiters separately from its one active
            // handoff per family. Retain the family guard through completion,
            // including cancellation of the awaiting transport task.
            let _queue_guard = queue_guard;
            let _permit = if domain == DispatchDomain::Queue {
                drop(permit);
                None
            } else {
                Some(permit)
            };
            blocking_router.route_to_domain(domain.as_str(), envelope)
        })
        .await
        {
            Ok(result) => result,
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    error = %error,
                    "Ingress: blocking dispatch task failed after handoff"
                );
                // Handoff may have enqueued the command; this must not become
                // a retryable pre-enqueue rejection.
                Err(crate::runtime::router::RouteError::DeliveryFailed(
                    destination,
                    crate::runtime::DeliveryError::ActorStopped,
                ))
            }
        }
    }
}

impl DomainFrameDispatcher {
    /// Sinks that wait for a family-actor reply must not park a Tokio worker.
    /// Stream, Queue, and RPC always wait; Notice waits for every request
    /// except PUBLISH, which the sink accepts without a reply. Returns `None`
    /// for deliveries that are safe to route inline.
    fn blocking_handoff_permits(
        &self,
        domain: DispatchDomain,
        envelope: &crate::runtime::Envelope,
    ) -> Option<&std::sync::Arc<tokio::sync::Semaphore>> {
        match domain {
            DispatchDomain::Stream => Some(&self.stream_dispatch_permits),
            DispatchDomain::Queue => Some(&self.queue_dispatch_permits),
            DispatchDomain::Rpc => Some(&self.rpc_dispatch_permits),
            DispatchDomain::Notice
                if !crate::domains::notice::NoticeDomain::can_accept_without_reply(envelope) =>
            {
                Some(&self.notice_dispatch_permits)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "tests/queue_handoff_serialization.rs"]
mod tests;
