use super::{
    dispatch_session_cleanup, dispatch_session_cleanup_for_domains, obs, DispatchDomain,
    PendingSessionCleanup, RuntimeIngress,
};
use std::sync::atomic::Ordering;
use std::time::Duration;

const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(10);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(1);
/// Give up on a cleanup ticket once it has been pending this long instead of
/// retrying forever. A domain actor that has permanently failed (see
/// `ManagedActor`'s fail-closed supervision) can never accept a cleanup
/// command again, so retrying indefinitely would leave the pending-cleanup
/// gauge and oldest-age metric growing without bound instead of surfacing a
/// terminal failure an operator can act on.
///
/// This is measured per ticket against its own age, not as an attempt count.
/// The backoff above is worker-global and is reset to its 10ms floor whenever
/// any *other* ticket in the batch succeeds, so under normal session churn an
/// attempt counter does not track elapsed time at all - eight attempts can
/// burn in under 100ms, abandoning the subscriptions and inflight leases of a
/// session whose actor was merely busy.
const MAX_CLEANUP_RETRY_WINDOW: Duration = Duration::from_millis(2_300);

pub(super) struct SessionCleanupCoordinator<'a> {
    ingress: &'a RuntimeIngress,
}

impl RuntimeIngress {
    pub(super) fn session_cleanup_coordinator(&self) -> SessionCleanupCoordinator<'_> {
        SessionCleanupCoordinator { ingress: self }
    }

    fn ensure_cleanup_worker(&self) {
        let Some(router) = self.router.clone() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_err()
            || self
                .cleanup_worker_started
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }

        let pending = self.pending_session_cleanups.clone();
        let wake = self.cleanup_wake.clone();
        let shutdown = self.cleanup_shutdown.clone();
        tokio::spawn(async move {
            run_cleanup_worker(pending, router, wake, shutdown).await;
        });
    }

    /// Drain cleanup tickets during broker shutdown. The worker keeps retrying
    /// only the domains that have not accepted a cleanup yet.
    pub(super) async fn drain_cleanup_tickets(&self) {
        self.ensure_cleanup_worker();
        self.cleanup_shutdown.store(true, Ordering::Release);
        self.cleanup_wake.notify_one();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !self.pending_session_cleanups.is_empty() {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    pending_sessions = self.pending_session_cleanups.len(),
                    "session cleanup drain timed out"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl SessionCleanupCoordinator<'_> {
    pub(super) fn record_failure(
        &self,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        failed_domains: &[DispatchDomain],
        store_retry_ticket: bool,
    ) {
        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.counter_add(
                obs::METRIC_SESSION_CLEANUP_FAILURES,
                failed_domains.len() as u64,
            );
        }

        if store_retry_ticket && !failed_domains.is_empty() {
            let failed_domains = failed_domains.to_vec();
            if let Some(mut ticket) = self.ingress.pending_session_cleanups.get_mut(&session_id) {
                ticket.route_family = route_family;
                ticket.pending_domains = failed_domains;
            } else {
                self.ingress.pending_session_cleanups.insert(
                    session_id,
                    PendingSessionCleanup {
                        route_family,
                        pending_domains: failed_domains,
                        created_at: std::time::Instant::now(),
                        attempts: 0,
                    },
                );
            }
            self.ingress.ensure_cleanup_worker();
            self.ingress.cleanup_wake.notify_one();
            update_cleanup_gauges(&self.ingress.pending_session_cleanups);
        }

        tracing::warn!(
            session_id = session_id,
            route_family = route_family.id(),
            failed_domains = ?failed_domains,
            retry_pending = store_retry_ticket && !failed_domains.is_empty(),
            "Ingress: session cleanup incomplete"
        );
    }

    pub(super) async fn cleanup_on_close(
        &self,
        session_id: u64,
        route_family: Option<crate::runtime::routing::RouteFamily>,
    ) {
        let (Some(router), Some(route_family)) = (&self.ingress.router, route_family) else {
            return;
        };

        let router = router.clone();
        match tokio::task::spawn_blocking(move || {
            dispatch_session_cleanup(router.as_ref(), route_family, session_id)
        })
        .await
        {
            Ok(failed_domains) => {
                if failed_domains.is_empty() {
                    tracing::debug!(
                        session_id = session_id,
                        route_family = route_family.id(),
                        "Ingress: dispatched session cleanup to every domain"
                    );
                } else {
                    self.record_failure(session_id, route_family, &failed_domains, true);
                }
            }
            Err(error) => {
                self.record_failure(
                    session_id,
                    route_family,
                    crate::runtime::DomainRegistry::cleanup_order(),
                    true,
                );
                tracing::warn!(
                    session_id = session_id,
                    route_family = route_family.id(),
                    error = %error,
                    "Ingress: cleanup worker task failed"
                );
            }
        }
    }
}

async fn run_cleanup_worker(
    pending: std::sync::Arc<dashmap::DashMap<u64, PendingSessionCleanup>>,
    router: std::sync::Arc<crate::runtime::Router>,
    wake: std::sync::Arc<tokio::sync::Notify>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    let mut retry_delay = INITIAL_RETRY_DELAY;

    loop {
        if pending.is_empty() {
            if shutdown.load(Ordering::Acquire) {
                break;
            }
            wake.notified().await;
            continue;
        }

        let snapshot = pending
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect::<Vec<_>>();
        let retry_router = router.clone();
        let outcomes = tokio::task::spawn_blocking(move || {
            snapshot
                .into_iter()
                .map(|(session_id, ticket)| {
                    let failed = dispatch_session_cleanup_for_domains(
                        retry_router.as_ref(),
                        ticket.route_family,
                        session_id,
                        &ticket.pending_domains,
                    );
                    (session_id, ticket, failed)
                })
                .collect::<Vec<_>>()
        })
        .await;

        match outcomes {
            Ok(outcomes) => {
                let mut made_progress = false;
                for (session_id, ticket, failed_domains) in outcomes {
                    if failed_domains.is_empty() {
                        pending.remove(&session_id);
                        crate::observability::counter_inc(obs::METRIC_SESSION_CLEANUP_SUCCESSES);
                        made_progress = true;
                    } else if let Some(mut current) = pending.get_mut(&session_id) {
                        let attempts = ticket.attempts.saturating_add(1);
                        if ticket.created_at.elapsed() >= MAX_CLEANUP_RETRY_WINDOW {
                            drop(current);
                            pending.remove(&session_id);
                            crate::observability::counter_inc(
                                obs::METRIC_SESSION_CLEANUP_PERMANENT_FAILURES,
                            );
                            tracing::error!(
                                session_id = session_id,
                                attempts,
                                pending_ms = ticket.created_at.elapsed().as_millis(),
                                pending_domains = ?failed_domains,
                                "Ingress: session cleanup permanently failed after exhausting \
                                 its retry window"
                            );
                        } else {
                            current.pending_domains = failed_domains;
                            current.attempts = attempts;
                            crate::observability::counter_inc(obs::METRIC_SESSION_CLEANUP_RETRIES);
                        }
                    }
                }
                update_cleanup_gauges(&pending);
                retry_delay = if made_progress {
                    INITIAL_RETRY_DELAY
                } else {
                    retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY)
                };
            }
            Err(error) => {
                tracing::warn!(error = %error, "session cleanup retry worker failed");
                retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
            }
        }

        tokio::select! {
            () = wake.notified() => {}
            () = tokio::time::sleep(retry_delay) => {}
        }
    }
}

fn update_cleanup_gauges(pending: &dashmap::DashMap<u64, PendingSessionCleanup>) {
    crate::observability::gauge_set(obs::METRIC_SESSION_CLEANUP_PENDING, pending.len() as u64);
    let oldest_age_ms = pending
        .iter()
        .map(|entry| {
            entry
                .created_at
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX)
        })
        .max()
        .unwrap_or(0);
    crate::observability::gauge_set(obs::METRIC_SESSION_CLEANUP_OLDEST_AGE_MS, oldest_age_ms);
}
