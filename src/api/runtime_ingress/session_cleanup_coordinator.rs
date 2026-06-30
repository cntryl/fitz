use super::{dispatch_session_cleanup, obs, DispatchDomain, PendingSessionCleanup, RuntimeIngress};
use std::sync::atomic::Ordering;

pub(super) struct SessionCleanupCoordinator<'a> {
    ingress: &'a RuntimeIngress,
}

impl RuntimeIngress {
    pub(super) fn session_cleanup_coordinator(&self) -> SessionCleanupCoordinator<'_> {
        SessionCleanupCoordinator { ingress: self }
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

        if store_retry_ticket {
            self.ingress
                .pending_session_cleanups
                .insert(session_id, PendingSessionCleanup { route_family });
        }

        tracing::warn!(
            session_id = session_id,
            route_family = route_family.id(),
            failed_domains = ?failed_domains,
            retry_pending = store_retry_ticket,
            "Ingress: session cleanup incomplete"
        );
    }

    pub(super) async fn retry_pending(&self) {
        if self.ingress.pending_session_cleanups.is_empty() {
            return;
        }

        let Some(router) = &self.ingress.router else {
            return;
        };

        if self
            .ingress
            .cleanup_retry_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let pending = self
            .ingress
            .pending_session_cleanups
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect::<Vec<_>>();
        let pending_len = pending.len();
        let router = router.clone();

        let retry_result = tokio::task::spawn_blocking(move || {
            pending
                .into_iter()
                .map(|(session_id, cleanup)| {
                    (
                        session_id,
                        cleanup.route_family,
                        dispatch_session_cleanup(router.as_ref(), cleanup.route_family, session_id),
                    )
                })
                .collect::<Vec<_>>()
        })
        .await;

        self.ingress
            .cleanup_retry_in_progress
            .store(false, Ordering::SeqCst);

        match retry_result {
            Ok(retry_outcomes) => {
                for (session_id, route_family, failed_domains) in retry_outcomes {
                    if failed_domains.is_empty() {
                        self.ingress.pending_session_cleanups.remove(&session_id);
                        tracing::debug!(
                            session_id = session_id,
                            route_family = route_family.id(),
                            "Ingress: pending session cleanup retry succeeded"
                        );
                    } else {
                        self.record_failure(session_id, route_family, &failed_domains, true);
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    pending_sessions = pending_len,
                    "Ingress: pending cleanup retry worker task failed"
                );
            }
        }
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
                        "Ingress: dispatched cleanup to KV, Notice, RPC, Stream, Schedule, Lease, and Queue domains"
                    );
                } else {
                    self.record_failure(session_id, route_family, &failed_domains, true);
                }
            }
            Err(error) => {
                self.ingress
                    .pending_session_cleanups
                    .insert(session_id, PendingSessionCleanup { route_family });
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
