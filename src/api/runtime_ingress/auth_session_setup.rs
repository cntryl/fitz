use super::{CloseReason, DispatchDomain, Ingress, RuntimeIngress};
use futures_util::StreamExt;
use std::borrow::Cow;
use std::sync::atomic::Ordering;

const SESSION_CLOSE_CONCURRENCY: usize = 32;

impl RuntimeIngress {
    pub async fn close_all_sessions(&self, reason: CloseReason) {
        self.registry
            .accepting_sessions
            .store(false, Ordering::Release);
        let session_ids = self
            .session_registry()
            .active_sessions()
            .into_iter()
            .map(|session| session.session_id)
            .collect::<Vec<_>>();
        futures_util::stream::iter(session_ids)
            .for_each_concurrent(SESSION_CLOSE_CONCURRENCY, |session_id| {
                self.on_close(session_id, reason.clone())
            })
            .await;
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn canonicalize_domain_route(
        domain: DispatchDomain,
        route: &crate::runtime::routing::Route,
    ) -> Result<crate::runtime::routing::Route, String> {
        Self::canonicalize_domain_route_str(domain, route.as_str())
            .map(|route| crate::runtime::routing::Route::new(route.as_ref()))
    }

    pub(super) fn canonicalize_domain_route_str(
        domain: DispatchDomain,
        route: &str,
    ) -> Result<Cow<'_, str>, String> {
        let descriptor = super::domain_registry::IngressDomainPolicy::descriptor_for_domain(domain);
        crate::protocol::auth_route::canonicalize(domain.as_str(), route, |route| {
            descriptor.canonical_auth_route(route)
        })
    }
}
