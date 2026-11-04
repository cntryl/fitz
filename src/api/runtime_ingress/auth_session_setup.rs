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
        Self::canonicalize_route_with(domain, route, |descriptor| {
            descriptor.canonical_auth_route(route)
        })
    }

    pub(super) fn canonicalize_operation_route_str(
        domain: DispatchDomain,
        message_type: u16,
        route: &str,
    ) -> Result<Cow<'_, str>, String> {
        Self::canonicalize_route_with(domain, route, |descriptor| {
            descriptor.canonical_operation_auth_route(message_type, route)
        })
    }

    fn canonicalize_route_with<'a>(
        domain: DispatchDomain,
        route: &'a str,
        canonicalize: impl FnOnce(
            &super::domain_registry::IngressDomainDescriptor,
        ) -> Result<Cow<'a, str>, String>,
    ) -> Result<Cow<'a, str>, String> {
        crate::utils::route_shape::validate_route_shape(route)?;
        Self::validate_qualified_domain_scheme(domain, route)?;

        let canonical = canonicalize(
            crate::api::runtime_ingress::domain_registry::IngressDomainPolicy::descriptor_for_domain(
                domain,
            ),
        )?;
        crate::utils::route_shape::validate_route_shape(canonical.as_ref())?;
        Ok(canonical)
    }

    fn validate_qualified_domain_scheme(domain: DispatchDomain, route: &str) -> Result<(), String> {
        let Some((scheme, _)) = route.split_once("://") else {
            return Ok(());
        };

        if scheme == domain.as_str() {
            return Ok(());
        }

        Err(format!(
            "{} message route must use {}://, not {}://",
            domain.as_str(),
            domain.as_str(),
            scheme
        ))
    }
}
