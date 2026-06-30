use super::{CloseReason, DispatchDomain, Ingress, RuntimeIngress};
use std::borrow::Cow;

impl RuntimeIngress {
    pub async fn close_all_sessions(&self, reason: CloseReason) {
        let session_ids = self
            .session_registry()
            .active_sessions()
            .into_iter()
            .map(|session| session.session_id)
            .collect::<Vec<_>>();
        for session_id in session_ids {
            self.on_close(session_id, reason.clone()).await;
        }
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
        match domain {
            DispatchDomain::Kv => Self::canonicalize_triplet_route_str(domain, route, true),
            DispatchDomain::Queue | DispatchDomain::Lease | DispatchDomain::Stream => {
                Self::canonicalize_triplet_route_str(domain, route, false)
            }
            DispatchDomain::Rpc | DispatchDomain::Notice | DispatchDomain::Schedule => {
                Ok(Self::scheme_prefixed_route_str(domain.as_str(), route))
            }
        }
    }

    pub(super) fn scheme_prefixed_route_str<'a>(domain: &str, route: &'a str) -> Cow<'a, str> {
        if route.contains("://") {
            Cow::Borrowed(route)
        } else {
            let trimmed = route.trim_start_matches('/');
            let mut canonical = String::with_capacity(domain.len() + 3 + trimmed.len());
            canonical.push_str(domain);
            canonical.push_str("://");
            canonical.push_str(trimmed);
            Cow::Owned(canonical)
        }
    }

    pub(super) fn canonicalize_triplet_route_str(
        domain: DispatchDomain,
        route: &str,
        exact: bool,
    ) -> Result<Cow<'_, str>, String> {
        let parts = if exact {
            crate::runtime::routing::route_exact_triplet(route)
        } else {
            crate::runtime::routing::route_triplet(route)
        }
        .ok_or_else(|| {
            format!(
                "{} route must be realm/area/resource{}",
                domain.as_str(),
                if exact { "" } else { " or deeper" }
            )
        })?;

        if parts.realm.is_empty() || parts.area.is_empty() || parts.resource.is_empty() {
            return Err(format!(
                "{} route must include non-empty realm/area/resource",
                domain.as_str()
            ));
        }

        let domain_name = domain.as_str();
        let mut canonical = String::with_capacity(
            domain_name.len() + 3 + parts.realm.len() + parts.area.len() + parts.resource.len() + 2,
        );
        canonical.push_str(domain_name);
        canonical.push_str("://");
        canonical.push_str(parts.realm);
        canonical.push('/');
        canonical.push_str(parts.area);
        canonical.push('/');
        canonical.push_str(parts.resource);

        if route == canonical {
            Ok(Cow::Borrowed(route))
        } else {
            Ok(Cow::Owned(canonical))
        }
    }
}
