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
        let closes = session_ids
            .into_iter()
            .map(|session_id| self.on_close(session_id, reason.clone()));
        futures::future::join_all(closes).await;
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
        crate::utils::route_shape::validate_route_shape(route)?;
        Self::validate_qualified_domain_scheme(domain, route)?;

        let canonical = match domain {
            DispatchDomain::Kv => Self::canonicalize_triplet_route_str(domain, route, true),
            DispatchDomain::Queue | DispatchDomain::Lease => {
                Self::canonicalize_triplet_route_str(domain, route, false)
            }
            DispatchDomain::Stream => Self::canonicalize_stream_route_str(route),
            DispatchDomain::Rpc | DispatchDomain::Notice | DispatchDomain::Schedule => {
                Ok(Self::scheme_prefixed_route_str(domain.as_str(), route))
            }
        }?;
        crate::utils::route_shape::validate_route_shape(canonical.as_ref())?;
        Ok(canonical)
    }

    /// Canonicalize a Stream route for authorization.
    ///
    /// Stream carries two route shapes, and they canonicalize differently:
    ///
    /// - **Selectors** (any route bearing a wildcard, used by `READ`, `LAST`,
    ///   `GET_METADATA`, and `SUBSCRIBE`) must be one of the 10 shapes in
    ///   `routing-design.md` §8.1, and fold to their expanded literal-or-`*`
    ///   spelling. That implements the §11.2 alias rule: `stream://acme/**` and
    ///   `stream://acme/*/*` are the same selector, so they must authorize
    ///   against the same concrete-route language instead of being compared as
    ///   two unrelated wildcard patterns. Routing them through the generic
    ///   triplet parser instead rejected both `**` aliases outright, because
    ///   they carry fewer than three segments.
    ///
    /// - **Concrete routes** (`BEGIN` addresses
    ///   `{realm}/{area}/{resource}/{operation}`) authorize against their
    ///   resource identity, so the trailing operation segment is dropped.
    ///
    /// Splitting on the wildcard keeps noncanonical spellings such as
    /// `stream://acme/**/orders` out of the selector path, where the generic
    /// depth check would otherwise accept a shape the domain sink rejects.
    fn canonicalize_stream_route_str(route: &str) -> Result<Cow<'_, str>, String> {
        if !route.contains('*') {
            return Self::canonicalize_triplet_route_str(DispatchDomain::Stream, route, false);
        }

        let scheme_qualified =
            Self::scheme_prefixed_route_str(DispatchDomain::Stream.as_str(), route);
        let shape = crate::domains::stream::route_grammar::classify_stream_route_shape(
            scheme_qualified.as_ref(),
        )?;
        Ok(Cow::Owned(format!("stream://{}", shape.canonical())))
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
