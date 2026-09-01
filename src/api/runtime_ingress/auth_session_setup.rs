use super::{CloseReason, DispatchDomain, Ingress, RuntimeIngress};
use futures_util::StreamExt;
use std::borrow::Cow;
use std::sync::atomic::Ordering;

const SESSION_CLOSE_CONCURRENCY: usize = 32;

impl RuntimeIngress {
    pub async fn close_all_sessions(&self, reason: CloseReason) {
        self.accepting_sessions.store(false, Ordering::Release);
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
        crate::utils::route_shape::validate_route_shape(route)?;
        Self::validate_qualified_domain_scheme(domain, route)?;

        let canonical = match domain {
            DispatchDomain::Kv => Self::canonicalize_triplet_route_str(domain, route, true),
            DispatchDomain::Queue => Self::canonicalize_triplet_route_str(domain, route, false),
            DispatchDomain::Lease => Self::canonicalize_lease_route_str(route),
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

    /// Canonicalize a Lease route for authorization.
    ///
    /// Lease carries two route shapes that must NOT canonicalize the same
    /// way:
    ///
    /// - **Concrete routes** (`ACQUIRE`/`EXTEND`/`RELEASE`/`QUERY`) never
    ///   contain a wildcard segment; they must be exactly three non-empty
    ///   segments, matching `LeaseKey::from_route_str`'s own exact-only
    ///   parsing. Routing them through the truncating (non-exact) triplet
    ///   parser would let an over-long route authorize under a silently
    ///   shortened identity that the sink then rejects outright.
    ///
    /// - **Selectors** (`SUBSCRIBE`/`UNSUBSCRIBE`/`LIST`) accept the shared
    ///   depth-three `*`/`**` grammar (routing-design.md §4), which lets a
    ///   selector resolve to fewer than three raw segments (`lease://**`) or
    ///   more than three when `**` collapses several (`lease://x/**/y/z`).
    ///   The generic (non-exact) triplet parser requires at least three raw
    ///   segments to succeed at all and truncates anything past the third,
    ///   so it would reject the short forms and silently narrow the long
    ///   ones to a different selector than the one the sink actually
    ///   matches against — authorizing a pattern the sink never sees.
    ///   Selectors are therefore scheme-qualified without truncation;
    ///   `compile_registration_pattern` (called separately by
    ///   `extract_auth_route_for_domain`) performs the actual shape and
    ///   depth validation against the same grammar the sink uses, so
    ///   authorization and the sink cannot accept different pattern
    ///   languages.
    ///
    /// Splitting on the presence of a wildcard (mirroring
    /// `canonicalize_stream_route_str`) is safe because exact-route Lease
    /// operations never carry one: the wire parser rejects any `*`/`**`
    /// segment for `ACQUIRE`/`EXTEND`/`RELEASE`/`QUERY` before this is ever
    /// reached.
    fn canonicalize_lease_route_str(route: &str) -> Result<Cow<'_, str>, String> {
        if route.contains('*') {
            return Ok(Self::scheme_prefixed_route_str(
                DispatchDomain::Lease.as_str(),
                route,
            ));
        }

        Self::canonicalize_triplet_route_str(DispatchDomain::Lease, route, true)
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
