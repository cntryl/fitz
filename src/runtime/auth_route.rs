//! Shared building blocks for domain-owned authorization route canonicalization.
//!
//! Each domain owns its auth-route grammar (`canonical_auth_route` in the
//! domain module); these helpers only implement the shapes several domains
//! share.

use std::borrow::Cow;

/// Qualify `route` with `scheme://` unless it already carries a scheme.
#[must_use]
pub(crate) fn scheme_prefixed_route<'a>(scheme: &str, route: &'a str) -> Cow<'a, str> {
    if route.contains("://") {
        Cow::Borrowed(route)
    } else {
        let trimmed = route.trim_start_matches('/');
        let mut canonical = String::with_capacity(scheme.len() + 3 + trimmed.len());
        canonical.push_str(scheme);
        canonical.push_str("://");
        canonical.push_str(trimmed);
        Cow::Owned(canonical)
    }
}

/// Canonicalize a `realm/area/resource` route under `scheme`.
///
/// With `exact`, the route must have exactly three segments; otherwise
/// segments past the third are dropped.
///
/// # Errors
///
/// Returns a message when the route lacks three non-empty segments (or has
/// more than three when `exact`).
pub(crate) fn canonical_triplet_route<'a>(
    scheme: &str,
    route: &'a str,
    exact: bool,
) -> Result<Cow<'a, str>, String> {
    let parts = if exact {
        crate::runtime::routing::route_exact_triplet(route)
    } else {
        crate::runtime::routing::route_triplet(route)
    }
    .ok_or_else(|| {
        format!(
            "{scheme} route must be realm/area/resource{}",
            if exact { "" } else { " or deeper" }
        )
    })?;

    if parts.realm.is_empty() || parts.area.is_empty() || parts.resource.is_empty() {
        return Err(format!(
            "{scheme} route must include non-empty realm/area/resource"
        ));
    }

    let mut canonical = String::with_capacity(
        scheme.len() + 3 + parts.realm.len() + parts.area.len() + parts.resource.len() + 2,
    );
    canonical.push_str(scheme);
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
