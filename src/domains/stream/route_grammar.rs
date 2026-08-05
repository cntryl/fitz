//! Shared route-shape grammar for stream READ and SUBSCRIBE.
//!
//! Both operations accept exactly the same selector matrix (routing-design.md
//! §8.1) so that every pattern a client can subscribe to has an equivalent
//! read selector, and vice versa: the 8 literal-or-`*` combinations of
//! `stream://{realm}/{area}/{resource}` plus the two canonical `**` aliases
//! (`{realm}/**` for `{realm}/*/*`, and `stream://**` for `*/*/*`).
//!
//! Noncanonical spellings that would select the same match set as one of the
//! 10 blessed shapes (e.g. `stream://acme/*/**`, `stream://acme/events/**`,
//! `stream://**/orders`) are rejected to keep the public vocabulary finite,
//! per §8.1.

use crate::runtime::routing::route_exact_triplet;

/// The 10 route shapes shared by stream READ and SUBSCRIBE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamRouteShape<'a> {
    Resource {
        realm: &'a str,
        area: &'a str,
        resource: &'a str,
    },
    Area {
        realm: &'a str,
        area: &'a str,
    },
    /// `stream://{realm}/*/{resource}` — realm-order scan filtered to one
    /// resource name across every area in the realm.
    RealmFilterResource {
        realm: &'a str,
        resource: &'a str,
    },
    /// `stream://{realm}/*/*` or its canonical alias `stream://{realm}/**`.
    Realm {
        realm: &'a str,
    },
    /// `stream://*/{area}/{resource}` — global-order scan filtered to one
    /// area/resource pair across every realm.
    GlobalFilterAreaResource {
        area: &'a str,
        resource: &'a str,
    },
    /// `stream://*/{area}/*` — global-order scan filtered to one area name
    /// across every realm.
    GlobalFilterArea {
        area: &'a str,
    },
    /// `stream://*/*/{resource}` — global-order scan filtered to one
    /// resource name across every realm and area.
    GlobalFilterResource {
        resource: &'a str,
    },
    /// `stream://*/*/*` or its canonical alias `stream://**`.
    Global,
}

pub(crate) const STREAM_ROUTE_SHAPE_ERROR: &str =
    "stream route selector must be one of the 10 shapes in routing-design.md \
     §8.1: realm/area/resource, realm/area/*, realm/*/resource, realm/*/* \
     (or realm/**), */area/resource, */area/*, */*/resource, or */*/* (or \
     stream://**)";

/// Classify a stream route into one of the 10 supported §8.1 shapes.
///
/// # Errors
///
/// Returns [`STREAM_ROUTE_SHAPE_ERROR`] when the route does not match one of
/// the 10 blessed shapes (including malformed routes, wrong segment counts,
/// noncanonical `**` placement, or partial-segment wildcards).
pub(crate) fn classify_stream_route_shape(route: &str) -> Result<StreamRouteShape<'_>, String> {
    // Stream selectors are deliberately stricter than the runtime's generic
    // route helpers: accepting a missing/wrong scheme or empty path segment
    // here would make authorization and dispatch disagree about the route's
    // identity. Keep these limits in the shared parser so every caller gets
    // the same validation.
    const MAX_ROUTE_BYTES: usize = 512;
    if route.len() > MAX_ROUTE_BYTES
        || !route.starts_with("stream://")
        || route[9..].is_empty()
        || route[9..].starts_with('/')
        || route[9..].contains("//")
    {
        return Err(STREAM_ROUTE_SHAPE_ERROR.to_string());
    }
    let path = &route[9..];
    if path.split('/').any(str::is_empty) {
        return Err(STREAM_ROUTE_SHAPE_ERROR.to_string());
    }
    if route == "stream://**" {
        return Ok(StreamRouteShape::Global);
    }

    // The only other accepted `**` spelling is the two-segment realm alias
    // `{realm}/**`. Any other appearance of `**` (e.g. `*/**`,
    // `{realm}/{area}/**`, `**/{resource}`, `{realm}/**/{resource}`,
    // `{realm}/**/*`) is a noncanonical alias for a shape already reachable
    // without `**` and is rejected.
    let mut segments = path.split('/');
    if let (Some(realm), Some("**"), None) =
        (segments.next(), segments.next(), segments.clone().next())
    {
        return if realm.contains('*') {
            Err(STREAM_ROUTE_SHAPE_ERROR.to_string())
        } else {
            Ok(StreamRouteShape::Realm { realm })
        };
    }
    if path.contains("**") {
        return Err(STREAM_ROUTE_SHAPE_ERROR.to_string());
    }

    let parts = route_exact_triplet(route).ok_or_else(|| STREAM_ROUTE_SHAPE_ERROR.to_string())?;
    let is_wild = |segment: &str| segment == "*";
    let is_literal = |segment: &str| !segment.contains('*');
    match (parts.realm, parts.area, parts.resource) {
        (realm, area, resource)
            if is_literal(realm) && is_literal(area) && is_literal(resource) =>
        {
            Ok(StreamRouteShape::Resource {
                realm,
                area,
                resource,
            })
        }
        (realm, area, resource) if is_literal(realm) && is_literal(area) && is_wild(resource) => {
            Ok(StreamRouteShape::Area { realm, area })
        }
        (realm, area, resource) if is_literal(realm) && is_wild(area) && is_literal(resource) => {
            Ok(StreamRouteShape::RealmFilterResource { realm, resource })
        }
        (realm, area, resource) if is_literal(realm) && is_wild(area) && is_wild(resource) => {
            Ok(StreamRouteShape::Realm { realm })
        }
        (realm, area, resource) if is_wild(realm) && is_literal(area) && is_literal(resource) => {
            Ok(StreamRouteShape::GlobalFilterAreaResource { area, resource })
        }
        (realm, area, resource) if is_wild(realm) && is_literal(area) && is_wild(resource) => {
            Ok(StreamRouteShape::GlobalFilterArea { area })
        }
        (realm, area, resource) if is_wild(realm) && is_wild(area) && is_literal(resource) => {
            Ok(StreamRouteShape::GlobalFilterResource { resource })
        }
        (realm, area, resource) if is_wild(realm) && is_wild(area) && is_wild(resource) => {
            Ok(StreamRouteShape::Global)
        }
        _ => Err(STREAM_ROUTE_SHAPE_ERROR.to_string()),
    }
}

impl StreamRouteShape<'_> {
    /// Canonical printable form used for cursor fingerprinting
    /// (routing-design.md §11.2): the two `**` aliases fold to their
    /// expanded literal-or-`*` spelling so a cursor issued for
    /// `stream://{realm}/**` validates identically against a resumed
    /// `stream://{realm}/*/*` request, matching §4's alias-equivalence
    /// requirement for authorization.
    pub(crate) fn canonical(&self) -> String {
        match *self {
            Self::Resource {
                realm,
                area,
                resource,
            } => format!("{realm}/{area}/{resource}"),
            Self::Area { realm, area } => format!("{realm}/{area}/*"),
            Self::RealmFilterResource { realm, resource } => format!("{realm}/*/{resource}"),
            Self::Realm { realm } => format!("{realm}/*/*"),
            Self::GlobalFilterAreaResource { area, resource } => format!("*/{area}/{resource}"),
            Self::GlobalFilterArea { area } => format!("*/{area}/*"),
            Self::GlobalFilterResource { resource } => format!("*/*/{resource}"),
            Self::Global => "*/*/*".to_string(),
        }
    }
}

/// Fingerprint binding a read cursor to the route family, canonical
/// selector, and filter that produced it (routing-design.md §11.2). Not a
/// stable cross-version wire format. FNV-1a keeps the input mapping explicit
/// and deterministic; the per-process HMAC in the cursor token supplies the
/// collision and tamper resistance required at the protocol boundary.
pub(crate) fn cursor_fingerprint(
    family: crate::runtime::routing::RouteFamily,
    shape: &StreamRouteShape<'_>,
    filter: Option<&crate::domains::stream::protocol::StreamFilterSet>,
) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut digest = FNV_OFFSET_BASIS;
    for byte in family
        .as_u64()
        .to_le_bytes()
        .into_iter()
        .chain(shape.canonical().bytes())
        .chain([0xff])
        .chain(
            filter
                .map(crate::domains::stream::protocol::StreamFilterSet::encode)
                .unwrap_or_default(),
        )
    {
        digest ^= u64::from(byte);
        digest = digest.wrapping_mul(FNV_PRIME);
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_classify_resource_shape() {
        assert_eq!(
            classify_stream_route_shape("stream://bench/events/orders"),
            Ok(StreamRouteShape::Resource {
                realm: "bench",
                area: "events",
                resource: "orders",
            })
        );
    }

    #[test]
    fn should_classify_area_shape() {
        assert_eq!(
            classify_stream_route_shape("stream://bench/events/*"),
            Ok(StreamRouteShape::Area {
                realm: "bench",
                area: "events",
            })
        );
    }

    #[test]
    fn should_reject_missing_or_wrong_scheme() {
        assert!(classify_stream_route_shape("bench/events/orders").is_err());
        assert!(classify_stream_route_shape("other://bench/events/orders").is_err());
    }

    #[test]
    fn should_reject_empty_segments_and_doubled_slashes() {
        assert!(classify_stream_route_shape("stream://bench//orders").is_err());
        assert!(classify_stream_route_shape("stream://bench/events/").is_err());
        assert!(classify_stream_route_shape("stream:///events/orders").is_err());
    }

    #[test]
    fn should_classify_realm_shape() {
        assert_eq!(
            classify_stream_route_shape("stream://bench/*/*"),
            Ok(StreamRouteShape::Realm { realm: "bench" })
        );
    }

    #[test]
    fn should_classify_global_shape() {
        assert_eq!(
            classify_stream_route_shape("stream://**"),
            Ok(StreamRouteShape::Global)
        );
        assert_eq!(
            classify_stream_route_shape("stream://*/*/*"),
            Ok(StreamRouteShape::Global)
        );
    }

    #[test]
    fn should_classify_realm_filter_resource_shape() {
        assert_eq!(
            classify_stream_route_shape("stream://bench/*/orders"),
            Ok(StreamRouteShape::RealmFilterResource {
                realm: "bench",
                resource: "orders",
            })
        );
    }

    #[test]
    fn should_classify_realm_shape_via_double_star_alias() {
        assert_eq!(
            classify_stream_route_shape("stream://bench/**"),
            Ok(StreamRouteShape::Realm { realm: "bench" })
        );
    }

    #[test]
    fn should_classify_global_filter_shapes() {
        assert_eq!(
            classify_stream_route_shape("stream://*/events/orders"),
            Ok(StreamRouteShape::GlobalFilterAreaResource {
                area: "events",
                resource: "orders",
            })
        );
        assert_eq!(
            classify_stream_route_shape("stream://*/events/*"),
            Ok(StreamRouteShape::GlobalFilterArea { area: "events" })
        );
        assert_eq!(
            classify_stream_route_shape("stream://*/*/orders"),
            Ok(StreamRouteShape::GlobalFilterResource { resource: "orders" })
        );
    }

    /// SUBSCRIBE compiles the same accepted shapes through the shared
    /// generic wildcard matcher (`compile_stream_subscription_pattern` in
    /// `mailbox_sink_impl.rs`), so the delivery-matching behavior for every
    /// newly accepted shape is exercised here directly against that matcher,
    /// independent of the full actor/commit pipeline.
    #[test]
    fn should_match_new_shapes_through_shared_subscription_matcher() {
        use crate::runtime::matcher::{compile_registration_pattern, PatternDepth};

        let cases: &[(&str, &str, bool)] = &[
            (
                "stream://bench/*/orders",
                "stream://bench/events/orders",
                true,
            ),
            (
                "stream://bench/*/orders",
                "stream://bench/events/audits",
                false,
            ),
            (
                "stream://bench/*/orders",
                "stream://other/events/orders",
                false,
            ),
            ("stream://bench/**", "stream://bench/events/orders", true),
            ("stream://bench/**", "stream://other/events/orders", false),
            ("stream://*/events/*", "stream://bench/events/orders", true),
            ("stream://*/events/*", "stream://bench/audit/orders", false),
            (
                "stream://*/events/orders",
                "stream://bench/events/orders",
                true,
            ),
            (
                "stream://*/events/orders",
                "stream://bench/events/audits",
                false,
            ),
            ("stream://*/*/orders", "stream://bench/events/orders", true),
            ("stream://*/*/orders", "stream://zeta/audit/orders", true),
            ("stream://*/*/orders", "stream://zeta/audit/audits", false),
        ];

        for (pattern, route, expected) in cases {
            assert!(
                classify_stream_route_shape(pattern).is_ok(),
                "expected {pattern} to be a valid selector"
            );
            let compiled = compile_registration_pattern(pattern, "stream", PatternDepth::Flexible)
                .unwrap_or_else(|error| panic!("expected {pattern} to compile: {error}"));
            assert_eq!(
                compiled.matches_str(route),
                *expected,
                "expected {pattern} matching {route} to be {expected}"
            );
        }
    }

    #[test]
    fn should_reject_partial_and_noncanonical_shapes() {
        let selectors = [
            "stream://bench/**/*",
            "stream://bench/events/**",
            "stream://*/**",
            "stream://**/orders",
            "stream://bench/**/orders",
            "stream://*/**/*",
            "stream://bench/events",
            "stream://bench/events/orders/extra",
            "stream://bench/event*/orders",
            "stream://bench/*event/orders",
        ];
        for selector in selectors {
            assert_eq!(
                classify_stream_route_shape(selector),
                Err(STREAM_ROUTE_SHAPE_ERROR.to_string()),
                "expected {selector} to be rejected"
            );
        }
    }
}
