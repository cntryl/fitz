//! Shared domain inventory used by boot registration and ingress cleanup.

use crate::runtime::matcher::PatternDepth;
use crate::runtime::routing::Route;
use crate::runtime::{MailboxSink, Router};
use once_cell::sync::Lazy;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DomainKind {
    Kv,
    Queue,
    Rpc,
    Lease,
    Notice,
    Stream,
    Schedule,
}

impl DomainKind {
    pub const ALL: [Self; 7] = [
        Self::Kv,
        Self::Queue,
        Self::Notice,
        Self::Stream,
        Self::Rpc,
        Self::Lease,
        Self::Schedule,
    ];

    pub const SESSION_CLEANUP_ORDER: [Self; 7] = [
        Self::Kv,
        Self::Notice,
        Self::Rpc,
        Self::Stream,
        Self::Schedule,
        Self::Lease,
        Self::Queue,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.descriptor().scheme
    }

    #[must_use]
    pub const fn wildcard_route(self) -> &'static str {
        self.descriptor().wildcard_route
    }

    #[must_use]
    pub fn cleanup_route(self) -> Route {
        self.descriptor().cleanup_route().clone()
    }

    #[must_use]
    pub fn inbound_route(self) -> &'static Route {
        self.descriptor().inbound_route()
    }

    #[must_use]
    pub const fn descriptor(self) -> &'static DomainDescriptor {
        match self {
            Self::Kv => &DOMAIN_DESCRIPTORS[0],
            Self::Queue => &DOMAIN_DESCRIPTORS[1],
            Self::Notice => &DOMAIN_DESCRIPTORS[2],
            Self::Stream => &DOMAIN_DESCRIPTORS[3],
            Self::Rpc => &DOMAIN_DESCRIPTORS[4],
            Self::Lease => &DOMAIN_DESCRIPTORS[5],
            Self::Schedule => &DOMAIN_DESCRIPTORS[6],
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DomainDescriptor {
    pub kind: DomainKind,
    pub scheme: &'static str,
    inbound_route: fn() -> &'static Route,
    cleanup_route: fn() -> &'static Route,
    pub wildcard_route: &'static str,
    /// Concrete-route depth every retained registration pattern must be able to
    /// match. This is the domain's routing physics, so it lives here rather
    /// than being restated at each ingress and sink call site.
    pub registration_depth: PatternDepth,
    /// Whether the domain retains wildcard registrations at all. Every
    /// current domain, including Lease, accepts the shared depth-appropriate
    /// wildcard grammar for `LIST`/`SUBSCRIBE`/`UNSUBSCRIBE`; exact mutation
    /// operations (Lease `ACQUIRE`/`EXTEND`/`RELEASE`) still reject wildcards
    /// regardless of this flag (architectural-laws.md).
    pub wildcard_registrations_allowed: bool,
}

impl DomainDescriptor {
    #[must_use]
    pub fn inbound_route(&self) -> &'static Route {
        (self.inbound_route)()
    }

    #[must_use]
    pub fn cleanup_route(&self) -> &'static Route {
        (self.cleanup_route)()
    }

    /// Compile and validate a registration pattern under this domain's rules.
    ///
    /// Ingress authorization and the domain sink must accept exactly the same
    /// pattern language: routing-design.md §4 calls a pattern that one layer
    /// accepts and another interprets differently a correctness defect. Both
    /// call this, so the scheme, depth, and wildcard rules cannot drift.
    ///
    /// Domains with a narrower selector vocabulary than the generic grammar
    /// (Stream) still apply their own grammar afterwards; this is the shared
    /// floor, not the whole contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the pattern violates the shared route grammar,
    /// cannot match this domain's concrete depth, or uses a wildcard in a
    /// domain that retains only exact registrations.
    pub fn compile_registration_pattern(
        &self,
        route: &str,
    ) -> Result<crate::runtime::matcher::Pattern, String> {
        let pattern = crate::runtime::matcher::compile_registration_pattern(
            route,
            self.scheme,
            self.registration_depth,
        )?;
        if pattern.is_wildcard() && !self.wildcard_registrations_allowed {
            return Err(format!(
                "{} registrations require an exact route",
                self.scheme
            ));
        }
        Ok(pattern)
    }

    pub fn register_sink(&self, router: &Router, sink: Arc<dyn MailboxSink>) {
        router.register_domain_pattern(self.scheme, sink);
    }
}

pub struct DomainRegistry;

impl DomainRegistry {
    #[must_use]
    pub const fn all() -> &'static [DomainDescriptor; 7] {
        &DOMAIN_DESCRIPTORS
    }

    #[must_use]
    pub const fn cleanup_order() -> &'static [DomainKind; 7] {
        &DomainKind::SESSION_CLEANUP_ORDER
    }

    #[must_use]
    pub const fn descriptor(kind: DomainKind) -> &'static DomainDescriptor {
        kind.descriptor()
    }
}

pub const DOMAIN_DESCRIPTORS: [DomainDescriptor; 7] = [
    DomainDescriptor {
        kind: DomainKind::Kv,
        scheme: "kv",
        inbound_route: kv_inbound_route,
        cleanup_route: kv_cleanup_route,
        wildcard_route: "kv://**",
        registration_depth: PatternDepth::CanMatch(3),
        wildcard_registrations_allowed: true,
    },
    DomainDescriptor {
        kind: DomainKind::Queue,
        scheme: "queue",
        inbound_route: queue_inbound_route,
        cleanup_route: queue_cleanup_route,
        wildcard_route: "queue://**",
        registration_depth: PatternDepth::CanMatch(3),
        wildcard_registrations_allowed: true,
    },
    DomainDescriptor {
        kind: DomainKind::Notice,
        scheme: "notice",
        inbound_route: notice_inbound_route,
        cleanup_route: notice_cleanup_route,
        wildcard_route: "notice://**",
        registration_depth: PatternDepth::Flexible,
        wildcard_registrations_allowed: true,
    },
    DomainDescriptor {
        kind: DomainKind::Stream,
        scheme: "stream",
        inbound_route: stream_inbound_route,
        cleanup_route: stream_cleanup_route,
        wildcard_route: "stream://**",
        registration_depth: PatternDepth::CanMatch(3),
        wildcard_registrations_allowed: true,
    },
    DomainDescriptor {
        kind: DomainKind::Rpc,
        scheme: "rpc",
        inbound_route: rpc_inbound_route,
        cleanup_route: rpc_cleanup_route,
        wildcard_route: "rpc://**",
        registration_depth: PatternDepth::Flexible,
        wildcard_registrations_allowed: true,
    },
    DomainDescriptor {
        kind: DomainKind::Lease,
        scheme: "lease",
        inbound_route: lease_inbound_route,
        cleanup_route: lease_cleanup_route,
        wildcard_route: "lease://**",
        registration_depth: PatternDepth::CanMatch(3),
        wildcard_registrations_allowed: true,
    },
    DomainDescriptor {
        kind: DomainKind::Schedule,
        scheme: "schedule",
        inbound_route: schedule_inbound_route,
        cleanup_route: schedule_cleanup_route,
        wildcard_route: "schedule://**",
        registration_depth: PatternDepth::CanMatch(4),
        wildcard_registrations_allowed: true,
    },
];

fn kv_inbound_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("kv://inbound"));
    &ROUTE
}

fn queue_inbound_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("queue://inbound"));
    &ROUTE
}

fn notice_inbound_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("notice://inbound"));
    &ROUTE
}

fn stream_inbound_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("stream://inbound"));
    &ROUTE
}

fn rpc_inbound_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("rpc://inbound"));
    &ROUTE
}

fn lease_inbound_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("lease://inbound"));
    &ROUTE
}

fn schedule_inbound_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("schedule://inbound"));
    &ROUTE
}

fn kv_cleanup_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("kv://cleanup"));
    &ROUTE
}

fn queue_cleanup_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("queue://cleanup"));
    &ROUTE
}

fn notice_cleanup_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("notice://cleanup"));
    &ROUTE
}

fn stream_cleanup_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("stream://cleanup"));
    &ROUTE
}

fn rpc_cleanup_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("rpc://cleanup"));
    &ROUTE
}

fn lease_cleanup_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("lease://cleanup"));
    &ROUTE
}

fn schedule_cleanup_route() -> &'static Route {
    static ROUTE: Lazy<Route> = Lazy::new(|| Route::new("schedule://cleanup"));
    &ROUTE
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn should_define_exactly_one_descriptor_for_every_domain_kind() {
        // Arrange
        let descriptors = DomainRegistry::all();

        // Act
        let descriptor_kinds = descriptors
            .iter()
            .map(|descriptor| descriptor.kind)
            .collect::<HashSet<_>>();
        let all_kinds = DomainKind::ALL.into_iter().collect::<HashSet<_>>();

        // Assert
        assert_eq!(descriptors.len(), DomainKind::ALL.len());
        assert_eq!(descriptor_kinds, all_kinds);
    }

    #[test]
    fn should_resolve_each_domain_kind_to_its_own_descriptor() {
        // `descriptor()` maps each variant to a hardcoded `DOMAIN_DESCRIPTORS`
        // index. Set-equality alone cannot catch a reordered array, which would
        // silently give every domain another domain's scheme and routes.

        // Arrange
        let kinds = DomainKind::ALL;

        // Act
        let resolved = kinds.map(|kind| kind.descriptor().kind);

        // Assert
        assert_eq!(resolved, kinds);
    }

    #[test]
    fn should_keep_cleanup_order_covering_registered_domains() {
        // Arrange
        let cleanup_order = DomainRegistry::cleanup_order();

        // Act
        let cleanup_kinds = cleanup_order.iter().copied().collect::<HashSet<_>>();
        let all_kinds = DomainKind::ALL.into_iter().collect::<HashSet<_>>();

        // Assert
        assert_eq!(cleanup_order.len(), DomainKind::ALL.len());
        assert_eq!(cleanup_kinds, all_kinds);
    }

    #[test]
    fn should_keep_legacy_routes_on_descriptors() {
        // Arrange
        let routes = DomainKind::ALL
            .into_iter()
            .map(|kind| {
                (
                    kind.as_str(),
                    kind.inbound_route().as_str().to_string(),
                    kind.cleanup_route().as_str().to_string(),
                    kind.wildcard_route(),
                )
            })
            .collect::<Vec<_>>();

        // Act
        let expected = vec![
            (
                "kv",
                "kv://inbound".to_string(),
                "kv://cleanup".to_string(),
                "kv://**",
            ),
            (
                "queue",
                "queue://inbound".to_string(),
                "queue://cleanup".to_string(),
                "queue://**",
            ),
            (
                "notice",
                "notice://inbound".to_string(),
                "notice://cleanup".to_string(),
                "notice://**",
            ),
            (
                "stream",
                "stream://inbound".to_string(),
                "stream://cleanup".to_string(),
                "stream://**",
            ),
            (
                "rpc",
                "rpc://inbound".to_string(),
                "rpc://cleanup".to_string(),
                "rpc://**",
            ),
            (
                "lease",
                "lease://inbound".to_string(),
                "lease://cleanup".to_string(),
                "lease://**",
            ),
            (
                "schedule",
                "schedule://inbound".to_string(),
                "schedule://cleanup".to_string(),
                "schedule://**",
            ),
        ];

        // Assert
        assert_eq!(routes, expected);
    }
}
