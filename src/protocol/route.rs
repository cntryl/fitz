//! Route parsing and validation helpers used by transports and control
//! components.
//!
//! Routes follow the canonical form:
//!
//! ```text
//! scheme://realm/area/resource[/operation]
//! ```
//!
//! Notes:
//! - `control://` routes are system-scoped and may omit the realm segment.
//! - Bare or development routes (e.g. `ntc/...`) are handled elsewhere as a
//!   convenience; this module focuses on scheme-prefixed parsing and realm
//!   enforcement.

/// Supported route schemes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// `notice://...` routes for ephemeral notices
    Notice,
    /// `stream://...` append-only streams
    Stream,
    /// `queue://...` queue/lease semantics
    Queue,
    /// `rpc://...` RPC-style services
    Rpc,
    /// `inbox://...` per-session reply-inboxes
    Inbox,
    /// `control://...` broker control plane routes (system scoped)
    Control,
}

impl Scheme {
    pub fn as_str(&self) -> &'static str {
        match self {
            Scheme::Notice => "notice",
            Scheme::Stream => "stream",
            Scheme::Queue => "queue",
            Scheme::Rpc => "rpc",
            Scheme::Inbox => "inbox",
            Scheme::Control => "control",
        }
    }
}

impl TryFrom<&str> for Scheme {
    type Error = &'static str;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "notice" => Ok(Scheme::Notice),
            "stream" => Ok(Scheme::Stream),
            "queue" => Ok(Scheme::Queue),
            "rpc" => Ok(Scheme::Rpc),
            "inbox" => Ok(Scheme::Inbox),
            "control" => Ok(Scheme::Control),
            _ => Err("unknown scheme"),
        }
    }
}

/// Structured representation of a parsed route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// The parsed scheme
    pub scheme: Scheme,
    /// Optional tenant realm (present for tenant-scoped schemes)
    pub realm: Option<String>,
    /// Optional area segment
    pub area: Option<String>,
    /// Optional resource segment
    pub resource: Option<String>,
    /// Optional operation segment (e.g. sub-action)
    pub operation: Option<String>,
    /// The original, raw route string as provided
    pub raw: String,
}

/// Parse a route string into a `Route` structure.
///
/// Returns `Err(&'static str)` when the input fails minimal validation (for
/// example missing scheme delimiter or missing realm for tenant-scoped
/// schemes).
///
/// `control://` routes may omit the realm; other schemes require a realm
/// segment.
pub fn parse_route(s: &str) -> Result<Route, &'static str> {
    // Expect something like: scheme://...
    let (scheme_str, rest) = s.split_once("://").ok_or("missing scheme delimiter")?;
    let scheme = Scheme::try_from(scheme_str)?;
    let mut realm: Option<String> = None;
    let mut area: Option<String> = None;
    let mut resource: Option<String> = None;
    let mut operation: Option<String> = None;

    if rest.is_empty() {
        // control:// may be bare
        if scheme != Scheme::Control {
            return Err("missing realm");
        }
    } else {
        let mut parts = rest.split('/').filter(|p| !p.is_empty());
        if scheme == Scheme::Control {
            // control can have arbitrary path; no realm enforcement here
            realm = parts.next().map(|s| s.to_string());
            area = parts.next().map(|s| s.to_string());
            resource = parts.next().map(|s| s.to_string());
            operation = parts.next().map(|s| s.to_string());
        } else {
            // realm required for tenant-scoped schemes
            match parts.next() {
                Some(r) => realm = Some(r.to_string()),
                None => return Err("missing realm"),
            }
            area = parts.next().map(|s| s.to_string());
            resource = parts.next().map(|s| s.to_string());
            operation = parts.next().map(|s| s.to_string());
        }
    }

    Ok(Route {
        scheme,
        realm,
        area,
        resource,
        operation,
        raw: s.to_string(),
    })
}

/// Returns true when the provided `jwt_realm` is authorized to access the
/// `route` per the broker's realm enforcement rules.
///
/// Rules applied:
/// - For `control://` and `inbox://` schemes the realm check is bypassed.
/// - For other schemes the route's `realm` must equal the `jwt_realm`.
pub fn realm_matches(route: &Route, jwt_realm: &str) -> bool {
    if route.scheme == Scheme::Control || route.scheme == Scheme::Inbox {
        return true;
    }
    match &route.realm {
        Some(r) => r == jwt_realm,
        None => false,
    }
}
