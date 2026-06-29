use super::{AdminAuthMode, AdminRouteFamilyAccess};

#[must_use]
pub fn protected_admin_configured_from_env() -> bool {
    let mode = match std::env::var("FITZ_ADMIN_AUTH_MODE") {
        Ok(value) if value.eq_ignore_ascii_case("open") => AdminAuthMode::Open,
        _ => AdminAuthMode::Protected,
    };

    if matches!(mode, AdminAuthMode::Open) {
        return false;
    }

    env_non_empty("FITZ_ADMIN_USERNAME").is_some()
        && env_non_empty("FITZ_ADMIN_PASSWORD_HASH").is_some()
}

pub(super) fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn parse_admin_route_family_access(raw: Option<&str>) -> AdminRouteFamilyAccess {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return AdminRouteFamilyAccess::wildcard();
    };

    if raw == "*" {
        return AdminRouteFamilyAccess::wildcard();
    }

    let mut route_families = Vec::new();
    for route_family in raw
        .split(',')
        .map(str::trim)
        .filter(|route_family| !route_family.is_empty())
    {
        if !route_families
            .iter()
            .any(|existing| existing == route_family)
        {
            route_families.push(route_family.to_string());
        }
    }

    if route_families.is_empty() {
        AdminRouteFamilyAccess::wildcard()
    } else {
        AdminRouteFamilyAccess::Explicit(route_families)
    }
}
