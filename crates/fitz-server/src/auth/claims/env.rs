use std::collections::HashMap;

use super::{ENV_ROUTE_FAMILY_MAP, REMOVED_ENV_AUTH_ALLOW_JWT_ROUTE_FAMILY};

pub(super) fn removed_legacy_route_family_env_reason() -> Option<String> {
    std::env::var_os(REMOVED_ENV_AUTH_ALLOW_JWT_ROUTE_FAMILY).map(|_| {
        format!(
            "{REMOVED_ENV_AUTH_ALLOW_JWT_ROUTE_FAMILY} has been removed; JWT fitz.route_family compatibility is no longer supported"
        )
    })
}

pub(super) fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn parse_route_family_map_env() -> (HashMap<String, u32>, Option<String>) {
    let Some(raw) = env_non_empty(ENV_ROUTE_FAMILY_MAP) else {
        return (HashMap::new(), None);
    };

    let mut mappings = HashMap::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let Some((identity, family)) = entry.split_once('=') else {
            return (
                mappings,
                Some(format!(
                    "{ENV_ROUTE_FAMILY_MAP} entries must use identity=family format"
                )),
            );
        };
        let identity = identity.trim();
        if identity.is_empty() {
            return (
                mappings,
                Some(format!(
                    "{ENV_ROUTE_FAMILY_MAP} identity keys must not be empty"
                )),
            );
        }
        if mappings.contains_key(identity) {
            return (
                mappings,
                Some(format!(
                    "{ENV_ROUTE_FAMILY_MAP} contains duplicate identity '{identity}'"
                )),
            );
        }
        let Ok(family) = family.trim().parse::<u32>() else {
            return (
                mappings,
                Some(format!(
                    "{ENV_ROUTE_FAMILY_MAP} family for identity '{identity}' must be an unsigned integer"
                )),
            );
        };
        mappings.insert(identity.to_string(), family);
    }

    (mappings, None)
}
