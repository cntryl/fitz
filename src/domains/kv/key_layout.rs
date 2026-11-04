//! Storage key layouts for committed user rows and admin inventory metadata.

use crate::runtime::routing::RouteFamily;
use crate::utils::storage_key::{self, DomainKeyspace};
use lexkey::LexKey;

const KV_KEY_SCOPE_MARKER: u8 = 0x01;
const KV_INVENTORY_SCOPE_MARKER: u8 = 0x02;

/// Separate user-data and inventory ranges for one DROP scope.
pub(crate) fn drop_prefixes(scope: &crate::domains::kv::KvDropScope) -> [Vec<u8>; 2] {
    use crate::domains::kv::KvDropTarget;

    let build_prefix = |marker| match &scope.target {
        KvDropTarget::Realm => scope_prefix(&scope.realm, marker, &[]),
        KvDropTarget::Area { area } => scope_prefix(&scope.realm, marker, &[area]),
        KvDropTarget::Resource { area, resource } => {
            scope_prefix(&scope.realm, marker, &[area, resource])
        }
    };
    [
        build_prefix(KV_KEY_SCOPE_MARKER),
        build_prefix(KV_INVENTORY_SCOPE_MARKER),
    ]
}

/// Resolve the column family from `RouteFamily`; resource isolation uses key prefixes.
///
/// # Errors
/// Returns an error if the family would select the forbidden default column family.
pub(crate) fn resolve_column_family(route_family: RouteFamily) -> Result<u32, String> {
    crate::storage::cf_validation::route_family_to_cf(route_family)
}

pub(crate) fn encode_scoped_key(prefix: &[u8], user_key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(prefix.len() + user_key.len());
    encoded.extend_from_slice(prefix);
    encoded.extend_from_slice(user_key);
    encoded
}

pub(crate) fn strip_scoped_prefix(prefix: &[u8], scoped_key: &[u8]) -> Option<Vec<u8>> {
    scoped_key.strip_prefix(prefix).map(<[u8]>::to_vec)
}

pub(crate) fn realm_resource_prefix(realm: &str, area: &str, resource: &str) -> Vec<u8> {
    scope_prefix(realm, KV_KEY_SCOPE_MARKER, &[area, resource])
}

pub(crate) fn inventory_metadata_key(realm: &str, area: &str, resource: &str) -> Vec<u8> {
    scope_prefix(realm, KV_INVENTORY_SCOPE_MARKER, &[area, resource])
}

pub(crate) fn inventory_realm_prefix(realm: &str) -> Vec<u8> {
    scope_prefix(realm, KV_INVENTORY_SCOPE_MARKER, &[])
}

pub(crate) fn inventory_area_prefix(realm: &str, area: &str) -> Vec<u8> {
    scope_prefix(realm, KV_INVENTORY_SCOPE_MARKER, &[area])
}

fn scope_prefix(realm: &str, marker: u8, segments: &[&str]) -> Vec<u8> {
    let capacity = segments.iter().map(|segment| segment.len() + 1).sum();
    let mut encoder =
        storage_key::domain_marker_encoder(realm, DomainKeyspace::Kv, marker, capacity);
    for segment in segments {
        storage_key::encode_bytes_segment_into(&mut encoder, segment.as_bytes());
    }
    encoder.into_vec()
}

pub(crate) fn parse_inventory_metadata_key(key: &[u8]) -> Option<(String, String, String)> {
    let scope = parse_scope_key(key, KV_INVENTORY_SCOPE_MARKER)?;
    // Metadata ends at the resource separator; it has no client-key suffix.
    if scope.suffix != Some(&[]) {
        return None;
    }
    Some(scope.into_owned())
}

pub(crate) fn parse_user_scope_key(key: &[u8]) -> Option<(String, String, String)> {
    // User keys may have arbitrary binary bytes after the scope boundary.
    parse_scope_key(key, KV_KEY_SCOPE_MARKER).map(ParsedScopeKey::into_owned)
}

struct ParsedScopeKey<'a> {
    realm: &'a str,
    area: &'a str,
    resource: &'a str,
    suffix: Option<&'a [u8]>,
}

impl ParsedScopeKey<'_> {
    fn into_owned(self) -> (String, String, String) {
        (self.realm.into(), self.area.into(), self.resource.into())
    }
}

fn parse_scope_key(key: &[u8], marker: u8) -> Option<ParsedScopeKey<'_>> {
    let (realm, suffix) = storage_key::split_domain_key(key, DomainKeyspace::Kv)?;
    if suffix.first().copied()? != marker {
        return None;
    }

    let mut parts = suffix[1..].splitn(3, |byte| *byte == LexKey::SEPARATOR);
    let area = parts.next()?;
    let resource = parts.next()?;
    if area.is_empty() || resource.is_empty() {
        return None;
    }

    Some(ParsedScopeKey {
        realm,
        area: std::str::from_utf8(area).ok()?,
        resource: std::str::from_utf8(resource).ok()?,
        suffix: parts.next(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_kv_scope_prefix_with_typed_segments() {
        // Arrange
        let expected = b"acme\0kv\0\x01users\0profiles\0";

        // Act
        let prefix = realm_resource_prefix("acme", "users", "profiles");

        // Assert
        assert_eq!(prefix, expected);
    }

    #[test]
    fn should_keep_drop_prefixes_at_complete_scope_boundaries() {
        // Arrange
        let cases = [
            (
                "kv://acme/*",
                [b"acme\0kv\0\x01".as_slice(), b"acme\0kv\0\x02".as_slice()],
            ),
            (
                "kv://acme/app/*",
                [
                    b"acme\0kv\0\x01app\0".as_slice(),
                    b"acme\0kv\0\x02app\0".as_slice(),
                ],
            ),
            (
                "kv://acme/app/rows",
                [
                    b"acme\0kv\0\x01app\0rows\0".as_slice(),
                    b"acme\0kv\0\x02app\0rows\0".as_slice(),
                ],
            ),
        ];

        // Act
        let results = cases.map(|(route, expected)| {
            let scope =
                crate::domains::kv::KvDropScope::parse(RouteFamily::new(41), route).unwrap();
            (drop_prefixes(&scope), expected)
        });

        // Assert
        for (actual, expected) in results {
            assert_eq!(actual[0], expected[0]);
            assert_eq!(actual[1], expected[1]);
        }
    }

    #[test]
    fn should_parse_scope_before_arbitrary_binary_user_key_suffixes() {
        // Arrange
        let prefix = realm_resource_prefix("acme", "app", "rows");
        let suffixes = [b"".as_slice(), b"key", b"\0\xff\0raw", b"\xff\xff"];

        // Act
        let scopes =
            suffixes.map(|suffix| parse_user_scope_key(&encode_scoped_key(&prefix, suffix)));

        // Assert
        for scope in scopes {
            assert_eq!(scope, Some(("acme".into(), "app".into(), "rows".into())));
        }
    }

    #[test]
    fn should_require_inventory_metadata_keys_to_end_at_the_scope_separator() {
        // Arrange
        let metadata = inventory_metadata_key("acme", "app", "rows");
        let mut missing_separator = metadata.clone();
        missing_separator.pop();
        let extra_suffixes = [b"key".as_slice(), b"\0", b"\xff"];

        // Act
        let exact = parse_inventory_metadata_key(&metadata);
        let truncated = parse_inventory_metadata_key(&missing_separator);
        let suffixed = extra_suffixes
            .map(|suffix| parse_inventory_metadata_key(&encode_scoped_key(&metadata, suffix)));

        // Assert
        assert_eq!(exact, Some(("acme".into(), "app".into(), "rows".into())));
        assert!(truncated.is_none());
        assert!(suffixed.iter().all(Option::is_none));
    }

    #[test]
    fn should_match_golden_kv_storage_key_plus_value_bytes() {
        // Arrange
        const GOLDEN_KEY: &str = "61636d65006b7600016a6f6273007374617465006b6579";
        const GOLDEN_VALUE: &str = "76616c7565";

        // Act
        let mut key = realm_resource_prefix("acme", "jobs", "state");
        key.extend_from_slice(b"key");

        // Assert
        assert_eq!(hex::encode(key), GOLDEN_KEY);
        assert_eq!(hex::encode(b"value"), GOLDEN_VALUE);
    }
}
