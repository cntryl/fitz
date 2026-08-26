//! Storage key layouts for committed user rows and admin inventory metadata.

use super::KvActor;
use crate::runtime::routing::RouteFamily;
use crate::utils::storage_key::{self, DomainKeyspace};
use lexkey::LexKey;

pub(super) const KV_KEY_SCOPE_MARKER: u8 = 0x01;
const KV_INVENTORY_SCOPE_MARKER: u8 = 0x02;

impl KvActor {
    /// Resolve the column family from `RouteFamily`; resource isolation uses key prefixes.
    ///
    /// # Errors
    /// Returns an error if the family would select the forbidden default column family.
    pub(crate) fn resolve_column_family(
        route_family: RouteFamily,
    ) -> Result<cntryl_midge::ColumnFamilyId, String> {
        crate::runtime::cf_validation::validate_route_family(route_family)?;
        Ok(route_family.id())
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

    pub(crate) fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
        crate::utils::storage_key::prefix_range_end(prefix)
    }

    pub(crate) fn realm_resource_prefix(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        let mut encoder = storage_key::domain_marker_encoder(
            realm,
            DomainKeyspace::Kv,
            KV_KEY_SCOPE_MARKER,
            area.len() + resource.len() + 2,
        );
        storage_key::encode_bytes_segment_into(&mut encoder, area.as_bytes());
        storage_key::encode_bytes_segment_into(&mut encoder, resource.as_bytes());
        encoder.into_vec()
    }

    pub(crate) fn inventory_metadata_key(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        let mut encoder = storage_key::domain_marker_encoder(
            realm,
            DomainKeyspace::Kv,
            KV_INVENTORY_SCOPE_MARKER,
            area.len() + resource.len() + 2,
        );
        storage_key::encode_bytes_segment_into(&mut encoder, area.as_bytes());
        storage_key::encode_bytes_segment_into(&mut encoder, resource.as_bytes());
        encoder.into_vec()
    }

    pub(crate) fn parse_inventory_metadata_key(key: &[u8]) -> Option<(String, String, String)> {
        let (realm, suffix) = storage_key::split_domain_key(key, DomainKeyspace::Kv)?;
        if suffix.first().copied()? != KV_INVENTORY_SCOPE_MARKER {
            return None;
        }

        let mut parts = suffix[1..].split(|byte| *byte == LexKey::SEPARATOR);
        let area = parts.next()?;
        let resource = parts.next()?;
        let trailing = parts.next();
        if area.is_empty() || resource.is_empty() || trailing != Some(&[]) || parts.next().is_some()
        {
            return None;
        }

        Some((
            realm.to_string(),
            String::from_utf8(area.to_vec()).ok()?,
            String::from_utf8(resource.to_vec()).ok()?,
        ))
    }
}
