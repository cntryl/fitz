use super::KvActor;
use crate::runtime::routing::RouteFamily;

impl KvActor {
    /// Resolve the column family from `RouteFamily`; resource isolation uses key prefixes.
    ///
    /// # Errors
    /// Returns an error if the family would select the forbidden default column family.
    pub(crate) fn resolve_column_family(
        route_family: RouteFamily,
        _resource: &str,
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
}
