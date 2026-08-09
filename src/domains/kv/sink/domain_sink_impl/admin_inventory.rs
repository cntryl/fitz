use super::KvDomainRuntime;
use crate::domains::kv::sink::AdminKvCommittedPair;
use bytes::Bytes;

impl KvDomainRuntime<'_> {
    pub(super) fn scan_scoped_prefix(
        tx: &cntryl_midge::Transaction,
        resource_prefix: &[u8],
        scoped_prefix: &[u8],
        scoped_start: &[u8],
        limit: usize,
    ) -> Result<Vec<AdminKvCommittedPair>, String> {
        let query = cntryl_midge::Query::new()
            .prefix(Bytes::copy_from_slice(scoped_prefix))
            .start_key(Bytes::copy_from_slice(scoped_start))
            .end_key(Bytes::from(crate::domains::kv::KvActor::prefix_range_end(
                scoped_prefix,
            )))
            .limit(limit);
        let iterator = tx.scan(&query).map_err(|error| error.to_string())?;
        let mut rows = Vec::new();
        for entry in iterator {
            let (scoped_key, value) = entry.map_err(|error| error.to_string())?;
            if let Some(user_key) =
                crate::domains::kv::KvActor::strip_scoped_prefix(resource_prefix, &scoped_key)
            {
                rows.push((user_key, value.to_vec()));
            }
        }
        Ok(rows)
    }
}
