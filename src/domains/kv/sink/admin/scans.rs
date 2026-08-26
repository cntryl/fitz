//! Storage-backed admin prefix and paginated row scans.

use super::super::locks::KvResourceLockKey;
use super::super::state::KvDomainRuntime;
use super::{AdminKvCommittedPair, AdminKvPrefixScanResult, AdminKvRowsRequest, AdminKvRowsResult};
use crate::domains::kv::KvActor;
use bytes::Bytes;

impl KvDomainRuntime<'_> {
    pub(super) fn admin_scan_committed_prefix(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<AdminKvPrefixScanResult, String> {
        let started_at = std::time::Instant::now();
        let column_family = KvActor::resolve_column_family(route_family)?;
        let tx = self
            .core
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let resource_prefix = KvActor::realm_resource_prefix(realm, area, resource);
        let scoped_prefix = KvActor::encode_scoped_key(&resource_prefix, key_prefix);
        let mut rows = Self::scan_scoped_prefix(
            &tx,
            &resource_prefix,
            &scoped_prefix,
            &scoped_prefix,
            limit.saturating_add(1),
        )?;

        let has_more = rows.len() > limit;
        rows.truncate(limit);
        self.record_read_latency(
            &KvResourceLockKey::new(route_family.as_u64(), realm, area, resource),
            started_at,
        );
        Ok(AdminKvPrefixScanResult {
            items: rows,
            has_more,
        })
    }

    pub(super) fn admin_scan_committed_rows(
        &self,
        request: &AdminKvRowsRequest<'_>,
    ) -> Result<AdminKvRowsResult, String> {
        let started_at = std::time::Instant::now();
        if let Some(cursor) = request.cursor {
            if !cursor.starts_with(request.starts_with) {
                return Err("cursor must start with starts_with prefix".to_string());
            }
        }

        let column_family = KvActor::resolve_column_family(request.route_family)?;
        let tx = self
            .core
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let resource_prefix =
            KvActor::realm_resource_prefix(request.realm, request.area, request.resource);
        let scoped_prefix = KvActor::encode_scoped_key(&resource_prefix, request.starts_with);
        let scoped_start = request.cursor.map_or_else(
            || scoped_prefix.clone(),
            |cursor| KvActor::encode_scoped_key(&resource_prefix, cursor),
        );
        let mut rows = Self::scan_scoped_prefix(
            &tx,
            &resource_prefix,
            &scoped_prefix,
            &scoped_start,
            request.limit.saturating_add(1),
        )?;
        rows.retain(|item| {
            request
                .cursor
                .is_none_or(|cursor| item.key.as_slice() > cursor)
        });

        let has_more = rows.len() > request.limit;
        rows.truncate(request.limit);
        let next_cursor = if has_more {
            rows.last().map(|item| item.key.clone())
        } else {
            None
        };
        self.record_read_latency(
            &KvResourceLockKey::new(
                request.route_family.as_u64(),
                request.realm,
                request.area,
                request.resource,
            ),
            started_at,
        );
        Ok(AdminKvRowsResult {
            items: rows,
            next_cursor,
            has_more,
        })
    }

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
            .end_key(Bytes::from(KvActor::prefix_range_end(scoped_prefix)))
            .limit(limit);
        let iterator = tx.scan(&query).map_err(|error| error.to_string())?;
        let mut rows = Vec::new();
        for entry in iterator {
            let (scoped_key, value) = entry.map_err(|error| error.to_string())?;
            if let Some(user_key) = KvActor::strip_scoped_prefix(resource_prefix, &scoped_key) {
                rows.push(AdminKvCommittedPair {
                    key: user_key,
                    value: value.to_vec(),
                });
            }
        }
        Ok(rows)
    }
}
