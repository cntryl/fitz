use super::read_support::{
    begin_read_tx, bounded_fragment_rows, bounded_posting_rows, broad_scope_fragment_rows,
    hydrate_realm_locator, load_global_locator_record, record_payload_bytes, resolve_blob_payload,
    validate_fragment_range, GlobalFragmentCache,
};
use super::{
    decode_realm_offset_from_key, encode_compact_global_page_key,
    encode_compressed_compact_realm_page_key, encode_global_area_posting_key,
    encode_global_area_resource_posting_key, encode_global_resource_posting_key,
    encode_realm_resource_posting_key, read_limit_to_usize, record_is_expired, Bytes,
    CompactGlobalPageValue, CompressedCompactRealmPageValue, PostingPageValue,
    ReadGlobalPostingParams, ReadRealmPostingParams, StreamFilterSet, StreamFilteredReason,
    StreamReadItem, StreamRecord, StreamStore, GLOBAL_PAGE_RECORD_LIMIT,
};
use crate::domains::stream::protocol::ReadCursor;

type CachedRealmPostingPage = Option<(u64, CompressedCompactRealmPageValue)>;

fn realm_posting_route(
    realm: &str,
    record: &super::CompactRealmPageRecord,
) -> crate::runtime::routing::Route {
    crate::runtime::routing::Route::new(format!(
        "stream://{}/{}/{}",
        realm, record.area, record.resource
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PostingScope {
    Realm,
    Global,
}

fn posting_cursor(scope: PostingScope, offset: u64, watermark: u64, has_more: bool) -> ReadCursor {
    ReadCursor {
        last_resource_offset: 0,
        last_area_offset: None,
        last_realm_offset: (scope == PostingScope::Realm).then_some(offset),
        last_global_offset: (scope == PostingScope::Global).then_some(offset),
        has_more,
        cursor_fingerprint: None,
        captured_watermark: Some(watermark),
    }
}

fn realm_posting_record(
    txn: &cntryl_midge::Transaction,
    realm: &str,
    entry: &super::PostingEntry,
    cache: &mut CachedRealmPostingPage,
    global_cache: &mut GlobalFragmentCache,
) -> Result<Option<super::CompactRealmPageRecord>, String> {
    if cache.as_ref().map(|(start, _)| *start) != Some(entry.parent_fragment_start) {
        let key = encode_compressed_compact_realm_page_key(realm, entry.parent_fragment_start);
        let direct = txn
            .get(&key)
            .map_err(|error| format!("read realm fragment failed: {error:?}"))?;
        *cache = if let Some(value) = direct {
            Some((
                entry.parent_fragment_start,
                CompressedCompactRealmPageValue::try_decode(&value)?,
            ))
        } else {
            let bucket_start = entry.offset / GLOBAL_PAGE_RECORD_LIMIT * GLOBAL_PAGE_RECORD_LIMIT;
            let rows = txn
                .scan(
                    &cntryl_midge::Query::new()
                        .start_key(Bytes::from(encode_compressed_compact_realm_page_key(
                            realm,
                            bucket_start,
                        )))
                        .prefix(Bytes::from(
                            StreamStore::build_compressed_compact_realm_page_prefix(realm),
                        ))
                        .limit(65),
                )
                .map_err(|error| format!("scan realm parent fragments failed: {error:?}"))?;
            let mut found = None;
            for row in rows {
                let (key, value) =
                    row.map_err(|error| format!("read realm parent candidate failed: {error:?}"))?;
                let first = decode_realm_offset_from_key(&key)?;
                let page = CompressedCompactRealmPageValue::try_decode(&value)?;
                let end = first.saturating_add(page.records.len() as u64);
                if first <= entry.offset && entry.offset < end {
                    found = Some((first, page));
                    break;
                }
            }
            found
        };
    }
    let actual_start = cache
        .as_ref()
        .map(|(start, _)| *start)
        .ok_or_else(|| "ERR_STREAM_CORRUPT_LOCATOR: missing realm parent fragment".to_string())?;
    let index = usize::try_from(entry.offset.saturating_sub(actual_start)).unwrap_or(usize::MAX);
    let (_, page) = cache
        .as_ref()
        .ok_or_else(|| "ERR_STREAM_CORRUPT_LOCATOR: missing realm parent fragment".to_string())?;
    let mut record = page.records.get(index).cloned().ok_or_else(|| {
        "ERR_STREAM_CORRUPT_LOCATOR: realm posting points outside parent fragment".to_string()
    })?;
    hydrate_realm_locator(txn, realm, &mut record, global_cache)?;
    Ok(Some(record))
}

fn global_posting_record(
    txn: &cntryl_midge::Transaction,
    entry: &super::PostingEntry,
    cache: &mut GlobalFragmentCache,
) -> Result<Option<super::CompactGlobalPageRecord>, String> {
    load_global_locator_record(txn, entry.offset, entry.parent_fragment_start, cache).map(Some)
}

fn global_posting_keys(
    area: Option<&str>,
    resource: Option<&str>,
    from_offset: u64,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let page_start = from_offset / GLOBAL_PAGE_RECORD_LIMIT * GLOBAL_PAGE_RECORD_LIMIT;
    let encode = |offset| match (area, resource) {
        (Some(area), Some(resource)) => Some(encode_global_area_resource_posting_key(
            area, resource, offset,
        )),
        (Some(area), None) => Some(encode_global_area_posting_key(area, offset)),
        (None, Some(resource)) => Some(encode_global_resource_posting_key(resource, offset)),
        (None, None) => None,
    };
    let start_key = encode(page_start)?;
    let mut prefix = encode(0)?;
    prefix.truncate(prefix.len().saturating_sub(24));
    Some((start_key, prefix))
}

impl StreamStore {
    /// Reads one resource name across a realm through its sparse posting.
    ///
    /// # Errors
    ///
    /// Returns an error when activation, posting/page reads, decoding, or
    /// discriminator loading fails.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn read_realm_resource_posting(
        &self,
        params: &ReadRealmPostingParams<'_>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        let ReadRealmPostingParams {
            family,
            realm,
            resource,
            from_offset,
            limit,
            max_bytes,
        } = *params;
        self.ensure_layout_activation_for_family(family)?;
        let watermark = self.get_realm_watermark(family, realm)?;
        let posting_page_start = from_offset / super::REALM_PAGE_RECORD_LIMIT as u64
            * super::REALM_PAGE_RECORD_LIMIT as u64;
        let start_key = encode_realm_resource_posting_key(realm, resource, posting_page_start);
        let mut prefix = encode_realm_resource_posting_key(realm, resource, 0);
        prefix.truncate(prefix.len().saturating_sub(24));
        let txn = begin_read_tx(self, family, "realm posting")?;
        let (rows, fragments_exhausted) =
            bounded_posting_rows(&txn, start_key, prefix, "realm-resource")?;
        let item_limit = read_limit_to_usize(limit);
        let byte_limit = max_bytes.unwrap_or(usize::MAX);
        let mut items = Vec::with_capacity(item_limit.min(1_000));
        let mut bytes_read = 0usize;
        let mut last_examined = from_offset;
        let mut has_more = false;
        let mut examined = 0usize;
        let mut cached_parent = None;
        let mut global_cache = GlobalFragmentCache::new();
        let now_epoch_ms = self.now_epoch_ms();
        'pages: for (_, value) in rows {
            for entry in PostingPageValue::try_decode(&value)?.entries {
                let offset = entry.offset;
                if offset < from_offset || offset > watermark {
                    continue;
                }
                if items.len() >= item_limit {
                    has_more = true;
                    break 'pages;
                }
                if examined >= crate::domains::stream::MAX_POSTING_ENTRIES_EXAMINED {
                    has_more = true;
                    break 'pages;
                }
                examined += 1;
                if record_is_expired(entry.expires_at, now_epoch_ms) {
                    last_examined = offset;
                    continue;
                }
                let Some(record) = realm_posting_record(
                    &txn,
                    realm,
                    &entry,
                    &mut cached_parent,
                    &mut global_cache,
                )?
                else {
                    last_examined = offset;
                    continue;
                };
                let route = realm_posting_route(realm, &record);
                let discriminator = Self::load_optional_discriminator(
                    &txn,
                    &crate::domains::stream::storage::encode_realm_discriminator_key(realm, offset),
                )?;
                if !Self::record_matches_filter(filter, discriminator.as_deref()) {
                    last_examined = offset;
                    items.push(StreamReadItem::Filtered {
                        route,
                        offset,
                        reason: Some(StreamFilteredReason::ServerFilter),
                    });
                    continue;
                }
                let record_bytes = record_payload_bytes(&record.body, record.metadata.as_ref());
                if bytes_read.saturating_add(record_bytes) > byte_limit && !items.is_empty() {
                    has_more = true;
                    break 'pages;
                }
                last_examined = offset;
                bytes_read = bytes_read.saturating_add(record_bytes);
                items.push(StreamReadItem::Event(StreamRecord {
                    route,
                    resource_offset: record.resource_offset,
                    area_offset: Some(record.area_offset),
                    realm_offset: Some(offset),
                    global_offset: None,
                    body: record.body,
                    metadata: record.metadata,
                    created_at: record.created_at,
                }));
            }
        }
        if fragments_exhausted {
            has_more = true;
        } else if !has_more {
            last_examined = last_examined.max(watermark);
        }
        Ok((
            items,
            posting_cursor(
                PostingScope::Realm,
                last_examined,
                watermark.saturating_add(1),
                has_more,
            ),
        ))
    }

    /// Reads a sparse family-global route-filter posting in global order.
    ///
    /// # Errors
    ///
    /// Returns an error when activation, posting/page reads, decoding, or
    /// discriminator loading fails.
    pub(crate) fn read_global_posting(
        &self,
        params: &ReadGlobalPostingParams<'_>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        let ReadGlobalPostingParams {
            family,
            from_offset,
            limit,
            max_bytes,
            area,
            resource,
        } = *params;
        self.ensure_layout_activation_for_family(family)?;
        let watermark = self.get_global_watermark(family)?;
        let Some((start_key, prefix)) = global_posting_keys(area, resource, from_offset) else {
            return self.read_global(family, from_offset, limit, max_bytes, filter);
        };
        let txn = begin_read_tx(self, family, "global posting")?;
        let (rows, fragments_exhausted) = bounded_posting_rows(&txn, start_key, prefix, "global")?;
        let item_limit = read_limit_to_usize(limit);
        let byte_limit = max_bytes.unwrap_or(usize::MAX);
        let mut items = Vec::with_capacity(item_limit.min(1_000));
        let mut bytes_read = 0usize;
        let mut last_examined = from_offset;
        let mut has_more = false;
        let mut examined = 0usize;
        let mut cached_parent = GlobalFragmentCache::new();
        let now_epoch_ms = self.now_epoch_ms();
        'pages: for (_, value) in rows {
            for entry in PostingPageValue::try_decode(&value)?.entries {
                let offset = entry.offset;
                if offset < from_offset || offset >= watermark {
                    continue;
                }
                if items.len() >= item_limit {
                    has_more = true;
                    break 'pages;
                }
                if examined >= crate::domains::stream::MAX_POSTING_ENTRIES_EXAMINED {
                    has_more = true;
                    break 'pages;
                }
                examined += 1;
                if record_is_expired(entry.expires_at, now_epoch_ms) {
                    last_examined = offset;
                    continue;
                }
                let Some(record) = global_posting_record(&txn, &entry, &mut cached_parent)? else {
                    last_examined = offset;
                    continue;
                };
                let route = crate::runtime::routing::Route::new(format!(
                    "stream://{}/{}/{}",
                    record.realm, record.area, record.resource
                ));
                let discriminator = Self::load_optional_discriminator(
                    &txn,
                    &super::encode_global_discriminator_key(offset),
                )?;
                if !Self::record_matches_filter(filter, discriminator.as_deref()) {
                    last_examined = offset;
                    items.push(StreamReadItem::Filtered {
                        route,
                        offset,
                        reason: Some(StreamFilteredReason::ServerFilter),
                    });
                    continue;
                }
                let record_bytes = record_payload_bytes(&record.body, record.metadata.as_ref());
                if bytes_read.saturating_add(record_bytes) > byte_limit && !items.is_empty() {
                    has_more = true;
                    break 'pages;
                }
                last_examined = offset;
                bytes_read = bytes_read.saturating_add(record_bytes);
                items.push(StreamReadItem::Event(StreamRecord {
                    route,
                    resource_offset: record.resource_offset,
                    area_offset: Some(record.area_offset),
                    realm_offset: Some(record.realm_offset),
                    global_offset: Some(offset),
                    body: record.body,
                    metadata: record.metadata,
                    created_at: record.created_at,
                }));
            }
        }
        if fragments_exhausted {
            has_more = true;
        } else if !has_more {
            last_examined = last_examined.max(watermark.saturating_sub(1));
        }
        Ok((
            items,
            posting_cursor(PostingScope::Global, last_examined, watermark, has_more),
        ))
    }

    /// Reads a family-global snapshot in global-offset order.
    ///
    /// # Errors
    ///
    /// Returns an error if activation, watermark loading, storage scanning,
    /// page decoding, discriminator loading, or route construction fails.
    #[allow(clippy::too_many_lines)]
    pub fn read_global(
        &self,
        family: u64,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        self.ensure_layout_activation_for_family(family)?;
        let watermark = self.get_global_watermark(family)?;
        let mut prefix = encode_compact_global_page_key(0);
        prefix.truncate(prefix.len().saturating_sub(24));
        let page_start = from_offset / GLOBAL_PAGE_RECORD_LIMIT * GLOBAL_PAGE_RECORD_LIMIT;
        let txn = begin_read_tx(self, family, "global")?;
        let (rows, fragments_exhausted) = bounded_fragment_rows(
            &txn,
            encode_compact_global_page_key(page_start),
            prefix,
            "global",
            broad_scope_fragment_rows(from_offset, limit),
        )?;
        let item_limit = read_limit_to_usize(limit);
        let byte_limit = max_bytes.unwrap_or(usize::MAX);
        let mut items = Vec::with_capacity(item_limit.min(1_000));
        let mut bytes_read = 0usize;
        let mut last_examined = from_offset;
        let mut has_more = false;
        let mut previous_fragment_end = None;
        let now_epoch_ms = self.now_epoch_ms();
        'pages: for (key, value) in rows {
            let first = decode_realm_offset_from_key(&key)?;
            let page = CompactGlobalPageValue::try_decode(&value)?;
            validate_fragment_range(
                "global",
                first,
                page.records.len(),
                &mut previous_fragment_end,
            )?;
            for (index, mut record) in page.records.into_iter().enumerate() {
                let offset = first.saturating_add(index as u64);
                if offset < from_offset {
                    continue;
                }
                if offset >= watermark {
                    break 'pages;
                }
                if record_is_expired(record.expires_at, now_epoch_ms) {
                    last_examined = offset;
                    continue;
                }
                resolve_blob_payload(&txn, &mut record.body, &mut record.metadata)?;
                let record_bytes = record
                    .body
                    .len()
                    .saturating_add(record.metadata.as_ref().map_or(0, Bytes::len));
                if items.len() >= item_limit {
                    has_more = true;
                    break 'pages;
                }
                let route = crate::runtime::routing::Route::new(format!(
                    "stream://{}/{}/{}",
                    record.realm, record.area, record.resource
                ));
                let discriminator = Self::load_optional_discriminator(
                    &txn,
                    &super::encode_global_discriminator_key(offset),
                )?;
                if !Self::record_matches_filter(filter, discriminator.as_deref()) {
                    last_examined = offset;
                    items.push(StreamReadItem::Filtered {
                        route,
                        offset,
                        reason: Some(StreamFilteredReason::ServerFilter),
                    });
                    continue;
                }
                if bytes_read.saturating_add(record_bytes) > byte_limit && !items.is_empty() {
                    has_more = true;
                    break 'pages;
                }
                last_examined = offset;
                bytes_read = bytes_read.saturating_add(record_bytes);
                items.push(StreamReadItem::Event(StreamRecord {
                    route,
                    resource_offset: record.resource_offset,
                    area_offset: Some(record.area_offset),
                    realm_offset: Some(record.realm_offset),
                    global_offset: Some(offset),
                    body: record.body,
                    metadata: record.metadata,
                    created_at: record.created_at,
                }));
            }
        }
        if fragments_exhausted {
            has_more = true;
        }
        Ok((
            items,
            ReadCursor {
                last_resource_offset: 0,
                last_area_offset: None,
                last_realm_offset: None,
                last_global_offset: Some(last_examined),
                has_more,
                cursor_fingerprint: None,
                captured_watermark: Some(watermark),
            },
        ))
    }
}
