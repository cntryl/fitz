use super::{
    area_page_record_bytes, collect_filtered_read_page_items, decode_area_offset_from_key,
    decode_realm_offset_from_key, decode_resource_offset_from_key, encode_compact_area_page_key,
    encode_compact_global_page_key, encode_compact_resource_page_key,
    encode_compressed_compact_realm_page_key, encode_global_area_posting_key,
    encode_global_area_resource_posting_key, encode_global_resource_posting_key,
    encode_realm_resource_posting_key, family_to_storage_partition, read_limit_to_usize,
    realm_page_record_bytes, resource_page_record_bytes, update_area_cursor, update_realm_cursor,
    update_resource_cursor, usize_to_u64_saturating, Bytes, CompactAreaPageValue,
    CompactGlobalPageValue, CompactResourcePageValue, CompressedCompactRealmPageValue,
    PostingPageValue, ReadAreaParams, ReadCursorState, ReadGlobalPostingParams, ReadPageState,
    ReadRealmPostingParams, ReadResourceParams, StreamFilterSet, StreamFilteredReason,
    StreamReadItem, StreamRecord, StreamStore, GLOBAL_PAGE_RECORD_LIMIT,
};
use crate::domains::stream::protocol::ReadCursor;

type CachedRealmPostingPage = Option<(u64, CompressedCompactRealmPageValue)>;
type CachedGlobalPostingPage = Option<(u64, CompactGlobalPageValue)>;

fn page_slot_offset(page_start: u64, slot: usize) -> u64 {
    page_start.saturating_add(usize_to_u64_saturating(slot))
}

fn realm_posting_route(
    realm: &str,
    record: &super::CompactRealmPageRecord,
) -> crate::runtime::routing::Route {
    crate::runtime::routing::Route::new(format!(
        "stream://{}/{}/{}",
        realm, record.area, record.resource
    ))
}

fn record_payload_bytes(body: &Bytes, metadata: Option<&Bytes>) -> usize {
    body.len().saturating_add(metadata.map_or(0, Bytes::len))
}

fn resource_page_query(params: &ReadResourceParams) -> cntryl_midge::Query {
    cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_resource_page_key(
            params.realm,
            params.area,
            params.resource,
            StreamStore::page_start_offset(params.from_offset),
        )))
        .prefix(Bytes::from(
            StreamStore::build_compact_resource_page_prefix(
                params.realm,
                params.area,
                params.resource,
            ),
        ))
        .limit(StreamStore::compact_page_query_limit(
            params.from_offset,
            params.limit,
        ))
}

fn bounded_fragment_rows(
    txn: &cntryl_midge::Transaction,
    start_key: Vec<u8>,
    prefix: Vec<u8>,
    context: &str,
    max_rows: usize,
) -> Result<(Vec<(Bytes, Bytes)>, bool), String> {
    let query_rows = max_rows.saturating_add(1);
    let mut rows = txn
        .scan(
            &cntryl_midge::Query::new()
                .start_key(Bytes::from(start_key))
                .prefix(Bytes::from(prefix))
                .limit(query_rows),
        )
        .map_err(|error| format!("scan {context} postings failed: {error:?}"))?
        .try_collect()
        .map_err(|error| format!("collect {context} postings failed: {error:?}"))?;
    let exhausted = rows.len() > max_rows;
    rows.truncate(max_rows);
    Ok((rows, exhausted))
}

fn broad_scope_fragment_rows(from_offset: u64, limit: u64) -> usize {
    let skipped_in_bucket =
        usize::try_from(from_offset % GLOBAL_PAGE_RECORD_LIMIT).unwrap_or(usize::MAX);
    skipped_in_bucket
        .saturating_add(read_limit_to_usize(limit))
        .min(crate::domains::stream::MAX_POSTING_FRAGMENTS_FETCHED)
}

fn bounded_posting_rows(
    txn: &cntryl_midge::Transaction,
    start_key: Vec<u8>,
    prefix: Vec<u8>,
    context: &str,
) -> Result<(Vec<(Bytes, Bytes)>, bool), String> {
    bounded_fragment_rows(
        txn,
        start_key,
        prefix,
        context,
        crate::domains::stream::MAX_POSTING_FRAGMENTS_FETCHED,
    )
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
) -> Result<Option<super::CompactRealmPageRecord>, String> {
    if cache.as_ref().map(|(start, _)| *start) != Some(entry.parent_fragment_start) {
        let key = encode_compressed_compact_realm_page_key(realm, entry.parent_fragment_start);
        *cache = txn
            .get(&key)
            .map_err(|error| format!("read realm fragment failed: {error:?}"))?
            .map(|value| {
                CompressedCompactRealmPageValue::try_decode(&value)
                    .map(|page| (entry.parent_fragment_start, page))
            })
            .transpose()?;
    }
    let index = usize::try_from(entry.offset.saturating_sub(entry.parent_fragment_start))
        .unwrap_or(usize::MAX);
    Ok(cache
        .as_ref()
        .and_then(|(_, page)| page.records.get(index))
        .cloned())
}

fn global_posting_record(
    txn: &cntryl_midge::Transaction,
    entry: &super::PostingEntry,
    cache: &mut CachedGlobalPostingPage,
) -> Result<Option<super::CompactGlobalPageRecord>, String> {
    if cache.as_ref().map(|(start, _)| *start) != Some(entry.parent_fragment_start) {
        let key = encode_compact_global_page_key(entry.parent_fragment_start);
        *cache = txn
            .get(&key)
            .map_err(|error| format!("read global fragment failed: {error:?}"))?
            .map(|value| {
                CompactGlobalPageValue::try_decode(&value)
                    .map(|page| (entry.parent_fragment_start, page))
            })
            .transpose()?;
    }
    let index = usize::try_from(entry.offset.saturating_sub(entry.parent_fragment_start))
        .unwrap_or(usize::MAX);
    Ok(cache
        .as_ref()
        .and_then(|(_, page)| page.records.get(index))
        .cloned())
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
    prefix.truncate(prefix.len().saturating_sub(8));
    Some((start_key, prefix))
}

impl StreamStore {
    /// Reads one resource name across a realm through its sparse posting.
    ///
    /// # Errors
    ///
    /// Returns an error when activation, posting/page reads, decoding, or
    /// discriminator loading fails.
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
        prefix.truncate(prefix.len().saturating_sub(8));
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|error| format!("begin realm posting read failed: {error:?}"))?;
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
                let Some(record) = realm_posting_record(&txn, realm, &entry, &mut cached_parent)?
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
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|error| format!("begin global posting read failed: {error:?}"))?;
        let (rows, fragments_exhausted) = bounded_posting_rows(&txn, start_key, prefix, "global")?;
        let item_limit = read_limit_to_usize(limit);
        let byte_limit = max_bytes.unwrap_or(usize::MAX);
        let mut items = Vec::with_capacity(item_limit.min(1_000));
        let mut bytes_read = 0usize;
        let mut last_examined = from_offset;
        let mut has_more = false;
        let mut examined = 0usize;
        let mut cached_parent = None;
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
        prefix.truncate(prefix.len().saturating_sub(16));
        let page_start = from_offset / GLOBAL_PAGE_RECORD_LIMIT * GLOBAL_PAGE_RECORD_LIMIT;
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|error| format!("begin global read failed: {error:?}"))?;
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
        'pages: for (key, value) in rows {
            let first = decode_realm_offset_from_key(&key)?;
            let page = CompactGlobalPageValue::try_decode(&value)?;
            for (index, record) in page.records.into_iter().enumerate() {
                let offset = first.saturating_add(index as u64);
                if offset < from_offset {
                    continue;
                }
                if offset >= watermark {
                    break 'pages;
                }
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

    /// Read resource stream records
    ///
    /// **NO WATERMARK GATING**: Resource reads are strictly ordered by `StreamActor`.
    /// Each resource offset is durably committed before being visible.
    /// Watermark is only relevant for area/realm dimensions.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, storage transaction creation,
    /// page scanning, or page decoding fails.
    pub fn read_resource(
        &self,
        params: &ReadResourceParams,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        self.read_resource_with_filter(params, None)
    }

    /// Read resource stream records with an optional server-side discriminator filter.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, storage transaction creation,
    /// page scanning, page decoding, or discriminator loading fails.
    pub fn read_resource_with_filter(
        &self,
        params: &ReadResourceParams,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        self.ensure_layout_activation_for_family(params.family)?;

        if params.limit == 0 {
            return Ok((
                Vec::new(),
                ReadCursor {
                    last_resource_offset: params.from_offset,
                    last_area_offset: None,
                    last_realm_offset: None,
                    last_global_offset: None,
                    has_more: false,
                    cursor_fingerprint: None,
                    captured_watermark: None,
                },
            ));
        }

        self.read_resource_promotion_frontier(params, filter)
    }

    pub(super) fn read_resource_promotion_frontier(
        &self,
        params: &ReadResourceParams,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        let query = resource_page_query(params);
        let route = crate::runtime::routing::Route::new(format!(
            "stream://{}/{}/{}",
            params.realm, params.area, params.resource
        ));

        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(params.family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        let iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        let results = iter
            .try_collect()
            .map_err(|e| format!("scan error: {e:?}"))?;

        let limit = read_limit_to_usize(params.limit);
        let mut items = Vec::with_capacity(limit.min(1000));
        let mut total_bytes = 0usize;
        let mut cursor = ReadCursorState {
            last_resource_offset: params.from_offset,
            last_area_offset: None,
            last_realm_offset: None,
            last_global_offset: None,
        };
        let max_bytes_limit = params.max_bytes.unwrap_or(usize::MAX);
        let mut has_more = false;

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_resource_offset_from_key(&key_bytes)?;
            let page = CompactResourcePageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_resource_page_error(
                    params.realm,
                    params.area,
                    params.resource,
                    page_start,
                    &error,
                )
            })?;

            let mut state = ReadPageState {
                from_offset: params.from_offset,
                limit,
                max_bytes_limit,
                watermark: None,
                filter,
                cursor: &mut cursor,
                total_bytes: &mut total_bytes,
                items: &mut items,
                has_more: &mut has_more,
            };
            let stop_scan = collect_filtered_read_page_items(
                page.records
                    .into_iter()
                    .enumerate()
                    .map(|(slot, page_record)| (page_slot_offset(page_start, slot), page_record)),
                &mut state,
                |resource_offset, _page_record| {
                    Self::load_optional_discriminator(
                        &txn,
                        &crate::domains::stream::storage::encode_resource_discriminator_key(
                            params.realm,
                            params.area,
                            params.resource,
                            resource_offset,
                        ),
                    )
                },
                resource_page_record_bytes,
                update_resource_cursor,
                |offset, _page_record| StreamReadItem::Filtered {
                    route: route.clone(),
                    offset,
                    reason: Some(StreamFilteredReason::ServerFilter),
                },
                |resource_offset, page_record| {
                    StreamReadItem::Event(StreamRecord {
                        route: route.clone(),
                        resource_offset,
                        area_offset: Some(page_record.area_offset),
                        realm_offset: Some(page_record.realm_offset),
                        global_offset: None,
                        body: page_record.body,
                        metadata: page_record.metadata,
                        created_at: page_record.created_at,
                    })
                },
            )?;

            if stop_scan {
                break 'page_scan;
            }
        }

        Ok((items, cursor.into_cursor(has_more)))
    }

    /// Read area stream records up to the current area watermark.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, watermark loading, storage
    /// transaction creation, page scanning, or page decoding fails.
    pub fn read_area(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        let params = ReadAreaParams {
            family,
            realm,
            area,
            from_offset,
            limit,
            max_bytes,
        };
        self.read_area_with_filter(&params, None)
    }

    pub(crate) fn read_area_with_filter(
        &self,
        params: &ReadAreaParams<'_>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        self.ensure_layout_activation_for_family(params.family)?;

        if params.limit == 0 {
            return Ok((
                Vec::new(),
                ReadCursor {
                    last_resource_offset: 0,
                    last_area_offset: Some(params.from_offset),
                    last_realm_offset: None,
                    last_global_offset: None,
                    has_more: false,
                    cursor_fingerprint: None,
                    captured_watermark: None,
                },
            ));
        }

        self.read_area_promotion_frontier(params, filter)
    }

    pub(super) fn read_area_promotion_frontier(
        &self,
        params: &ReadAreaParams<'_>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        let watermark = self.get_watermark(params.family, params.realm, params.area)?;
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(params.family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        let (results, fragments_exhausted) = bounded_fragment_rows(
            &txn,
            encode_compact_area_page_key(
                params.realm,
                params.area,
                Self::page_start_offset(params.from_offset),
            ),
            Self::build_compact_area_page_prefix(params.realm, params.area),
            "area",
            broad_scope_fragment_rows(params.from_offset, params.limit),
        )?;

        let limit = read_limit_to_usize(params.limit);
        let mut items = Vec::with_capacity(limit.min(1000));
        let mut total_bytes = 0usize;
        let mut cursor = ReadCursorState {
            last_resource_offset: 0,
            last_area_offset: Some(params.from_offset),
            last_realm_offset: None,
            last_global_offset: None,
        };
        let max_bytes_limit = params.max_bytes.unwrap_or(usize::MAX);
        let mut has_more = false;

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_area_offset_from_key(&key_bytes)?;
            let page = CompactAreaPageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_area_page_error(params.realm, params.area, page_start, &error)
            })?;

            let mut state = ReadPageState {
                from_offset: params.from_offset,
                limit,
                max_bytes_limit,
                watermark: Some(watermark),
                filter,
                cursor: &mut cursor,
                total_bytes: &mut total_bytes,
                items: &mut items,
                has_more: &mut has_more,
            };
            let stop_scan = collect_filtered_read_page_items(
                page.records
                    .into_iter()
                    .enumerate()
                    .map(|(slot, page_record)| (page_slot_offset(page_start, slot), page_record)),
                &mut state,
                |area_offset, _page_record| {
                    Self::load_optional_discriminator(
                        &txn,
                        &crate::domains::stream::storage::encode_area_discriminator_key(
                            params.realm,
                            params.area,
                            area_offset,
                        ),
                    )
                },
                area_page_record_bytes,
                update_area_cursor,
                |offset, page_record| StreamReadItem::Filtered {
                    route: crate::runtime::routing::Route::new(format!(
                        "stream://{}/{}/{}",
                        params.realm, params.area, page_record.resource
                    )),
                    offset,
                    reason: Some(StreamFilteredReason::ServerFilter),
                },
                |area_offset, page_record| {
                    StreamReadItem::Event(StreamRecord {
                        route: crate::runtime::routing::Route::new(format!(
                            "stream://{}/{}/{}",
                            params.realm, params.area, page_record.resource
                        )),
                        resource_offset: page_record.resource_offset,
                        area_offset: Some(area_offset),
                        realm_offset: None,
                        global_offset: None,
                        body: page_record.body,
                        metadata: page_record.metadata,
                        created_at: page_record.created_at,
                    })
                },
            )?;

            if stop_scan {
                break 'page_scan;
            }
        }

        if fragments_exhausted {
            has_more = true;
        }

        Ok((items, cursor.into_cursor(has_more)))
    }

    /// Read realm stream records up to the current realm watermark.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, realm watermark loading,
    /// storage transaction creation, page scanning, or page decoding fails.
    pub fn read_realm(
        &self,
        family: u64,
        realm: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        self.read_realm_with_filter(family, realm, from_offset, limit, max_bytes, None)
    }

    /// Read realm stream records with an optional server-side discriminator filter.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, realm watermark loading,
    /// storage transaction creation, page scanning, page decoding, or
    /// discriminator loading fails.
    pub fn read_realm_with_filter(
        &self,
        family: u64,
        realm: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        self.ensure_layout_activation_for_family(family)?;

        if limit == 0 {
            return Ok((
                Vec::new(),
                ReadCursor {
                    last_resource_offset: 0,
                    last_area_offset: None,
                    last_realm_offset: Some(from_offset),
                    last_global_offset: None,
                    has_more: false,
                    cursor_fingerprint: None,
                    captured_watermark: None,
                },
            ));
        }

        self.read_realm_promotion_frontier(family, realm, from_offset, limit, max_bytes, filter)
    }

    pub(super) fn read_realm_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        let realm_watermark = self.get_realm_watermark(family, realm)?;
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        let (results, fragments_exhausted) = bounded_fragment_rows(
            &txn,
            encode_compressed_compact_realm_page_key(realm, Self::page_start_offset(from_offset)),
            Self::build_compressed_compact_realm_page_prefix(realm),
            "realm",
            broad_scope_fragment_rows(from_offset, limit),
        )?;

        let limit = read_limit_to_usize(limit);
        let mut items = Vec::with_capacity(limit.min(1000));
        let mut total_bytes = 0usize;
        let mut cursor = ReadCursorState {
            last_resource_offset: 0,
            last_area_offset: None,
            last_realm_offset: Some(from_offset),
            last_global_offset: None,
        };
        let max_bytes_limit = max_bytes.unwrap_or(usize::MAX);
        let mut has_more = false;

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_realm_offset_from_key(&key_bytes)?;
            let page = CompressedCompactRealmPageValue::try_decode(&value_bytes)
                .map_err(|error| Self::invalid_compact_realm_page_error(page_start, &error))?
                .into_compact_realm_page();

            let mut state = ReadPageState {
                from_offset,
                limit,
                max_bytes_limit,
                watermark: Some(realm_watermark),
                filter,
                cursor: &mut cursor,
                total_bytes: &mut total_bytes,
                items: &mut items,
                has_more: &mut has_more,
            };
            let stop_scan = collect_filtered_read_page_items(
                page.records
                    .into_iter()
                    .enumerate()
                    .map(|(slot, page_record)| (page_slot_offset(page_start, slot), page_record)),
                &mut state,
                |realm_offset, _page_record| {
                    Self::load_optional_discriminator(
                        &txn,
                        &crate::domains::stream::storage::encode_realm_discriminator_key(
                            realm,
                            realm_offset,
                        ),
                    )
                },
                realm_page_record_bytes,
                update_realm_cursor,
                |offset, page_record| StreamReadItem::Filtered {
                    route: crate::runtime::routing::Route::new(format!(
                        "stream://{}/{}/{}",
                        realm, page_record.area, page_record.resource
                    )),
                    offset,
                    reason: Some(StreamFilteredReason::ServerFilter),
                },
                |realm_offset, page_record| {
                    StreamReadItem::Event(StreamRecord {
                        route: crate::runtime::routing::Route::new(format!(
                            "stream://{}/{}/{}",
                            realm, page_record.area, page_record.resource
                        )),
                        resource_offset: page_record.resource_offset,
                        area_offset: Some(page_record.area_offset),
                        realm_offset: Some(realm_offset),
                        global_offset: None,
                        body: page_record.body,
                        metadata: page_record.metadata,
                        created_at: page_record.created_at,
                    })
                },
            )?;

            if stop_scan {
                break 'page_scan;
            }
        }

        if fragments_exhausted {
            has_more = true;
        }

        Ok((items, cursor.into_cursor(has_more)))
    }
}
