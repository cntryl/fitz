use super::read_support::{
    begin_read_tx, bounded_fragment_rows, broad_scope_fragment_rows, hydrate_area_locator,
    hydrate_realm_locator, page_slot_offset, resolve_blob_payload, validate_fragment_range,
    GlobalFragmentCache,
};
use super::{
    area_page_record_bytes, collect_filtered_read_page_items, decode_area_offset_from_key,
    decode_realm_offset_from_key, decode_resource_offset_from_key, encode_compact_area_page_key,
    encode_compact_resource_page_key, encode_compressed_compact_realm_page_key,
    read_limit_to_usize, realm_page_record_bytes, record_is_expired, resource_page_record_bytes,
    update_area_cursor, update_realm_cursor, update_resource_cursor, CompactAreaPageValue,
    CompactResourcePageValue, CompressedCompactRealmPageValue, ReadAreaParams, ReadCursorState,
    ReadPageState, ReadResourceParams, StreamFilterSet, StreamFilteredReason, StreamReadItem,
    StreamRecord, StreamStore,
};
use crate::domains::stream::protocol::ReadCursor;

fn stream_route(realm: &str, area: &str, resource: &str) -> crate::runtime::routing::Route {
    crate::runtime::routing::Route::new(format!("stream://{realm}/{area}/{resource}"))
}

impl StreamStore {
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

    #[allow(clippy::too_many_lines)]
    pub(super) fn read_resource_promotion_frontier(
        &self,
        params: &ReadResourceParams,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        let route = stream_route(params.realm, params.area, params.resource);

        let txn = begin_read_tx(self, params.family, "resource")?;
        let (results, fragments_exhausted) = bounded_fragment_rows(
            &txn,
            encode_compact_resource_page_key(
                params.realm,
                params.area,
                params.resource,
                Self::page_start_offset(params.from_offset),
            ),
            Self::build_compact_resource_page_prefix(params.realm, params.area, params.resource),
            "resource",
            broad_scope_fragment_rows(params.from_offset, params.limit),
        )?;

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
        let mut previous_fragment_end = None;
        let now_epoch_ms = self.now_epoch_ms();

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
            validate_fragment_range(
                "resource",
                page_start,
                page.records.len(),
                &mut previous_fragment_end,
            )?;
            let mut page_records = Vec::with_capacity(page.records.len());
            for (slot, mut page_record) in page.records.into_iter().enumerate() {
                let offset = page_slot_offset(page_start, slot);
                if record_is_expired(page_record.expires_at, now_epoch_ms) {
                    update_resource_cursor(&mut cursor, offset, &page_record);
                    continue;
                }
                resolve_blob_payload(&txn, &mut page_record.body, &mut page_record.metadata)?;
                page_records.push((offset, page_record));
            }

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
                page_records,
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

        if fragments_exhausted {
            has_more = true;
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

    #[allow(clippy::too_many_lines)]
    pub(super) fn read_area_promotion_frontier(
        &self,
        params: &ReadAreaParams<'_>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<(Vec<StreamReadItem>, ReadCursor), String> {
        let watermark = self.get_watermark(params.family, params.realm, params.area)?;
        let txn = begin_read_tx(self, params.family, "area")?;
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
        let mut previous_fragment_end = None;
        let mut global_cache = GlobalFragmentCache::new();
        let now_epoch_ms = self.now_epoch_ms();

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_area_offset_from_key(&key_bytes)?;
            let page = CompactAreaPageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_area_page_error(params.realm, params.area, page_start, &error)
            })?;
            validate_fragment_range(
                "area",
                page_start,
                page.records.len(),
                &mut previous_fragment_end,
            )?;
            let mut page_records = Vec::with_capacity(page.records.len());
            for (slot, mut page_record) in page.records.into_iter().enumerate() {
                let offset = page_slot_offset(page_start, slot);
                if record_is_expired(page_record.expires_at, now_epoch_ms) {
                    update_area_cursor(&mut cursor, offset, &page_record);
                    continue;
                }
                hydrate_area_locator(
                    &txn,
                    params.realm,
                    params.area,
                    &mut page_record,
                    &mut global_cache,
                )?;
                page_records.push((offset, page_record));
            }

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
                page_records,
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
                    route: stream_route(params.realm, params.area, &page_record.resource),
                    offset,
                    reason: Some(StreamFilteredReason::ServerFilter),
                },
                |area_offset, page_record| {
                    StreamReadItem::Event(StreamRecord {
                        route: stream_route(params.realm, params.area, &page_record.resource),
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

    #[allow(clippy::too_many_lines)]
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
        let txn = begin_read_tx(self, family, "realm")?;
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
        let mut previous_fragment_end = None;
        let mut global_cache = GlobalFragmentCache::new();
        let now_epoch_ms = self.now_epoch_ms();

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_realm_offset_from_key(&key_bytes)?;
            let page = CompressedCompactRealmPageValue::try_decode(&value_bytes)
                .map_err(|error| Self::invalid_compact_realm_page_error(page_start, &error))?
                .into_compact_realm_page();
            validate_fragment_range(
                "realm",
                page_start,
                page.records.len(),
                &mut previous_fragment_end,
            )?;
            let mut page_records = Vec::with_capacity(page.records.len());
            for (slot, mut page_record) in page.records.into_iter().enumerate() {
                let offset = page_slot_offset(page_start, slot);
                if record_is_expired(page_record.expires_at, now_epoch_ms) {
                    update_realm_cursor(&mut cursor, offset, &page_record);
                    continue;
                }
                hydrate_realm_locator(&txn, realm, &mut page_record, &mut global_cache)?;
                page_records.push((offset, page_record));
            }

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
                page_records,
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
