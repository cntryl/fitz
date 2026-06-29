use super::*;

impl StreamStore {
    /// Read resource stream records
    ///
    /// **NO WATERMARK GATING**: Resource reads are strictly ordered by StreamActor.
    /// Each resource offset is durably committed before being visible.
    /// Watermark is only relevant for area/realm dimensions.
    pub fn read_resource(
        &self,
        params: &ReadResourceParams,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        self.read_resource_with_filter(params, None)
    }

    pub fn read_resource_with_filter(
        &self,
        params: &ReadResourceParams,
        filter: Option<&StreamFilterSet>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        self.ensure_layout_activation_for_family(params.family)?;

        if params.limit == 0 {
            return Ok((
                Vec::new(),
                crate::domains::stream::protocol::ReadCursor {
                    last_resource_offset: params.from_offset,
                    last_area_offset: None,
                    last_realm_offset: None,
                    has_more: false,
                },
            ));
        }

        self.read_resource_promotion_frontier(params, filter)
    }

    pub(super) fn read_resource_promotion_frontier(
        &self,
        params: &ReadResourceParams,
        filter: Option<&StreamFilterSet>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_compact_resource_page_key(
                params.realm,
                params.area,
                params.resource,
                Self::page_start_offset(params.from_offset),
            )))
            .prefix(Bytes::from(Self::build_compact_resource_page_prefix(
                params.realm,
                params.area,
                params.resource,
            )))
            .limit(Self::compact_page_query_limit(
                params.from_offset,
                params.limit,
            ));

        let txn = self
            .db
            .begin_tx(
                params.family as u32,
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        let limit = params.limit as usize;
        let mut items = Vec::with_capacity(limit.min(1000));
        let mut total_bytes = 0usize;
        let mut cursor = ReadCursorState {
            last_resource_offset: params.from_offset,
            last_area_offset: None,
            last_realm_offset: None,
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
                    error,
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
                    .map(|(slot, page_record)| (page_start + slot as u64, page_record)),
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
                |offset| StreamReadItem::Filtered {
                    offset,
                    reason: Some(StreamFilteredReason::ServerFilter),
                },
                |resource_offset, page_record| {
                    StreamReadItem::Event(StreamRecord {
                        resource_offset,
                        area_offset: Some(page_record.area_offset),
                        realm_offset: Some(page_record.realm_offset),
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

    pub fn read_area(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
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
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        self.ensure_layout_activation_for_family(params.family)?;

        if params.limit == 0 {
            return Ok((
                Vec::new(),
                crate::domains::stream::protocol::ReadCursor {
                    last_resource_offset: 0,
                    last_area_offset: Some(params.from_offset),
                    last_realm_offset: None,
                    has_more: false,
                },
            ));
        }

        self.read_area_promotion_frontier(params, filter)
    }

    pub(super) fn read_area_promotion_frontier(
        &self,
        params: &ReadAreaParams<'_>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        let watermark = self.get_watermark(params.family, params.realm, params.area)?;
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_compact_area_page_key(
                params.realm,
                params.area,
                Self::page_start_offset(params.from_offset),
            )))
            .prefix(Bytes::from(Self::build_compact_area_page_prefix(
                params.realm,
                params.area,
            )))
            .limit(Self::compact_page_query_limit(
                params.from_offset,
                params.limit,
            ));

        let txn = self
            .db
            .begin_tx(
                params.family as u32,
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        let limit = params.limit as usize;
        let mut items = Vec::with_capacity(limit.min(1000));
        let mut total_bytes = 0usize;
        let mut cursor = ReadCursorState {
            last_resource_offset: 0,
            last_area_offset: Some(params.from_offset),
            last_realm_offset: None,
        };
        let max_bytes_limit = params.max_bytes.unwrap_or(usize::MAX);
        let mut has_more = false;

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_area_offset_from_key(&key_bytes)?;
            let page = CompactAreaPageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_area_page_error(params.realm, params.area, page_start, error)
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
                    .map(|(slot, page_record)| (page_start + slot as u64, page_record)),
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
                |offset| StreamReadItem::Filtered {
                    offset,
                    reason: Some(StreamFilteredReason::ServerFilter),
                },
                |area_offset, page_record| {
                    StreamReadItem::Event(StreamRecord {
                        resource_offset: page_record.resource_offset,
                        area_offset: Some(area_offset),
                        realm_offset: None,
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

    pub fn read_realm(
        &self,
        family: u64,
        realm: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        self.read_realm_with_filter(family, realm, from_offset, limit, max_bytes, None)
    }

    pub fn read_realm_with_filter(
        &self,
        family: u64,
        realm: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        self.ensure_layout_activation_for_family(family)?;

        if limit == 0 {
            return Ok((
                Vec::new(),
                crate::domains::stream::protocol::ReadCursor {
                    last_resource_offset: 0,
                    last_area_offset: None,
                    last_realm_offset: Some(from_offset),
                    has_more: false,
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
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        let realm_watermark = self.get_realm_watermark(family, realm)?;
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_compressed_compact_realm_page_key(
                realm,
                Self::page_start_offset(from_offset),
            )))
            .prefix(Bytes::from(
                Self::build_compressed_compact_realm_page_prefix(realm),
            ))
            .limit(Self::compact_page_query_limit(from_offset, limit));

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        let limit = limit as usize;
        let mut items = Vec::with_capacity(limit.min(1000));
        let mut total_bytes = 0usize;
        let mut cursor = ReadCursorState {
            last_resource_offset: 0,
            last_area_offset: None,
            last_realm_offset: Some(from_offset),
        };
        let max_bytes_limit = max_bytes.unwrap_or(usize::MAX);
        let mut has_more = false;

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_realm_offset_from_key(&key_bytes)?;
            let page = CompressedCompactRealmPageValue::try_decode(&value_bytes)
                .map_err(|error| Self::invalid_compact_realm_page_error(page_start, error))?
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
                    .map(|(slot, page_record)| (page_start + slot as u64, page_record)),
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
                |offset| StreamReadItem::Filtered {
                    offset,
                    reason: Some(StreamFilteredReason::ServerFilter),
                },
                |realm_offset, page_record| {
                    StreamReadItem::Event(StreamRecord {
                        resource_offset: page_record.resource_offset,
                        area_offset: Some(page_record.area_offset),
                        realm_offset: Some(realm_offset),
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
}
