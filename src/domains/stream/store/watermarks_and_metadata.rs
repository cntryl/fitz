use super::*;

impl StreamStore {
    pub fn get_watermark(&self, family: u64, realm: &str, area: &str) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = encode_watermark_key(realm, area);
        let counter_key = encode_area_counter_key(realm, area);

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        match txn
            .get(&key)
            .map_err(|e| format!("midge get error: {:?}", e))?
        {
            Some(bytes) => {
                let value = WatermarkValue::decode(&bytes)?;
                Ok(value.watermark)
            }
            None => match txn
                .get(&counter_key)
                .map_err(|e| format!("midge get error: {:?}", e))?
            {
                Some(bytes) => Ok(AreaCounterValue::decode(&bytes)?
                    .next_offset
                    .saturating_sub(1)),
                None => Ok(self
                    .scan_next_area_offset(family, realm, area)?
                    .saturating_sub(1)),
            },
        }
    }

    pub fn set_watermark(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        watermark: u64,
    ) -> Result<(), String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = encode_watermark_key(realm, area);
        let counter_key = encode_area_counter_key(realm, area);

        let mut txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        // Monotonicity guard: watermarks must only advance, never regress.
        // If the new value is not strictly greater than the current value, no-op.
        let current =
            Self::load_effective_area_watermark_for_guard(&txn, realm, area, &key, &counter_key)?;
        if watermark <= current {
            return Ok(());
        }

        let value = WatermarkValue { watermark };
        txn.put(key, value.encode(), None)
            .map_err(|e| format!("txn put failed: {:?}", e))?;
        let opts = cntryl_midge::WriteOptions::sync();
        txn.commit(opts)
            .map_err(|e| format!("midge commit error: {:?}", e))
    }

    pub fn get_realm_watermark(&self, family: u64, realm: &str) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = crate::domains::stream::storage::encode_realm_watermark_key(realm);
        let counter_key = encode_realm_counter_key(realm);

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        match txn
            .get(&key)
            .map_err(|e| format!("midge get error: {:?}", e))?
        {
            Some(bytes) => {
                let value = WatermarkValue::decode(&bytes)?;
                Ok(value.watermark)
            }
            None => match txn
                .get(&counter_key)
                .map_err(|e| format!("midge get error: {:?}", e))?
            {
                Some(bytes) => Ok(RealmCounterValue::decode(&bytes)?
                    .next_offset
                    .saturating_sub(1)),
                None => Ok(self
                    .scan_next_realm_offset(family, realm)?
                    .saturating_sub(1)),
            },
        }
    }

    pub fn set_realm_watermark(
        &self,
        family: u64,
        realm: &str,
        watermark: u64,
    ) -> Result<(), String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = crate::domains::stream::storage::encode_realm_watermark_key(realm);
        let counter_key = encode_realm_counter_key(realm);

        let mut txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        // Monotonicity guard: realm watermarks must only advance, never regress.
        let current =
            Self::load_effective_realm_watermark_for_guard(&txn, realm, &key, &counter_key)?;
        if watermark <= current {
            return Ok(());
        }

        let value = WatermarkValue { watermark };
        txn.put(key, value.encode(), None)
            .map_err(|e| format!("txn put failed: {:?}", e))?;
        let opts = cntryl_midge::WriteOptions::sync();
        txn.commit(opts)
            .map_err(|e| format!("midge commit error: {:?}", e))
    }

    /// Get stream metadata (limits, TTL, offsets, watermarks)
    ///
    /// Used for describe_stream / introspection API
    pub fn get_metadata(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<crate::domains::stream::protocol::StreamMetadata, String> {
        let first_resource_offset =
            self.get_first_resource_offset(family, realm, area, resource)?;
        let last_resource_offset = self.get_last_resource_offset(family, realm, area, resource)?;
        let area_watermark = self.get_watermark(family, realm, area)?;
        let realm_watermark = self.get_realm_watermark(family, realm)?;
        let resource_count = match (first_resource_offset, last_resource_offset) {
            (Some(first_offset), Some(last_offset)) if last_offset >= first_offset => {
                last_offset - first_offset + 1
            }
            _ => 0,
        };

        Ok(crate::domains::stream::protocol::StreamMetadata {
            max_batch_events: self.limits.max_batch_events,
            max_batch_bytes: self.limits.max_batch_bytes,
            ttl_seconds: self.ttl.ttl_seconds,
            first_resource_offset,
            last_resource_offset,
            resource_count,
            area_watermark,
            realm_watermark,
        })
    }

    /// Get the last committed resource offset for recovery
    ///
    /// **CRITICAL**: StreamActor must call this on initialization to recover
    /// next_resource_offset and avoid reusing offsets after restart.
    pub fn get_last_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.get_last_resource_offset_promotion_frontier(family, realm, area, resource)
    }

    pub(in crate::domains::stream::store) fn get_last_resource_offset_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compact_resource_page_prefix(realm, area, resource),
        ));
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start = decode_resource_offset_from_key(key)?;
            let page = CompactResourcePageValue::try_decode(value).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, error)
            })?;

            if page.records.is_empty() {
                Ok(None)
            } else {
                Ok(Some(page_start + page.records.len() as u64 - 1))
            }
        } else {
            Ok(None)
        }
    }

    /// Get the first committed resource offset that is still readable.
    ///
    /// This is used by exact-resource metadata surfaces that need to remain
    /// truthful when older resource rows are trimmed from the head.
    pub fn get_first_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.get_first_resource_offset_promotion_frontier(family, realm, area, resource)
    }

    pub(super) fn get_first_resource_offset_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compact_resource_page_prefix(realm, area, resource),
        ));
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        for (key, value) in results {
            let page_start = decode_resource_offset_from_key(&key)?;
            let page = CompactResourcePageValue::try_decode(&value).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, error)
            })?;

            if !page.records.is_empty() {
                return Ok(Some(page_start));
            }
        }

        Ok(None)
    }

    /// Get the next resource offset from durable stream metadata.
    ///
    /// Returns 0 if no metadata exists for the resource yet.
    pub fn get_next_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.get_next_resource_offset_promotion_frontier(family, realm, area, resource)
    }

    pub(super) fn get_next_resource_offset_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        self.load_next_resource_offset_from_txn(&txn, family, realm, area, resource)
    }
}
