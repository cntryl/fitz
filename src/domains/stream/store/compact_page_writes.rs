use super::*;

impl StreamStore {
    pub(super) fn build_realm_page_records(
        events: &[EventPayload],
        first_resource_offset: u64,
        first_area_offset: u64,
        created_at: u64,
    ) -> Vec<CompactRealmPageRecord> {
        let mut realm_records = Vec::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            let resource_offset = first_resource_offset + index as u64;
            let area_offset = first_area_offset + index as u64;

            realm_records.push(CompactRealmPageRecord {
                area_offset,
                resource_offset,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            });
        }

        realm_records
    }

    pub(super) fn build_promotion_frontier_area_records(
        events: &[EventPayload],
        first_resource_offset: u64,
        created_at: u64,
    ) -> Vec<CompactAreaPageRecord> {
        let mut records = Vec::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            records.push(CompactAreaPageRecord {
                resource_offset: first_resource_offset + index as u64,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            });
        }

        records
    }

    pub(super) fn build_promotion_frontier_resource_records(
        events: &[EventPayload],
        first_area_offset: u64,
        first_realm_offset: u64,
        created_at: u64,
    ) -> Vec<CompactResourcePageRecord> {
        let mut records = Vec::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            records.push(CompactResourcePageRecord {
                area_offset: first_area_offset + index as u64,
                realm_offset: first_realm_offset + index as u64,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            });
        }

        records
    }

    pub(super) fn load_compact_area_page_for_write(
        txn: &cntryl_midge::Transaction,
        realm: &str,
        area: &str,
        page_start_offset: u64,
    ) -> Result<CompactAreaPageValue, String> {
        match txn
            .get(&encode_compact_area_page_key(
                realm,
                area,
                page_start_offset,
            ))
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(value_bytes) => CompactAreaPageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_area_page_error(realm, area, page_start_offset, error)
            }),
            None => Ok(CompactAreaPageValue {
                records: Vec::new(),
            }),
        }
    }

    pub(super) fn write_compact_area_records(
        txn: &mut cntryl_midge::Transaction,
        realm: &str,
        area: &str,
        first_area_offset: u64,
        records: &[CompactAreaPageRecord],
        ttl_opt: Option<u64>,
    ) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        let mut next_record_index = 0usize;
        let mut current_area_offset = first_area_offset;

        while next_record_index < records.len() {
            let page_start_offset = Self::page_start_offset(current_area_offset);
            let page_offset = (current_area_offset - page_start_offset) as usize;
            let mut page =
                Self::load_compact_area_page_for_write(txn, realm, area, page_start_offset)?;

            if page.records.len() != page_offset {
                return Err("ERR_OVERLAPPING_COMPACT_AREA_PAGE_APPEND".to_string());
            }

            let append_count =
                (REALM_PAGE_RECORD_LIMIT - page_offset).min(records.len() - next_record_index);
            page.records
                .extend_from_slice(&records[next_record_index..next_record_index + append_count]);

            txn.put(
                encode_compact_area_page_key(realm, area, page_start_offset),
                page.encode(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {:?}", e))?;

            next_record_index += append_count;
            current_area_offset = current_area_offset.saturating_add(append_count as u64);
        }

        Ok(())
    }

    pub(super) fn load_compact_resource_page_for_write(
        txn: &cntryl_midge::Transaction,
        realm: &str,
        area: &str,
        resource: &str,
        page_start_offset: u64,
    ) -> Result<CompactResourcePageValue, String> {
        match txn
            .get(&encode_compact_resource_page_key(
                realm,
                area,
                resource,
                page_start_offset,
            ))
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(value_bytes) => {
                CompactResourcePageValue::try_decode(&value_bytes).map_err(|error| {
                    Self::invalid_compact_resource_page_error(
                        realm,
                        area,
                        resource,
                        page_start_offset,
                        error,
                    )
                })
            }
            None => Ok(CompactResourcePageValue {
                records: Vec::new(),
            }),
        }
    }

    pub(super) fn write_compact_resource_records(
        txn: &mut cntryl_midge::Transaction,
        realm: &str,
        area: &str,
        resource: &str,
        first_resource_offset: u64,
        records: &[CompactResourcePageRecord],
        ttl_opt: Option<u64>,
    ) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        let mut next_record_index = 0usize;
        let mut current_resource_offset = first_resource_offset;

        while next_record_index < records.len() {
            let page_start_offset = Self::page_start_offset(current_resource_offset);
            let page_offset = (current_resource_offset - page_start_offset) as usize;
            let mut page = Self::load_compact_resource_page_for_write(
                txn,
                realm,
                area,
                resource,
                page_start_offset,
            )?;

            if page.records.len() != page_offset {
                return Err("ERR_OVERLAPPING_COMPACT_RESOURCE_PAGE_APPEND".to_string());
            }

            let append_count =
                (REALM_PAGE_RECORD_LIMIT - page_offset).min(records.len() - next_record_index);
            page.records
                .extend_from_slice(&records[next_record_index..next_record_index + append_count]);

            txn.put(
                encode_compact_resource_page_key(realm, area, resource, page_start_offset),
                page.encode(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {:?}", e))?;

            next_record_index += append_count;
            current_resource_offset = current_resource_offset.saturating_add(append_count as u64);
        }

        Ok(())
    }

    pub(super) fn load_compressed_compact_realm_page_for_write(
        txn: &cntryl_midge::Transaction,
        realm: &str,
        page_start_offset: u64,
    ) -> Result<CompressedCompactRealmPageValue, String> {
        match txn
            .get(&encode_compressed_compact_realm_page_key(
                realm,
                page_start_offset,
            ))
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(value_bytes) => CompressedCompactRealmPageValue::try_decode(&value_bytes)
                .map_err(|error| Self::invalid_compact_realm_page_error(page_start_offset, error)),
            None => Ok(CompressedCompactRealmPageValue {
                records: Vec::new(),
            }),
        }
    }

    pub(super) fn write_compressed_compact_realm_records(
        txn: &mut cntryl_midge::Transaction,
        realm: &str,
        first_realm_offset: u64,
        records: &[CompactRealmPageRecord],
        ttl_opt: Option<u64>,
    ) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        let mut next_record_index = 0usize;
        let mut current_realm_offset = first_realm_offset;

        while next_record_index < records.len() {
            let page_start_offset = Self::page_start_offset(current_realm_offset);
            let page_offset = (current_realm_offset - page_start_offset) as usize;
            let mut page =
                Self::load_compressed_compact_realm_page_for_write(txn, realm, page_start_offset)?;

            if page.records.len() != page_offset {
                return Err("ERR_OVERLAPPING_COMPRESSED_COMPACT_REALM_PAGE_APPEND".to_string());
            }

            let append_count =
                (REALM_PAGE_RECORD_LIMIT - page_offset).min(records.len() - next_record_index);
            page.records
                .extend_from_slice(&records[next_record_index..next_record_index + append_count]);

            txn.put(
                encode_compressed_compact_realm_page_key(realm, page_start_offset),
                page.encode(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {:?}", e))?;

            next_record_index += append_count;
            current_realm_offset = current_realm_offset.saturating_add(append_count as u64);
        }

        Ok(())
    }

    pub(super) fn write_promotion_frontier_event_rows(
        &self,
        txn: &mut cntryl_midge::Transaction,
        params: PromotionFrontierWriteRowsParams<'_>,
    ) -> Result<(), String> {
        let PromotionFrontierWriteRowsParams {
            realm,
            area,
            resource,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            events,
            created_at,
        } = params;
        let resource_records = Self::build_promotion_frontier_resource_records(
            events,
            first_area_offset,
            first_realm_offset,
            created_at,
        );
        Self::write_compact_resource_records(
            txn,
            realm,
            area,
            resource,
            first_resource_offset,
            &resource_records,
            self.ttl.ttl_seconds,
        )?;

        let area_records =
            Self::build_promotion_frontier_area_records(events, first_resource_offset, created_at);
        Self::write_compact_area_records(
            txn,
            realm,
            area,
            first_area_offset,
            &area_records,
            self.ttl.ttl_seconds,
        )?;

        let realm_records = Self::build_realm_page_records(
            events,
            first_resource_offset,
            first_area_offset,
            created_at,
        );
        Self::write_compressed_compact_realm_records(
            txn,
            realm,
            first_realm_offset,
            &realm_records,
            self.ttl.ttl_seconds,
        )
    }

    pub(super) fn commit_promotion_frontier_batch(
        &self,
        params: CommitPromotionFrontierBatchParams<'_>,
    ) -> Result<(CommitResponse, ResourceMetaValue), String> {
        let CommitPromotionFrontierBatchParams {
            family,
            realm,
            area,
            resource,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            events,
            committed_size_before,
            ingest_metadata,
            mode,
        } = params;
        let created_at = Self::now_epoch_ms();
        let batch_size = events.len();
        let batch_size_u64 = batch_size as u64;
        let last_resource_offset = first_resource_offset + batch_size_u64 - 1;
        let last_area_offset = first_area_offset + batch_size_u64 - 1;
        let last_realm_offset = first_realm_offset + batch_size_u64 - 1;
        let committed_size_delta = events.iter().map(Self::event_size_bytes).sum::<u64>();

        let mut txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;
        self.write_promotion_frontier_event_rows(
            &mut txn,
            PromotionFrontierWriteRowsParams {
                realm,
                area,
                resource,
                first_resource_offset,
                first_area_offset,
                first_realm_offset,
                events,
                created_at,
            },
        )?;
        Self::write_discriminator_rows(
            &mut txn,
            DiscriminatorWriteRowsParams {
                realm,
                area,
                resource,
                first_resource_offset,
                first_area_offset,
                first_realm_offset,
                events,
                ttl_opt: self.ttl.ttl_seconds,
            },
        )?;

        let resource_meta_after = ResourceMetaValue {
            next_offset: last_resource_offset.saturating_add(1),
            committed_size_bytes: committed_size_before.saturating_add(committed_size_delta),
        };
        txn.put(
            encode_resource_meta_key(realm, area, resource),
            resource_meta_after.encode(),
            None,
        )
        .map_err(|e| format!("txn put failed: {:?}", e))?;

        txn.put(
            encode_area_counter_key(realm, area),
            AreaCounterValue {
                next_offset: last_area_offset.saturating_add(1),
            }
            .encode(),
            None,
        )
        .map_err(|e| format!("txn put failed: {:?}", e))?;

        txn.put(
            encode_realm_counter_key(realm),
            RealmCounterValue {
                next_offset: last_realm_offset.saturating_add(1),
            }
            .encode(),
            None,
        )
        .map_err(|e| format!("txn put failed: {:?}", e))?;

        let write_options = match mode {
            StreamWriteMode::Sync => cntryl_midge::WriteOptions::sync(),
            StreamWriteMode::Buffered => cntryl_midge::WriteOptions::buffered(),
            StreamWriteMode::CloudStrict => cntryl_midge::WriteOptions::cloud_strict(),
        };
        #[cfg(test)]
        {
            let should_fail = FAIL_NEXT_PROMOTION_FRONTIER_COMMIT.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                return Err("Injected stream commit failure".to_string());
            }
        }
        txn.commit(write_options)
            .map_err(|e| format!("midge commit error: {:?}", e))?;

        Ok((
            CommitResponse {
                first_resource_offset,
                last_resource_offset,
                first_area_offset,
                last_area_offset,
                first_realm_offset,
                last_realm_offset,
                batch_size,
                ingest_metadata,
            },
            resource_meta_after,
        ))
    }
}
