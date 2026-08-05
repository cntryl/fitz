use super::{
    encode_area_counter_key, encode_compact_area_page_key, encode_compact_global_page_key,
    encode_compact_resource_page_key, encode_compressed_compact_realm_page_key,
    encode_family_writer_epoch_key, encode_global_area_posting_key,
    encode_global_area_resource_posting_key, encode_global_resource_posting_key,
    encode_realm_counter_key, encode_realm_resource_posting_key, encode_resource_meta_key,
    AreaCounterValue, CommitPromotionFrontierBatchParams, CommitResponse, CompactAreaPageRecord,
    CompactAreaPageValue, CompactGlobalPageRecord, CompactGlobalPageValue, CompactRealmPageRecord,
    CompactResourcePageRecord, CompactResourcePageValue, CompressedCompactRealmPageValue,
    DiscriminatorWriteRowsParams, EventPayload, PostingEntry, PostingPageValue,
    PromotionCommitFailure, PromotionFrontierWriteRowsParams, PromotionTransactionFailure,
    PromotionWriteFailure, RealmCounterValue, ResourceMetaValue, StreamStore, StreamWriteMode,
    GLOBAL_PAGE_RECORD_LIMIT, REALM_PAGE_RECORD_LIMIT,
};

#[cfg(test)]
use std::sync::atomic::Ordering;

fn u64_to_u32_saturating(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn posting_entry_pages(entries: &[PostingEntry], page_size: u64) -> Vec<&[PostingEntry]> {
    let mut pages = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let bucket = entries[start].offset / page_size;
        let end = entries[start..]
            .iter()
            .position(|entry| entry.offset / page_size != bucket)
            .map_or(entries.len(), |relative| start + relative);
        pages.push(&entries[start..end]);
        start = end;
    }
    pages
}

struct PromotionFrontierBatchWritePlan {
    created_at: u64,
    batch_size: usize,
    last_resource_offset: u64,
    last_area_offset: u64,
    last_realm_offset: u64,
    last_global_offset: u64,
    committed_size_after: u64,
}

impl StreamStore {
    fn verify_current_writer_epoch(
        &self,
        family: u64,
        expected: u64,
        epoch_key: &[u8],
    ) -> Result<(), PromotionWriteFailure> {
        let actual = self
            .db
            .begin_tx(
                u64_to_u32_saturating(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|error| {
                PromotionWriteFailure::Other(format!("begin epoch recheck failed: {error:?}"))
            })?
            .get(epoch_key)
            .map_err(|error| {
                PromotionWriteFailure::Other(format!(
                    "re-read family writer epoch failed: {error:?}"
                ))
            })?
            .map_or(Ok(0), |bytes| {
                RealmCounterValue::decode(&bytes).map(|value| value.next_offset)
            })?;
        if actual != expected {
            return Err(PromotionWriteFailure::WriterFenced);
        }
        Ok(())
    }

    fn build_global_page_records(
        params: &CommitPromotionFrontierBatchParams<'_>,
        created_at: u64,
    ) -> Vec<CompactGlobalPageRecord> {
        params
            .events
            .iter()
            .enumerate()
            .map(|(index, event)| CompactGlobalPageRecord {
                realm: params.realm.to_string(),
                area: params.area.to_string(),
                resource: params.resource.to_string(),
                resource_offset: params.first_resource_offset + index as u64,
                area_offset: params.first_area_offset + index as u64,
                realm_offset: params.first_realm_offset + index as u64,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            })
            .collect()
    }

    pub(super) fn build_realm_page_records(
        events: &[EventPayload],
        area: &str,
        resource: &str,
        first_resource_offset: u64,
        first_area_offset: u64,
        created_at: u64,
    ) -> Vec<CompactRealmPageRecord> {
        let mut realm_records = Vec::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            let resource_offset = first_resource_offset + index as u64;
            let area_offset = first_area_offset + index as u64;

            realm_records.push(CompactRealmPageRecord {
                area: area.to_string(),
                resource: resource.to_string(),
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
        resource: &str,
        first_resource_offset: u64,
        created_at: u64,
    ) -> Vec<CompactAreaPageRecord> {
        let mut records = Vec::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            records.push(CompactAreaPageRecord {
                resource: resource.to_string(),
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

        // Broad-scope writes are immutable commit fragments keyed by their
        // exact first offset. Do not align and merge these rows: concurrent
        // resources can commit into the same 64-offset bucket. Area readers
        // prefix-scan every fragment in offset order.
        let mut next_record_index = 0usize;
        let mut current_area_offset = first_area_offset;

        while next_record_index < records.len() {
            let page_start_offset = Self::page_start_offset(current_area_offset);
            let page_offset = u64_to_usize_saturating(current_area_offset - page_start_offset);
            let append_count =
                (REALM_PAGE_RECORD_LIMIT - page_offset).min(records.len() - next_record_index);
            txn.put(
                encode_compact_area_page_key(realm, area, current_area_offset),
                CompactAreaPageValue {
                    records: records[next_record_index..next_record_index + append_count].to_vec(),
                }
                .encode(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {e:?}"))?;

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
            .map_err(|e| format!("get error: {e:?}"))?
        {
            Some(value_bytes) => {
                CompactResourcePageValue::try_decode(&value_bytes).map_err(|error| {
                    Self::invalid_compact_resource_page_error(
                        realm,
                        area,
                        resource,
                        page_start_offset,
                        &error,
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
            let page_offset = u64_to_usize_saturating(current_resource_offset - page_start_offset);
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
            .map_err(|e| format!("txn put failed: {e:?}"))?;

            next_record_index += append_count;
            current_resource_offset = current_resource_offset.saturating_add(append_count as u64);
        }

        Ok(())
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

        // Realm posting entries retain this exact fragment start so filtered
        // reads can address immutable same-bucket fragments independently.
        let mut next_record_index = 0usize;
        let mut current_realm_offset = first_realm_offset;

        while next_record_index < records.len() {
            let page_start_offset = Self::page_start_offset(current_realm_offset);
            let page_offset = u64_to_usize_saturating(current_realm_offset - page_start_offset);
            let append_count =
                (REALM_PAGE_RECORD_LIMIT - page_offset).min(records.len() - next_record_index);
            txn.put(
                encode_compressed_compact_realm_page_key(realm, current_realm_offset),
                CompressedCompactRealmPageValue {
                    records: records[next_record_index..next_record_index + append_count].to_vec(),
                }
                .encode(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {e:?}"))?;

            next_record_index += append_count;
            current_realm_offset = current_realm_offset.saturating_add(append_count as u64);
        }

        Ok(())
    }

    pub(super) fn write_promotion_frontier_event_rows(
        &self,
        txn: &mut cntryl_midge::Transaction,
        params: &PromotionFrontierWriteRowsParams<'_>,
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
        } = *params;
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

        let area_records = Self::build_promotion_frontier_area_records(
            events,
            resource,
            first_resource_offset,
            created_at,
        );
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
            area,
            resource,
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
    ) -> Result<(CommitResponse, ResourceMetaValue), PromotionCommitFailure> {
        let plan = match Self::derive_promotion_frontier_batch_write_plan(&params) {
            Ok(plan) => plan,
            Err(error) => {
                let count = u64::try_from(params.events.len()).unwrap_or(u64::MAX);
                self.resolve_global_range(
                    params.family,
                    params.first_global_offset,
                    params.first_global_offset.saturating_add(count),
                )
                .map_err(PromotionCommitFailure::Resolved)?;
                return Err(PromotionCommitFailure::Resolved(error));
            }
        };
        let end_global_offset = plan.last_global_offset.saturating_add(1);
        let mut txn = match self.write_promotion_frontier_batch_rows(&params, &plan) {
            Ok(txn) => txn,
            Err(PromotionWriteFailure::WriterFenced) => {
                self.resolve_global_range(
                    params.family,
                    params.first_global_offset,
                    end_global_offset,
                )
                .map_err(PromotionCommitFailure::Resolved)?;
                return Err(PromotionCommitFailure::Resolved(
                    "ERR_STREAM_WRITER_FENCED".to_string(),
                ));
            }
            Err(PromotionWriteFailure::ScopeConflict) => {
                return Err(PromotionCommitFailure::ScopeConflict);
            }
            Err(PromotionWriteFailure::Other(error)) => {
                return Err(PromotionCommitFailure::Retryable(error));
            }
        };
        let resource_meta_after =
            Self::persist_promotion_frontier_counters_and_metadata(&mut txn, &params, &plan)
                .map_err(PromotionCommitFailure::Retryable)?;
        match self.commit_promotion_frontier_tx(txn, params.mode) {
            Ok(()) => {}
            Err(PromotionTransactionFailure::WriteConflict) => {
                return Err(PromotionCommitFailure::ScopeConflict);
            }
            Err(PromotionTransactionFailure::Other(error)) => {
                return Err(PromotionCommitFailure::Retryable(error));
            }
        }
        self.resolve_durable_global_range(
            params.family,
            params.first_global_offset,
            end_global_offset,
        );

        Ok((
            Self::build_promotion_frontier_commit_response(params, &plan),
            resource_meta_after,
        ))
    }

    fn derive_promotion_frontier_batch_write_plan(
        params: &CommitPromotionFrontierBatchParams<'_>,
    ) -> Result<PromotionFrontierBatchWritePlan, String> {
        let created_at = Self::now_epoch_ms();
        let batch_size = params.events.len();
        let batch_size_u64 =
            u64::try_from(batch_size).map_err(|_| "ERR_STREAM_OFFSET_EXHAUSTED".to_string())?;
        let next_resource_offset = params
            .first_resource_offset
            .checked_add(batch_size_u64)
            .ok_or_else(|| "ERR_STREAM_OFFSET_EXHAUSTED".to_string())?;
        let next_area_offset = params
            .first_area_offset
            .checked_add(batch_size_u64)
            .ok_or_else(|| "ERR_STREAM_OFFSET_EXHAUSTED".to_string())?;
        let next_realm_offset = params
            .first_realm_offset
            .checked_add(batch_size_u64)
            .ok_or_else(|| "ERR_STREAM_OFFSET_EXHAUSTED".to_string())?;
        let last_resource_offset = next_resource_offset - 1;
        let last_area_offset = next_area_offset - 1;
        let last_realm_offset = next_realm_offset - 1;
        let last_global_offset = params
            .first_global_offset
            .checked_add(batch_size_u64)
            .ok_or_else(|| "ERR_STREAM_OFFSET_EXHAUSTED".to_string())?
            - 1;
        let committed_size_delta = params
            .events
            .iter()
            .map(Self::event_size_bytes)
            .try_fold(0_u64, u64::checked_add)
            .ok_or_else(|| "ERR_STREAM_SIZE_EXHAUSTED".to_string())?;
        let committed_size_after = params
            .committed_size_before
            .checked_add(committed_size_delta)
            .ok_or_else(|| "ERR_STREAM_SIZE_EXHAUSTED".to_string())?;

        Ok(PromotionFrontierBatchWritePlan {
            created_at,
            batch_size,
            last_resource_offset,
            last_area_offset,
            last_realm_offset,
            last_global_offset,
            committed_size_after,
        })
    }

    fn write_promotion_frontier_batch_rows(
        &self,
        params: &CommitPromotionFrontierBatchParams<'_>,
        plan: &PromotionFrontierBatchWritePlan,
    ) -> Result<cntryl_midge::Transaction, PromotionWriteFailure> {
        let mut txn = self
            .db
            .begin_tx(
                u64_to_u32_saturating(params.family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|e| PromotionWriteFailure::Other(format!("begin_tx failed: {e:?}")))?;
        txn.set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);

        let epoch_key = encode_family_writer_epoch_key();
        let current_epoch = txn
            .get(&epoch_key)
            .map_err(|error| {
                PromotionWriteFailure::Other(format!("read family writer epoch failed: {error:?}"))
            })?
            .map_or(Ok(0), |bytes| {
                RealmCounterValue::decode(&bytes).map(|value| value.next_offset)
            })?;
        if current_epoch != params.writer_epoch {
            return Err(PromotionWriteFailure::WriterFenced);
        }
        txn.assert_value(
            epoch_key.clone(),
            Some(
                RealmCounterValue {
                    next_offset: params.writer_epoch,
                }
                .encode(),
            ),
        )
        .map_err(|error| {
            PromotionWriteFailure::Other(format!("assert family writer epoch failed: {error:?}"))
        })?;
        #[cfg(test)]
        if self
            .fence_next_global_reservation
            .swap(false, Ordering::AcqRel)
        {
            self.advance_family_writer_epoch(params.family)
                .map_err(PromotionWriteFailure::Other)?;
        }
        self.verify_current_writer_epoch(params.family, params.writer_epoch, &epoch_key)?;

        self.reserve_broad_scope_ranges(&mut txn, params, plan)?;

        self.write_promotion_frontier_event_rows(
            &mut txn,
            &PromotionFrontierWriteRowsParams {
                realm: params.realm,
                area: params.area,
                resource: params.resource,
                first_resource_offset: params.first_resource_offset,
                first_area_offset: params.first_area_offset,
                first_realm_offset: params.first_realm_offset,
                events: params.events,
                created_at: plan.created_at,
            },
        )?;
        Self::write_discriminator_rows(
            &mut txn,
            DiscriminatorWriteRowsParams {
                realm: params.realm,
                area: params.area,
                resource: params.resource,
                first_resource_offset: params.first_resource_offset,
                first_area_offset: params.first_area_offset,
                first_realm_offset: params.first_realm_offset,
                first_global_offset: params.first_global_offset,
                events: params.events,
                ttl_opt: self.ttl.ttl_seconds,
            },
        )?;
        let global_records = Self::build_global_page_records(params, plan.created_at);
        let mut record_index = 0usize;
        while record_index < global_records.len() {
            let fragment_start = params.first_global_offset + record_index as u64;
            let room = GLOBAL_PAGE_RECORD_LIMIT - fragment_start % GLOBAL_PAGE_RECORD_LIMIT;
            let count = usize::try_from(room)
                .unwrap_or(usize::MAX)
                .min(global_records.len() - record_index);
            txn.put(
                encode_compact_global_page_key(fragment_start),
                CompactGlobalPageValue {
                    records: global_records[record_index..record_index + count].to_vec(),
                }
                .encode(),
                self.ttl.ttl_seconds,
            )
            .map_err(|error| format!("write compact global page failed: {error:?}"))?;
            record_index += count;
        }

        Self::write_posting_rows(&mut txn, params, plan, self.ttl.ttl_seconds)?;

        Ok(txn)
    }

    fn reserve_broad_scope_ranges(
        &self,
        txn: &mut cntryl_midge::Transaction,
        params: &CommitPromotionFrontierBatchParams<'_>,
        plan: &PromotionFrontierBatchWritePlan,
    ) -> Result<(), PromotionWriteFailure> {
        let area_key = encode_area_counter_key(params.realm, params.area);
        let first_area_offset = txn
            .get(&area_key)
            .map_err(|error| {
                PromotionWriteFailure::Other(format!("read area counter failed: {error:?}"))
            })?
            .map_or_else(
                || self.scan_next_area_offset(params.family, params.realm, params.area),
                |bytes| AreaCounterValue::decode(&bytes).map(|value| value.next_offset),
            )
            .map_err(PromotionWriteFailure::Other)?;
        if first_area_offset != params.first_area_offset {
            return Err(PromotionWriteFailure::ScopeConflict);
        }

        let realm_key = encode_realm_counter_key(params.realm);
        let first_realm_offset = txn
            .get(&realm_key)
            .map_err(|error| {
                PromotionWriteFailure::Other(format!("read realm counter failed: {error:?}"))
            })?
            .map_or_else(
                || self.scan_next_realm_offset(params.family, params.realm),
                |bytes| RealmCounterValue::decode(&bytes).map(|value| value.next_offset),
            )
            .map_err(PromotionWriteFailure::Other)?;
        if first_realm_offset != params.first_realm_offset {
            return Err(PromotionWriteFailure::ScopeConflict);
        }

        txn.put(
            area_key,
            AreaCounterValue {
                next_offset: plan.last_area_offset.saturating_add(1),
            }
            .encode(),
            None,
        )
        .map_err(|error| {
            PromotionWriteFailure::Other(format!("write area counter failed: {error:?}"))
        })?;
        txn.put(
            realm_key,
            RealmCounterValue {
                next_offset: plan.last_realm_offset.saturating_add(1),
            }
            .encode(),
            None,
        )
        .map_err(|error| {
            PromotionWriteFailure::Other(format!("write realm counter failed: {error:?}"))
        })?;
        Ok(())
    }

    fn write_posting_rows(
        txn: &mut cntryl_midge::Transaction,
        params: &CommitPromotionFrontierBatchParams<'_>,
        plan: &PromotionFrontierBatchWritePlan,
        ttl: Option<u64>,
    ) -> Result<(), String> {
        let fragment_start = |offset: u64, first: u64, page_size: u64| {
            if offset / page_size == first / page_size {
                first
            } else {
                offset / page_size * page_size
            }
        };
        let realm_entries: Vec<PostingEntry> = (params.first_realm_offset..=plan.last_realm_offset)
            .map(|offset| PostingEntry {
                offset,
                parent_fragment_start: fragment_start(
                    offset,
                    params.first_realm_offset,
                    REALM_PAGE_RECORD_LIMIT as u64,
                ),
            })
            .collect();
        for entries in posting_entry_pages(&realm_entries, REALM_PAGE_RECORD_LIMIT as u64) {
            txn.put(
                encode_realm_resource_posting_key(params.realm, params.resource, entries[0].offset),
                PostingPageValue {
                    entries: entries.to_vec(),
                }
                .encode(),
                ttl,
            )
            .map_err(|error| format!("write realm-resource posting failed: {error:?}"))?;
        }

        let global_entries: Vec<PostingEntry> = (params.first_global_offset
            ..=plan.last_global_offset)
            .map(|offset| PostingEntry {
                offset,
                parent_fragment_start: fragment_start(
                    offset,
                    params.first_global_offset,
                    GLOBAL_PAGE_RECORD_LIMIT,
                ),
            })
            .collect();
        for entries in posting_entry_pages(&global_entries, GLOBAL_PAGE_RECORD_LIMIT) {
            let encoded = PostingPageValue {
                entries: entries.to_vec(),
            }
            .encode();
            for key in [
                encode_global_area_posting_key(params.area, entries[0].offset),
                encode_global_resource_posting_key(params.resource, entries[0].offset),
                encode_global_area_resource_posting_key(
                    params.area,
                    params.resource,
                    entries[0].offset,
                ),
            ] {
                txn.put(key, encoded.clone(), ttl)
                    .map_err(|error| format!("write global posting failed: {error:?}"))?;
            }
        }

        Ok(())
    }

    fn persist_promotion_frontier_counters_and_metadata(
        txn: &mut cntryl_midge::Transaction,
        params: &CommitPromotionFrontierBatchParams<'_>,
        plan: &PromotionFrontierBatchWritePlan,
    ) -> Result<ResourceMetaValue, String> {
        let resource_meta_after = ResourceMetaValue {
            next_offset: plan.last_resource_offset.saturating_add(1),
            committed_size_bytes: plan.committed_size_after,
        };
        txn.put(
            encode_resource_meta_key(params.realm, params.area, params.resource),
            resource_meta_after.encode(),
            None,
        )
        .map_err(|e| format!("txn put failed: {e:?}"))?;

        Ok(resource_meta_after)
    }

    fn commit_promotion_frontier_tx(
        &self,
        txn: cntryl_midge::Transaction,
        mode: StreamWriteMode,
    ) -> Result<(), PromotionTransactionFailure> {
        let write_options = match mode {
            StreamWriteMode::Sync => self.sync_write_options,
            StreamWriteMode::Buffered => self.buffered_write_options,
            StreamWriteMode::CloudStrict => cntryl_midge::WriteOptions::cloud_strict(),
        };
        #[cfg(test)]
        {
            let should_fail = self
                .fail_next_promotion_frontier_commit
                .swap(false, Ordering::AcqRel);

            if should_fail {
                return Err(PromotionTransactionFailure::Other(
                    "Injected stream commit failure".to_string(),
                ));
            }
        }
        txn.commit(write_options).map_err(|error| match error {
            cntryl_midge::MidgeError::WriteConflict(_) => {
                PromotionTransactionFailure::WriteConflict
            }
            other => PromotionTransactionFailure::Other(format!("midge commit error: {other:?}")),
        })?;

        Ok(())
    }

    fn build_promotion_frontier_commit_response(
        params: CommitPromotionFrontierBatchParams<'_>,
        plan: &PromotionFrontierBatchWritePlan,
    ) -> CommitResponse {
        CommitResponse {
            first_resource_offset: params.first_resource_offset,
            last_resource_offset: plan.last_resource_offset,
            first_area_offset: params.first_area_offset,
            last_area_offset: plan.last_area_offset,
            first_realm_offset: params.first_realm_offset,
            last_realm_offset: plan.last_realm_offset,
            first_global_offset: params.first_global_offset,
            last_global_offset: plan.last_global_offset,
            batch_size: plan.batch_size,
            ingest_metadata: params.ingest_metadata,
        }
    }
}
