//! Durable bucket-at-a-time reclamation of payloads hidden by namespace resets.
//!
//! Global positions and identities remain as small expired placeholders. They
//! let retained sparse postings distinguish deletion from missing/corrupt data.

use super::drop_scope::cutoff_snapshot;
use super::{
    decode_realm_offset_from_key, encode_compact_global_page_key, Bytes, CompactGlobalPageValue,
    StreamMaintenanceResult, StreamStore, GLOBAL_PAGE_RECORD_LIMIT,
};
use crate::utils::storage_key::{self, DomainKeyspace};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
struct ReclaimedBucket {
    result: StreamMaintenanceResult,
    global_offsets: BTreeSet<u64>,
    sidecars: BTreeMap<Vec<u8>, BTreeSet<u64>>,
}

struct ReclamationProgress {
    next_bucket: u64,
    head: u64,
    pending_head: Option<u64>,
}

impl ReclamationProgress {
    fn new(head: u64) -> Self {
        Self {
            next_bucket: 0,
            head,
            pending_head: None,
        }
    }

    fn decode(value: &[u8]) -> Result<Self, String> {
        if !matches!(value.len(), 16 | 24) {
            return Err("invalid Stream reclamation progress".to_string());
        }
        Ok(Self {
            next_bucket: u64::from_le_bytes(value[..8].try_into().unwrap()),
            head: u64::from_le_bytes(value[8..16].try_into().unwrap()),
            pending_head: value
                .get(16..24)
                .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap())),
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut value = self.next_bucket.to_le_bytes().to_vec();
        value.extend_from_slice(&self.head.to_le_bytes());
        if let Some(head) = self.pending_head {
            value.extend_from_slice(&head.to_le_bytes());
        }
        value
    }

    fn include(&mut self, head: u64) {
        if self.next_bucket == 0 {
            self.head = self.head.max(head);
        } else {
            // Finish this finite pass before revisiting buckets affected by a
            // later DROP. New requests coalesce without rewinding active work.
            self.pending_head = Some(self.pending_head.unwrap_or(self.head).max(head));
        }
    }

    fn advance(mut self, next_bucket: u64) -> Option<Self> {
        if next_bucket < self.head {
            self.next_bucket = next_bucket;
            Some(self)
        } else {
            self.pending_head.map(Self::new)
        }
    }
}

pub(super) fn progress_key() -> Vec<u8> {
    storage_key::domain_marker_encoder(
        "",
        DomainKeyspace::Stream,
        super::KeyPrefix::DropReclamation as u8,
        0,
    )
    .into_vec()
}

pub(super) fn decode_progress(value: &[u8]) -> Result<(u64, u64), String> {
    ReclamationProgress::decode(value).map(|progress| (progress.next_bucket, progress.head))
}

pub(super) fn queue_reclamation(
    transaction: &mut cntryl_midge::Transaction,
    head: u64,
) -> Result<(), String> {
    let key = progress_key();
    let current = transaction.get(&key).map_err(|error| error.to_string())?;
    let mut progress = current
        .as_ref()
        .map(|value| ReclamationProgress::decode(value))
        .transpose()?
        .unwrap_or_else(|| ReclamationProgress::new(head));
    progress.include(head);
    transaction
        .assert_value(key.clone(), current.map(|bytes| bytes.to_vec()))
        .map_err(|error| error.to_string())?;
    transaction
        .put(key, progress.encode(), None)
        .map_err(|error| error.to_string())
}

fn delete_offset_ranges(
    transaction: &mut cntryl_midge::Transaction,
    offsets: &BTreeSet<u64>,
    encode: impl Fn(u64) -> Vec<u8>,
) -> Result<(), String> {
    let mut offsets = offsets.iter().copied().peekable();
    while let Some(first) = offsets.next() {
        let mut end = first
            .checked_add(1)
            .ok_or_else(|| "invalid Stream reclaim offset".to_string())?;
        while offsets.peek() == Some(&end) {
            offsets.next();
            end = end
                .checked_add(1)
                .ok_or_else(|| "invalid Stream reclaim offset".to_string())?;
        }
        transaction
            .delete_range(encode(first), encode(end))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn scrub_record(
    transaction: &mut cntryl_midge::Transaction,
    offset: u64,
    record: &mut super::CompactGlobalPageRecord,
    sidecars: &mut BTreeMap<Vec<u8>, BTreeSet<u64>>,
) -> Result<bool, String> {
    let cutoffs = cutoff_snapshot(
        transaction,
        &record.realm,
        Some(&record.area),
        Some(&record.resource),
    )?;
    cutoffs.assert_unchanged(transaction)?;
    if offset >= cutoffs.maximum {
        return Ok(false);
    }
    // Scoped sidecar offsets may have been reset and reused. A reset already
    // deleted its whole sidecar prefix, so only delete rows in retained axes.
    let area_reset = cutoff_snapshot(transaction, &record.realm, Some(&record.area), None)?;
    let realm_reset = cutoff_snapshot(transaction, &record.realm, None, None)?;
    if offset >= area_reset.maximum {
        let mut prefix = crate::domains::stream::storage::encode_area_discriminator_key(
            &record.realm,
            &record.area,
            0,
        );
        prefix.truncate(prefix.len() - 8);
        sidecars
            .entry(prefix)
            .or_default()
            .insert(record.area_offset);
    }
    if offset >= realm_reset.maximum {
        let mut prefix =
            crate::domains::stream::storage::encode_realm_discriminator_key(&record.realm, 0);
        prefix.truncate(prefix.len() - 8);
        sidecars
            .entry(prefix)
            .or_default()
            .insert(record.realm_offset);
    }
    record.body = Bytes::new();
    record.metadata = None;
    record.expires_at = Some(0);
    Ok(true)
}

fn reclaim_bucket(
    transaction: &mut cntryl_midge::Transaction,
    rows: Vec<(Bytes, Bytes)>,
    bucket: u64,
    head: u64,
) -> Result<ReclaimedBucket, String> {
    let mut reclaimed = ReclaimedBucket::default();
    reclaimed.result.buckets_compacted = 1;
    let mut previous_end = None;
    for (key, value) in rows {
        let first = decode_realm_offset_from_key(&key)?;
        if first / GLOBAL_PAGE_RECORD_LIMIT * GLOBAL_PAGE_RECORD_LIMIT != bucket {
            break;
        }
        let mut page = CompactGlobalPageValue::try_decode(&value)?;
        super::read_support::validate_fragment_range(
            "global",
            first,
            page.records.len(),
            &mut previous_end,
        )?;
        let mut changed = false;
        for (index, record) in page.records.iter_mut().enumerate() {
            let offset = first
                .checked_add(index as u64)
                .ok_or_else(|| "invalid Stream reclaim offset".to_string())?;
            if offset < head && scrub_record(transaction, offset, record, &mut reclaimed.sidecars)?
            {
                reclaimed.global_offsets.insert(offset);
                reclaimed.result.records_compacted += 1;
                changed = true;
            }
        }
        reclaimed.result.bytes_examined =
            reclaimed.result.bytes_examined.saturating_add(value.len());
        if changed {
            transaction
                .assert_value(key.to_vec(), Some(value.to_vec()))
                .map_err(|error| error.to_string())?;
            transaction
                .put(key.to_vec(), page.encode(), None)
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(reclaimed)
}

impl StreamStore {
    pub(super) fn has_pending_drop_reclamation(&self, family: u64) -> bool {
        self.db
            .begin_tx(
                super::family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .and_then(|transaction| transaction.get(&progress_key()))
            .map_or(true, |value| value.is_some())
    }

    pub(super) fn run_drop_reclamation(
        &self,
        family: u64,
    ) -> Result<Option<StreamMaintenanceResult>, String> {
        let mut transaction = self
            .db
            .begin_tx(
                super::family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| error.to_string())?;
        transaction.set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);
        let progress_key = progress_key();
        let Some(progress_bytes) = transaction
            .get(&progress_key)
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        let progress = ReclamationProgress::decode(&progress_bytes)?;
        let mut prefix = encode_compact_global_page_key(0);
        prefix.truncate(prefix.len() - 24);
        let rows = transaction
            .scan(
                &cntryl_midge::Query::new()
                    .start_key(Bytes::from(encode_compact_global_page_key(
                        progress.next_bucket,
                    )))
                    .prefix(Bytes::from(prefix))
                    .limit(65),
            )
            .map_err(|error| error.to_string())?
            .try_collect()
            .map_err(|error| error.to_string())?;
        transaction
            .assert_value(progress_key.clone(), Some(progress_bytes.to_vec()))
            .map_err(|error| error.to_string())?;
        let bucket = rows
            .first()
            .map(|(key, _)| decode_realm_offset_from_key(key))
            .transpose()?
            .map(|offset| offset / GLOBAL_PAGE_RECORD_LIMIT * GLOBAL_PAGE_RECORD_LIMIT);
        let mut reclaimed = ReclaimedBucket::default();
        let next_progress = if let Some(bucket) = bucket.filter(|bucket| *bucket < progress.head) {
            reclaimed = reclaim_bucket(&mut transaction, rows, bucket, progress.head)?;
            progress.advance(bucket.saturating_add(GLOBAL_PAGE_RECORD_LIMIT))
        } else {
            let head = progress.head;
            progress.advance(head)
        };
        if let Some(progress) = next_progress {
            transaction
                .put(progress_key, progress.encode(), None)
                .map_err(|error| error.to_string())?;
        } else {
            transaction
                .delete(progress_key)
                .map_err(|error| error.to_string())?;
        }
        delete_offset_ranges(
            &mut transaction,
            &reclaimed.global_offsets,
            super::encode_payload_blob_key,
        )?;
        delete_offset_ranges(
            &mut transaction,
            &reclaimed.global_offsets,
            super::encode_global_discriminator_key,
        )?;
        for (prefix, offsets) in reclaimed.sidecars {
            delete_offset_ranges(&mut transaction, &offsets, |offset| {
                let mut key = prefix.clone();
                key.extend_from_slice(&offset.to_be_bytes());
                key
            })?;
        }
        transaction
            .commit(self.sync_write_options)
            .map_err(|error| format!("commit Stream reclamation failed: {error:?}"))?;
        Ok(Some(reclaimed.result))
    }
}
