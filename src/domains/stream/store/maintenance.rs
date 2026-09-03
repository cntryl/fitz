use super::{
    family_to_storage_partition, CompactAreaPageRecord, CompactAreaPageValue,
    CompactGlobalPageRecord, CompactGlobalPageValue, CompactRealmPageRecord,
    CompactResourcePageRecord, CompactResourcePageValue, CompressedCompactRealmPageValue,
    KeyPrefix, PostingEntry, PostingPageValue, StreamStore, GLOBAL_PAGE_RECORD_LIMIT,
};
use std::collections::BTreeMap;

const FRAGMENT_COMPACTION_THRESHOLD: usize = 8;
// Maintenance executes on the same synchronous family actor as client Stream
// commands. Yield after one bucket so a series of strict storage commits cannot
// monopolize that actor beyond the client liveness budget.
const MAX_BUCKETS_PER_INVOCATION: usize = 1;
const MAX_BYTES_PER_INVOCATION: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamMaintenanceResult {
    pub buckets_compacted: usize,
    pub records_compacted: usize,
    pub bytes_examined: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Plane {
    Resource,
    Area,
    Realm,
    Global,
    Posting,
}

enum MergedPage {
    Resource(Vec<CompactResourcePageRecord>),
    Area(Vec<CompactAreaPageRecord>),
    Realm(Vec<CompactRealmPageRecord>),
    Global(Vec<CompactGlobalPageRecord>),
    Posting(Vec<PostingEntry>),
}

impl MergedPage {
    fn empty(plane: Plane) -> Self {
        match plane {
            Plane::Resource => Self::Resource(Vec::new()),
            Plane::Area => Self::Area(Vec::new()),
            Plane::Realm => Self::Realm(Vec::new()),
            Plane::Global => Self::Global(Vec::new()),
            Plane::Posting => Self::Posting(Vec::new()),
        }
    }

    fn append_encoded(&mut self, bytes: &[u8]) -> Result<usize, String> {
        match self {
            Self::Resource(records) => {
                let page = CompactResourcePageValue::try_decode(bytes)?;
                let count = page.records.len();
                records.extend(page.records);
                Ok(count)
            }
            Self::Area(records) => {
                let page = CompactAreaPageValue::try_decode(bytes)?;
                let count = page.records.len();
                records.extend(page.records);
                Ok(count)
            }
            Self::Realm(records) => {
                let page = CompressedCompactRealmPageValue::try_decode(bytes)?;
                let count = page.records.len();
                records.extend(page.records);
                Ok(count)
            }
            Self::Global(records) => {
                let page = CompactGlobalPageValue::try_decode(bytes)?;
                let count = page.records.len();
                records.extend(page.records);
                Ok(count)
            }
            Self::Posting(entries) => {
                let page = PostingPageValue::try_decode(bytes)?;
                let count = page.entries.len();
                entries.extend(page.entries);
                Ok(count)
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Resource(records) => records.len(),
            Self::Area(records) => records.len(),
            Self::Realm(records) => records.len(),
            Self::Global(records) => records.len(),
            Self::Posting(entries) => entries.len(),
        }
    }

    fn retains_positional_range(&self) -> bool {
        !matches!(self, Self::Posting(_))
    }

    fn posting_bounds(&self) -> Option<(u64, u64)> {
        let Self::Posting(entries) = self else {
            return None;
        };
        Some((entries.first()?.offset, entries.last()?.offset))
    }

    fn latest_expiration(&self) -> Option<u64> {
        let mut expirations: Box<dyn Iterator<Item = Option<u64>> + '_> = match self {
            Self::Resource(records) => Box::new(records.iter().map(|record| record.expires_at)),
            Self::Area(records) => Box::new(records.iter().map(|record| record.expires_at)),
            Self::Realm(records) => Box::new(records.iter().map(|record| record.expires_at)),
            Self::Global(records) => Box::new(records.iter().map(|record| record.expires_at)),
            Self::Posting(entries) => Box::new(entries.iter().map(|entry| entry.expires_at)),
        };
        expirations.try_fold(0, |latest, expiration| {
            expiration.map(|value| latest.max(value))
        })
    }

    fn prune_expired(&mut self, now_epoch_ms: u64) {
        match self {
            Self::Resource(records) => records
                .iter_mut()
                .filter(|record| super::record_is_expired(record.expires_at, now_epoch_ms))
                .for_each(|record| {
                    record.body = bytes::Bytes::new();
                    record.metadata = None;
                }),
            Self::Area(records) => records
                .iter_mut()
                .filter(|record| super::record_is_expired(record.expires_at, now_epoch_ms))
                .for_each(|record| {
                    record.body = bytes::Bytes::new();
                    record.metadata = None;
                }),
            Self::Realm(records) => records
                .iter_mut()
                .filter(|record| super::record_is_expired(record.expires_at, now_epoch_ms))
                .for_each(|record| {
                    record.body = bytes::Bytes::new();
                    record.metadata = None;
                }),
            Self::Global(records) => records
                .iter_mut()
                .filter(|record| super::record_is_expired(record.expires_at, now_epoch_ms))
                .for_each(|record| {
                    record.body = bytes::Bytes::new();
                    record.metadata = None;
                }),
            Self::Posting(entries) => {
                entries.retain(|entry| !super::record_is_expired(entry.expires_at, now_epoch_ms));
            }
        }
    }

    fn encode(self) -> Vec<u8> {
        match self {
            Self::Resource(records) => CompactResourcePageValue { records }.encode(),
            Self::Area(records) => CompactAreaPageValue { records }.encode(),
            Self::Realm(records) => CompressedCompactRealmPageValue { records }.encode(),
            Self::Global(records) => CompactGlobalPageValue { records }.encode(),
            Self::Posting(entries) => PostingPageValue { entries }.encode(),
        }
    }
}

struct Fragment {
    plane: Plane,
    key: Vec<u8>,
    first_offset: u64,
    generation: u64,
    value: Vec<u8>,
}

struct CompactedBucket {
    group_key: Vec<u8>,
    source_keys: Vec<Vec<u8>>,
    bucket_start: u64,
    replacement_generation: u64,
    bytes_examined: usize,
    merged: MergedPage,
}

enum BucketMaintenanceOutcome {
    NotCompactable { bytes: usize },
    OverBudget { bytes: usize },
    Compacted { records: usize, bytes: usize },
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum MaintenanceFailureStage {
    BeforeReplacement = 1,
    DuringReplacement = 2,
    BeforeDeletion = 3,
}

fn compactable_plane(prefix: u8) -> Option<Plane> {
    match prefix {
        value if value == KeyPrefix::CompactResourcePage as u8 => Some(Plane::Resource),
        value if value == KeyPrefix::CompactAreaPage as u8 => Some(Plane::Area),
        value if value == KeyPrefix::CompressedCompactRealmPage as u8 => Some(Plane::Realm),
        value if value == KeyPrefix::CompactGlobalPage as u8 => Some(Plane::Global),
        value
            if [
                KeyPrefix::RealmResourcePostingPage as u8,
                KeyPrefix::GlobalAreaPostingPage as u8,
                KeyPrefix::GlobalResourcePostingPage as u8,
                KeyPrefix::GlobalAreaResourcePostingPage as u8,
            ]
            .contains(&value) =>
        {
            Some(Plane::Posting)
        }
        _ => None,
    }
}

fn scan_maintenance_buckets(
    store: &StreamStore,
    family: u64,
) -> Result<BTreeMap<Vec<u8>, Vec<Fragment>>, String> {
    #[cfg(test)]
    store
        .maintenance_full_scans
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let txn = store
        .db
        .begin_tx(
            family_to_storage_partition(family),
            cntryl_midge::TransactionMode::ReadOnly,
        )
        .map_err(|error| format!("begin Stream maintenance scan failed: {error:?}"))?;
    let mut buckets: BTreeMap<Vec<u8>, Vec<Fragment>> = BTreeMap::new();
    for row in txn
        .scan(&cntryl_midge::Query::new())
        .map_err(|error| format!("scan Stream maintenance rows failed: {error:?}"))?
    {
        let (key, value) =
            row.map_err(|error| format!("read Stream maintenance row failed: {error:?}"))?;
        let suffix = crate::domains::stream::storage::stream_key_suffix(&key);
        let Some(plane) = suffix.first().copied().and_then(compactable_plane) else {
            continue;
        };
        if key.len() < 24 {
            return Err("ERR_STREAM_CORRUPT_FRAGMENT: compactable key too short".to_string());
        }
        let first_offset = u64::from_be_bytes(
            key[key.len() - 16..key.len() - 8]
                .try_into()
                .map_err(|_| "decode Stream fragment offset failed".to_string())?,
        );
        let generation = u64::from_be_bytes(
            key[key.len() - 8..]
                .try_into()
                .map_err(|_| "decode Stream fragment generation failed".to_string())?,
        );
        let group_key = key[..key.len() - 16].to_vec();
        buckets.entry(group_key).or_default().push(Fragment {
            plane,
            key: key.to_vec(),
            first_offset,
            generation,
            value: value.to_vec(),
        });
    }
    Ok(buckets)
}

fn scan_maintenance_bucket(
    store: &StreamStore,
    family: u64,
    group_key: &[u8],
) -> Result<Vec<Fragment>, String> {
    let txn = store
        .db
        .begin_tx(
            family_to_storage_partition(family),
            cntryl_midge::TransactionMode::ReadOnly,
        )
        .map_err(|error| format!("begin Stream maintenance bucket scan failed: {error:?}"))?;
    let mut fragments = Vec::new();
    for row in txn
        .scan(&cntryl_midge::Query::new().prefix(bytes::Bytes::copy_from_slice(group_key)))
        .map_err(|error| format!("scan Stream maintenance bucket failed: {error:?}"))?
    {
        let (key, value) =
            row.map_err(|error| format!("read Stream maintenance bucket failed: {error:?}"))?;
        if key.len() < group_key.len().saturating_add(16) {
            return Err("ERR_STREAM_CORRUPT_FRAGMENT: compactable key too short".to_string());
        }
        let suffix = crate::domains::stream::storage::stream_key_suffix(&key);
        let plane = suffix
            .first()
            .copied()
            .and_then(compactable_plane)
            .ok_or_else(|| "ERR_STREAM_CORRUPT_FRAGMENT: invalid queued plane".to_string())?;
        let first_offset = u64::from_be_bytes(
            key[key.len() - 16..key.len() - 8]
                .try_into()
                .map_err(|_| "decode Stream fragment offset failed".to_string())?,
        );
        let generation = u64::from_be_bytes(
            key[key.len() - 8..]
                .try_into()
                .map_err(|_| "decode Stream fragment generation failed".to_string())?,
        );
        fragments.push(Fragment {
            plane,
            key: key.to_vec(),
            first_offset,
            generation,
            value: value.to_vec(),
        });
    }
    Ok(fragments)
}

fn merge_maintenance_bucket(
    group_key: Vec<u8>,
    mut fragments: Vec<Fragment>,
) -> Result<Option<CompactedBucket>, String> {
    if fragments.len() <= FRAGMENT_COMPACTION_THRESHOLD {
        return Ok(None);
    }
    fragments.sort_by_key(|fragment| fragment.first_offset);
    let plane = fragments[0].plane;
    if fragments.iter().any(|fragment| fragment.plane != plane) {
        return Err("ERR_STREAM_CORRUPT_FRAGMENT: mixed maintenance planes".to_string());
    }
    let bucket_start =
        fragments[0].first_offset / GLOBAL_PAGE_RECORD_LIMIT * GLOBAL_PAGE_RECORD_LIMIT;
    let replacement_generation = fragments
        .iter()
        .map(|fragment| fragment.generation)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| "ERR_STREAM_CORRUPT_FRAGMENT: generation exhausted".to_string())?;
    let mut merged = MergedPage::empty(plane);
    let mut expected = bucket_start;
    let mut prior_posting = None;
    for fragment in &fragments {
        let before = merged.len();
        let count = merged.append_encoded(&fragment.value)?;
        if count == 0 {
            return Err("ERR_STREAM_CORRUPT_FRAGMENT: empty fragment".to_string());
        }
        if plane == Plane::Posting {
            validate_merged_posting(
                &merged,
                before,
                fragment.first_offset,
                bucket_start,
                &mut prior_posting,
            )?;
        } else if fragment.first_offset != expected {
            if plane == Plane::Global {
                return Ok(None);
            }
            return Err("ERR_STREAM_CORRUPT_FRAGMENT: gap or overlap".to_string());
        } else {
            expected = expected
                .checked_add(u64::try_from(count).map_err(|_| {
                    "ERR_STREAM_CORRUPT_FRAGMENT: record count overflow".to_string()
                })?)
                .ok_or_else(|| "ERR_STREAM_CORRUPT_FRAGMENT: offset overflow".to_string())?;
            if expected > bucket_start.saturating_add(GLOBAL_PAGE_RECORD_LIMIT) {
                return Err("ERR_STREAM_CORRUPT_FRAGMENT: fragment crosses bucket".to_string());
            }
        }
    }
    let bytes_examined = fragments.iter().map(|fragment| fragment.value.len()).sum();
    Ok(Some(CompactedBucket {
        group_key,
        source_keys: fragments.into_iter().map(|fragment| fragment.key).collect(),
        bucket_start,
        replacement_generation,
        bytes_examined,
        merged,
    }))
}

fn validate_merged_posting(
    merged: &MergedPage,
    fragment_index: usize,
    fragment_first_offset: u64,
    bucket_start: u64,
    prior_posting: &mut Option<u64>,
) -> Result<(), String> {
    let (first, last) = merged
        .posting_bounds()
        .ok_or_else(|| "ERR_STREAM_CORRUPT_FRAGMENT: empty posting".to_string())?;
    let MergedPage::Posting(entries) = merged else {
        unreachable!();
    };
    let actual_first = entries[fragment_index].offset;
    if actual_first != fragment_first_offset
        || prior_posting.is_some_and(|prior| actual_first <= prior)
        || first < bucket_start
        || last >= bucket_start.saturating_add(GLOBAL_PAGE_RECORD_LIMIT)
    {
        return Err("ERR_STREAM_CORRUPT_FRAGMENT: unordered posting fragment".to_string());
    }
    *prior_posting = Some(last);
    Ok(())
}

impl StreamStore {
    pub(crate) fn has_pending_maintenance(&self, family: u64) -> bool {
        self.maintenance_queues
            .lock()
            .get(&family)
            .is_none_or(|queue| !queue.initialized || !queue.buckets.is_empty())
    }

    #[cfg(test)]
    pub(super) fn fail_maintenance_at_for_tests(&self, stage: MaintenanceFailureStage) {
        self.maintenance_failure_stage
            .store(stage as u8, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn maintenance_full_scan_count_for_tests(&self) -> usize {
        self.maintenance_full_scans
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    fn inject_maintenance_failure(&self, stage: MaintenanceFailureStage) -> Result<(), String> {
        if self
            .maintenance_failure_stage
            .compare_exchange(
                stage as u8,
                0,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            return Err(format!(
                "injected Stream maintenance failure at stage {}",
                stage as u8
            ));
        }
        Ok(())
    }

    pub(super) fn queue_maintenance_key(&self, family: u64, fragment_key: &[u8]) {
        if fragment_key.len() >= 16 {
            self.maintenance_queues
                .lock()
                .entry(family)
                .or_default()
                .buckets
                .insert(fragment_key[..fragment_key.len() - 16].to_vec());
        }
    }

    fn next_maintenance_group(&self, family: u64) -> Result<Option<(Vec<u8>, bool)>, String> {
        loop {
            {
                let mut queues = self.maintenance_queues.lock();
                let queue = queues.entry(family).or_default();
                if queue.initialized {
                    return Ok(queue.buckets.pop_first().map(|group_key| {
                        let is_retry = queue.retry_buckets.contains(&group_key);
                        (group_key, is_retry)
                    }));
                }
            }
            let discovered = scan_maintenance_buckets(self, family)?;
            let mut queues = self.maintenance_queues.lock();
            let queue = queues.entry(family).or_default();
            if !queue.initialized {
                queue.buckets.extend(
                    discovered
                        .into_iter()
                        .filter(|(_, fragments)| fragments.len() > FRAGMENT_COMPACTION_THRESHOLD)
                        .map(|(group_key, _)| group_key),
                );
                queue.initialized = true;
            }
        }
    }

    fn requeue_maintenance_group(&self, family: u64, group_key: Vec<u8>, failed: bool) {
        let mut queues = self.maintenance_queues.lock();
        let queue = queues.entry(family).or_default();
        queue.buckets.insert(group_key.clone());
        if failed {
            queue.retry_buckets.insert(group_key);
        }
    }

    fn clear_maintenance_retry(&self, family: u64, group_key: &[u8]) {
        if let Some(queue) = self.maintenance_queues.lock().get_mut(&family) {
            queue.retry_buckets.remove(group_key);
        }
    }

    fn compact_maintenance_group(
        &self,
        family: u64,
        group_key: Vec<u8>,
        remaining_bytes: usize,
    ) -> Result<BucketMaintenanceOutcome, String> {
        let fragments = scan_maintenance_bucket(self, family, &group_key)?;
        let bytes_examined = fragments.iter().fold(0usize, |total, fragment| {
            total.saturating_add(fragment.value.len())
        });
        let Some(bucket) = merge_maintenance_bucket(group_key, fragments)? else {
            return Ok(BucketMaintenanceOutcome::NotCompactable {
                bytes: bytes_examined,
            });
        };
        if bucket.bytes_examined > remaining_bytes {
            return Ok(BucketMaintenanceOutcome::OverBudget {
                bytes: bucket.bytes_examined,
            });
        }
        let record_count = bucket.merged.len();
        let now_epoch_ms = self.now_epoch_ms();
        let mut merged = bucket.merged;
        merged.prune_expired(now_epoch_ms);
        let latest_expiration = merged.latest_expiration();
        let retain_expired_range = merged.retains_positional_range();
        let replacement_ttl = latest_expiration.and_then(|deadline| {
            (deadline > now_epoch_ms)
                .then(|| deadline.saturating_sub(now_epoch_ms).saturating_add(999) / 1_000)
        });
        // A fragment's key names the offset of its FIRST record, and only the
        // positional planes are guaranteed to tile from the bucket start - a
        // posting holds just the offsets belonging to one area or resource, so
        // its first entry lands wherever that scope's first commit did (and
        // moves again when `prune_expired` drops leading entries). Keying the
        // replacement at `bucket_start` regardless would make it disagree with
        // its own key, and `validate_merged_posting` would reject the bucket
        // on the next merge - failing every later slice and requeueing the
        // bucket forever.
        let replacement_first_offset = merged
            .posting_bounds()
            .map_or(bucket.bucket_start, |(first, _)| first);
        let mut replacement_key = bucket.group_key;
        replacement_key.extend_from_slice(&replacement_first_offset.to_be_bytes());
        replacement_key.extend_from_slice(&bucket.replacement_generation.to_be_bytes());
        let mut write_txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| format!("begin Stream maintenance tx failed: {error:?}"))?;
        #[cfg(test)]
        self.inject_maintenance_failure(MaintenanceFailureStage::BeforeReplacement)?;
        if retain_expired_range || latest_expiration.is_none_or(|deadline| deadline > now_epoch_ms)
        {
            write_txn
                .put(replacement_key.clone(), merged.encode(), replacement_ttl)
                .map_err(|error| format!("write Stream replacement failed: {error:?}"))?;
        }
        #[cfg(test)]
        self.inject_maintenance_failure(MaintenanceFailureStage::DuringReplacement)?;
        #[cfg(test)]
        self.inject_maintenance_failure(MaintenanceFailureStage::BeforeDeletion)?;
        for source_key in bucket.source_keys {
            if source_key != replacement_key {
                write_txn
                    .delete(source_key)
                    .map_err(|error| format!("delete Stream source failed: {error:?}"))?;
            }
        }
        write_txn
            .commit(self.sync_write_options)
            .map_err(|error| format!("commit Stream maintenance failed: {error:?}"))?;
        Ok(BucketMaintenanceOutcome::Compacted {
            records: record_count,
            bytes: bucket.bytes_examined,
        })
    }

    /// Runs one bounded synchronous D4 maintenance slice.
    ///
    /// Maintenance is separate from append so discovery never adds historical
    /// payload reads to commits. One invocation handles at most one bucket and
    /// four MiB across resource, area, realm, global, and posting planes.
    /// Absolute record expirations remain authoritative after replacement;
    /// Midge reclaims the replacement at the latest contained deadline.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed/overlapping fragments or a failed atomic
    /// replacement transaction.
    pub fn run_maintenance(&self, family: u64) -> Result<StreamMaintenanceResult, String> {
        self.ensure_layout_activation_for_family(family)?;
        let mut result = StreamMaintenanceResult::default();
        let mut buckets_examined = 0usize;
        while buckets_examined < MAX_BUCKETS_PER_INVOCATION
            && result.bytes_examined < MAX_BYTES_PER_INVOCATION
        {
            let Some((group_key, is_retry)) = self.next_maintenance_group(family)? else {
                break;
            };
            buckets_examined += 1;
            self.maintenance_metrics
                .counter_inc(crate::domains::stream::metrics::METRIC_MAINTENANCE_ATTEMPTS_TOTAL);
            if is_retry {
                self.maintenance_metrics
                    .counter_inc(crate::domains::stream::metrics::METRIC_MAINTENANCE_RETRIES_TOTAL);
            }
            let remaining_bytes = MAX_BYTES_PER_INVOCATION.saturating_sub(result.bytes_examined);
            match self.compact_maintenance_group(family, group_key.clone(), remaining_bytes) {
                Ok(BucketMaintenanceOutcome::NotCompactable { bytes }) => {
                    self.clear_maintenance_retry(family, &group_key);
                    result.bytes_examined = result.bytes_examined.saturating_add(bytes);
                }
                Ok(BucketMaintenanceOutcome::OverBudget { bytes }) => {
                    self.requeue_maintenance_group(family, group_key, false);
                    result.bytes_examined = result.bytes_examined.saturating_add(bytes);
                    break;
                }
                Ok(BucketMaintenanceOutcome::Compacted { records, bytes }) => {
                    self.clear_maintenance_retry(family, &group_key);
                    self.maintenance_metrics.counter_inc(
                        crate::domains::stream::metrics::METRIC_MAINTENANCE_BUCKETS_COMPACTED_TOTAL,
                    );
                    result.buckets_compacted += 1;
                    result.records_compacted = result.records_compacted.saturating_add(records);
                    result.bytes_examined = result.bytes_examined.saturating_add(bytes);
                }
                Err(error) => {
                    self.requeue_maintenance_group(family, group_key, true);
                    self.maintenance_metrics.counter_inc(
                        crate::domains::stream::metrics::METRIC_MAINTENANCE_FAILURES_TOTAL,
                    );
                    return Err(error);
                }
            }
        }
        Ok(result)
    }
}
