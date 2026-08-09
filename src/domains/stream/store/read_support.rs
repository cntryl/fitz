use super::{
    decode_realm_offset_from_key, encode_compact_global_page_key, family_to_storage_partition,
    read_limit_to_usize, usize_to_u64_saturating, Bytes, CompactGlobalPageValue, StreamStore,
    GLOBAL_PAGE_RECORD_LIMIT,
};
use std::collections::HashMap;

pub(super) type GlobalFragmentCache = HashMap<u64, (u64, CompactGlobalPageValue)>;

pub(super) fn page_slot_offset(page_start: u64, slot: usize) -> u64 {
    page_start.saturating_add(usize_to_u64_saturating(slot))
}

pub(super) fn record_payload_bytes(body: &Bytes, metadata: Option<&Bytes>) -> usize {
    body.len().saturating_add(metadata.map_or(0, Bytes::len))
}

pub(super) fn begin_read_tx(
    store: &StreamStore,
    family: u64,
    context: &str,
) -> Result<cntryl_midge::Transaction, String> {
    store
        .db
        .begin_tx(
            family_to_storage_partition(family),
            cntryl_midge::TransactionMode::ReadOnly,
        )
        .map_err(|error| format!("begin {context} read failed: {error:?}"))
}

pub(super) fn validate_fragment_range(
    plane: &str,
    first_offset: u64,
    record_count: usize,
    previous_end: &mut Option<u64>,
) -> Result<(), String> {
    if record_count == 0 {
        return Err(format!(
            "ERR_STREAM_CORRUPT_FRAGMENT: plane={plane} empty fragment"
        ));
    }
    let count = u64::try_from(record_count)
        .map_err(|_| format!("ERR_STREAM_CORRUPT_FRAGMENT: plane={plane} record count overflow"))?;
    let end = first_offset
        .checked_add(count)
        .ok_or_else(|| format!("ERR_STREAM_CORRUPT_FRAGMENT: plane={plane} offset overflow"))?;
    let bucket_end = (first_offset / GLOBAL_PAGE_RECORD_LIMIT * GLOBAL_PAGE_RECORD_LIMIT)
        .saturating_add(GLOBAL_PAGE_RECORD_LIMIT);
    if end > bucket_end {
        return Err(format!(
            "ERR_STREAM_CORRUPT_FRAGMENT: plane={plane} fragment crosses 64-offset bucket"
        ));
    }
    if previous_end.is_some_and(|prior_end| first_offset < prior_end) {
        return Err(format!(
            "ERR_STREAM_CORRUPT_FRAGMENT: plane={plane} duplicate or overlapping offsets"
        ));
    }
    *previous_end = Some(end);
    Ok(())
}

pub(super) fn load_global_locator_record(
    txn: &cntryl_midge::Transaction,
    global_offset: u64,
    parent_fragment_start: u64,
    cache: &mut GlobalFragmentCache,
) -> Result<super::CompactGlobalPageRecord, String> {
    if global_offset < parent_fragment_start
        || global_offset.saturating_sub(parent_fragment_start) >= GLOBAL_PAGE_RECORD_LIMIT
    {
        return Err("ERR_STREAM_CORRUPT_LOCATOR: global offset outside parent bucket".to_string());
    }
    let (actual_start, page) = match cache.entry(parent_fragment_start) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let direct = txn
                .get(&encode_compact_global_page_key(parent_fragment_start))
                .map_err(|error| format!("read locator parent fragment failed: {error:?}"))?;
            let loaded = if let Some(value) = direct {
                (
                    parent_fragment_start,
                    CompactGlobalPageValue::try_decode(&value)?,
                )
            } else {
                find_compacted_global_fragment(txn, global_offset, parent_fragment_start)?
            };
            entry.insert(loaded)
        }
    };
    let index = usize::try_from(global_offset.saturating_sub(*actual_start)).unwrap_or(usize::MAX);
    let mut record = page.records.get(index).cloned().ok_or_else(|| {
        "ERR_STREAM_CORRUPT_LOCATOR: global offset outside parent fragment".to_string()
    })?;
    resolve_blob_payload(txn, &mut record.body, &mut record.metadata)?;
    Ok(record)
}

fn find_compacted_global_fragment(
    txn: &cntryl_midge::Transaction,
    global_offset: u64,
    parent_fragment_start: u64,
) -> Result<(u64, CompactGlobalPageValue), String> {
    let bucket_start = global_offset / GLOBAL_PAGE_RECORD_LIMIT * GLOBAL_PAGE_RECORD_LIMIT;
    let mut prefix = encode_compact_global_page_key(0);
    prefix.truncate(prefix.len().saturating_sub(24));
    let rows = txn
        .scan(
            &cntryl_midge::Query::new()
                .start_key(Bytes::from(encode_compact_global_page_key(bucket_start)))
                .prefix(Bytes::from(prefix))
                .limit(65),
        )
        .map_err(|error| format!("scan locator parent fragments failed: {error:?}"))?;
    for row in rows {
        let (key, value) =
            row.map_err(|error| format!("read locator parent candidate failed: {error:?}"))?;
        let first = decode_realm_offset_from_key(&key)?;
        let candidate = CompactGlobalPageValue::try_decode(&value)?;
        let count = u64::try_from(candidate.records.len())
            .map_err(|_| "ERR_STREAM_CORRUPT_LOCATOR: record count overflow".to_string())?;
        if first <= global_offset && global_offset < first.saturating_add(count) {
            if first != parent_fragment_start && first != bucket_start {
                return Err(
                    "ERR_STREAM_CORRUPT_LOCATOR: unexpected compacted parent start".to_string(),
                );
            }
            return Ok((first, candidate));
        }
        if first / GLOBAL_PAGE_RECORD_LIMIT > bucket_start / GLOBAL_PAGE_RECORD_LIMIT {
            break;
        }
    }
    Err("ERR_STREAM_CORRUPT_LOCATOR: missing global parent fragment".to_string())
}

pub(super) fn resolve_blob_payload(
    txn: &cntryl_midge::Transaction,
    body: &mut Bytes,
    metadata: &mut Option<Bytes>,
) -> Result<(), String> {
    let Some((global_offset, expected_checksum)) = super::decode_payload_blob_ref(body)? else {
        return Ok(());
    };
    if metadata.is_some() {
        return Err("ERR_STREAM_CORRUPT_BLOB: blob reference carries metadata".to_string());
    }
    let value = txn
        .get(&super::encode_payload_blob_key(global_offset))
        .map_err(|error| format!("read Stream payload blob failed: {error:?}"))?
        .ok_or_else(|| "ERR_STREAM_CORRUPT_BLOB: missing payload blob".to_string())?;
    let (stored_body, stored_metadata, actual_checksum) = super::decode_payload_blob(&value)?;
    if actual_checksum != expected_checksum {
        return Err("ERR_STREAM_CORRUPT_BLOB: reference checksum mismatch".to_string());
    }
    *body = stored_body;
    *metadata = stored_metadata;
    Ok(())
}

pub(super) fn hydrate_area_locator(
    txn: &cntryl_midge::Transaction,
    realm: &str,
    area: &str,
    record: &mut super::CompactAreaPageRecord,
    cache: &mut GlobalFragmentCache,
) -> Result<(), String> {
    let Some((global_offset, parent)) = super::decode_payload_locator(&record.body)? else {
        return Err("ERR_STREAM_CORRUPT_LOCATOR: area record has inline payload".to_string());
    };
    let global = load_global_locator_record(txn, global_offset, parent, cache)?;
    if global.realm.as_ref() != realm
        || global.area.as_ref() != area
        || global.resource != record.resource
        || global.resource_offset != record.resource_offset
        || global.expires_at != record.expires_at
    {
        return Err("ERR_STREAM_CORRUPT_LOCATOR: area/global identity mismatch".to_string());
    }
    record.body = global.body;
    record.metadata = global.metadata;
    record.created_at = global.created_at;
    record.expires_at = global.expires_at;
    Ok(())
}

pub(super) fn hydrate_realm_locator(
    txn: &cntryl_midge::Transaction,
    realm: &str,
    record: &mut super::CompactRealmPageRecord,
    cache: &mut GlobalFragmentCache,
) -> Result<(), String> {
    let Some((global_offset, parent)) = super::decode_payload_locator(&record.body)? else {
        return Err("ERR_STREAM_CORRUPT_LOCATOR: realm record has inline payload".to_string());
    };
    let global = load_global_locator_record(txn, global_offset, parent, cache)?;
    if global.realm.as_ref() != realm
        || global.area != record.area
        || global.resource != record.resource
        || global.area_offset != record.area_offset
        || global.resource_offset != record.resource_offset
        || global.expires_at != record.expires_at
    {
        return Err("ERR_STREAM_CORRUPT_LOCATOR: realm/global identity mismatch".to_string());
    }
    record.body = global.body;
    record.metadata = global.metadata;
    record.created_at = global.created_at;
    record.expires_at = global.expires_at;
    Ok(())
}

pub(super) fn bounded_fragment_rows(
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

pub(super) fn broad_scope_fragment_rows(from_offset: u64, limit: u64) -> usize {
    let skipped_in_bucket =
        usize::try_from(from_offset % GLOBAL_PAGE_RECORD_LIMIT).unwrap_or(usize::MAX);
    skipped_in_bucket
        .saturating_add(read_limit_to_usize(limit))
        .min(crate::domains::stream::MAX_POSTING_FRAGMENTS_FETCHED)
}

pub(super) fn bounded_posting_rows(
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
