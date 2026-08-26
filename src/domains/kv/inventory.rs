//! Shared persisted format for per-resource KV inventory estimates.
//!
//! The actor write path in `actor/inventory_delta.rs` updates this metadata,
//! while `sink/admin/inventory.rs` reads and refreshes it for admin views.

const VALUE_VERSION: u8 = 1;
const VALUE_LEN: usize = 18;
const RECORD_COUNT_RANGE: std::ops::Range<usize> = 2..10;
const STORAGE_BYTES_RANGE: std::ops::Range<usize> = 10..18;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct KvInventoryEstimate {
    pub(crate) estimated_record_count: u64,
    pub(crate) estimated_storage_bytes: u64,
    pub(crate) estimate_complete: bool,
}

impl Default for KvInventoryEstimate {
    fn default() -> Self {
        Self {
            estimated_record_count: 0,
            estimated_storage_bytes: 0,
            estimate_complete: true,
        }
    }
}

pub(crate) fn encode_estimate(estimate: KvInventoryEstimate) -> Vec<u8> {
    let mut out = Vec::with_capacity(VALUE_LEN);
    out.push(VALUE_VERSION);
    out.push(u8::from(estimate.estimate_complete));
    out.extend_from_slice(&estimate.estimated_record_count.to_be_bytes());
    out.extend_from_slice(&estimate.estimated_storage_bytes.to_be_bytes());
    out
}

/// Decode one persisted inventory estimate.
///
/// # Errors
///
/// Returns an error when the value has an unknown version or invalid length.
pub(crate) fn decode_estimate(bytes: &[u8]) -> Result<KvInventoryEstimate, String> {
    if bytes.len() != VALUE_LEN || bytes.first().copied() != Some(VALUE_VERSION) {
        return Err("invalid KV inventory metadata value".to_string());
    }

    let estimated_record_count = u64::from_be_bytes(
        bytes[RECORD_COUNT_RANGE]
            .try_into()
            .map_err(|_| "invalid KV inventory record count".to_string())?,
    );
    let estimated_storage_bytes = u64::from_be_bytes(
        bytes[STORAGE_BYTES_RANGE]
            .try_into()
            .map_err(|_| "invalid KV inventory storage estimate".to_string())?,
    );
    Ok(KvInventoryEstimate {
        estimated_record_count,
        estimated_storage_bytes,
        estimate_complete: bytes[1] != 0,
    })
}
