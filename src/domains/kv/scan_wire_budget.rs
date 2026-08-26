//! Wire-size budget for KV scan responses.
//!
//! A scan response is carried as a single TLV value with a `u16` length, so the
//! page must be bounded by bytes and not only by item count. Every pair can be
//! individually small and legal while the aggregate is unencodable: at 1 KiB
//! values the frame overflows at roughly 63 items, far below the 1,024-item
//! cap a client gets when it omits `limit`.

/// A KV response is carried as one TLV value, whose length prefix is a `u16`.
pub(crate) const MAX_KV_RESPONSE_PAYLOAD_BYTES: usize = u16::MAX as usize;

/// A scan response's non-item bytes, exactly as `encode_response` writes them:
/// the status flag (1), the item count (4), and the `has_more` marker (1).
///
/// Exact rather than generous on purpose. Over-charging rejects responses that
/// are wire-valid: a single 300-byte key with a 65,200-byte value encodes to
/// 65,514 bytes and fits, but a padded budget refuses it. `scan_wire_budget`
/// tests assert these constants against the real encoded length, so codec drift
/// fails a test instead of silently re-introducing false rejections.
const KV_SCAN_ENVELOPE_OVERHEAD_BYTES: usize = 6;

/// One encoded pair's fixed parts: the `u32` length prefixes on key and value.
const KV_SCAN_ITEM_FIXED_OVERHEAD_BYTES: usize = 8;

/// Largest total item payload a scan response can carry.
#[must_use]
pub(crate) fn kv_scan_response_byte_ceiling() -> usize {
    MAX_KV_RESPONSE_PAYLOAD_BYTES.saturating_sub(KV_SCAN_ENVELOPE_OVERHEAD_BYTES)
}

/// A SCAN request is carried as one TLV value too. Resuming a page requires
/// re-issuing SCAN with `start_key` set to the last returned key (plus
/// `start_exclusive`), so a page boundary is only useful if that key can
/// itself be echoed back inside a fresh request.
///
/// Generous, fixed reserve for everything else a SCAN request carries besides
/// the key: `tx_id` (8), the route string (realm/area/resource - normally a
/// few dozen bytes, budgeted here at up to 512), and the fixed flag/length
/// bytes for `has_start`, `start_key_len`, `has_end`, `has_limit`, `reverse`,
/// and `start_exclusive` (roughly 20). Deliberately generous so this reserve
/// stays safe even if the request envelope grows.
const KV_SCAN_CONTINUATION_REQUEST_RESERVE_BYTES: usize = 1024;

/// Largest key that can safely become a page's resume boundary.
///
/// A key longer than this may still fit comfortably in the PUT that wrote it
/// and in a SCAN response that returns it once, but re-issuing it as
/// `start_key` in a continuation request could overflow the request's own
/// wire ceiling - producing a page whose `has_more=1` promises a continuation
/// that cannot actually be sent. Bounding it here means such a key is instead
/// refused up front with an explicit error (the same one already used when a
/// pair cannot fit any response), rather than manufacturing an unusable
/// resume point.
#[must_use]
pub(crate) fn kv_scan_continuation_max_key_bytes() -> usize {
    MAX_KV_RESPONSE_PAYLOAD_BYTES.saturating_sub(KV_SCAN_CONTINUATION_REQUEST_RESERVE_BYTES)
}

/// Conservative wire cost of one encoded scan pair.
#[must_use]
pub(crate) fn kv_scan_item_wire_bytes(key_len: usize, value_len: usize) -> usize {
    KV_SCAN_ITEM_FIXED_OVERHEAD_BYTES
        .saturating_add(key_len)
        .saturating_add(value_len)
}

#[cfg(test)]
#[path = "tests/scan_wire_budget.rs"]
mod tests;
