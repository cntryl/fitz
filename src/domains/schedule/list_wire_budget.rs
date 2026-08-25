//! Wire-size budget for schedule LIST responses.
//!
//! A LIST response is carried as a single TLV value with a `u16` length, so
//! the page must be bounded by bytes and not only by entry count. Every entry
//! can be individually small and legal while the aggregate is unencodable.

use super::protocol::ScheduleListEntry;

/// A schedule response is carried as one TLV value, whose length prefix is a
/// `u16`. Anything past this can never be framed.
pub(crate) const MAX_SCHEDULE_RESPONSE_PAYLOAD_BYTES: usize = u16::MAX as usize;

/// Generous allowance for a list response's non-entry bytes: the success flag,
/// the total count or the version, `has_more` and continuation fields, and the end
/// sentinel. Deliberately over-counted so the ceiling stays safe if the
/// encoder's envelope grows.
const SCHEDULE_LIST_ENVELOPE_OVERHEAD_BYTES: usize = 128;

/// Generous allowance for one encoded list entry's fixed parts: the has-entry
/// marker, the delivery mode, and the length prefixes on route, cron and
/// payload.
const SCHEDULE_LIST_ENTRY_FIXED_OVERHEAD_BYTES: usize = 32;

/// Largest entry payload a list response can carry.
#[must_use]
pub(crate) fn schedule_list_response_byte_ceiling() -> usize {
    MAX_SCHEDULE_RESPONSE_PAYLOAD_BYTES.saturating_sub(SCHEDULE_LIST_ENVELOPE_OVERHEAD_BYTES)
}

/// Reject a definition that could never appear in a LIST response.
///
/// A CREATE arrives as a single TLV value, so its payload may be larger than
/// the same definition costs as a list entry. Storing one leaves a schedule
/// that fires normally but can never be listed: the page cannot be framed, so
/// the response is dropped rather than answered. Refusing it at write time is
/// the only point where the client can still do something about it.
///
/// # Errors
///
/// Returns the reason when the encoded entry would exceed the list ceiling.
pub(crate) fn validate_listable_definition(
    route: &str,
    cron: &str,
    payload_len: usize,
) -> Result<(), String> {
    let entry_bytes = SCHEDULE_LIST_ENTRY_FIXED_OVERHEAD_BYTES
        .saturating_add(route.len())
        .saturating_add(cron.len())
        .saturating_add(payload_len);
    let ceiling = schedule_list_response_byte_ceiling();
    if entry_bytes > ceiling {
        return Err(format!(
            "schedule definition is {entry_bytes} wire bytes, exceeding the {ceiling}-byte \
             limit a list response can return"
        ));
    }
    Ok(())
}

/// Conservative wire cost of one encoded `ScheduleListEntry`.
#[must_use]
pub(crate) fn schedule_list_entry_wire_bytes(entry: &ScheduleListEntry) -> usize {
    SCHEDULE_LIST_ENTRY_FIXED_OVERHEAD_BYTES
        .saturating_add(entry.route.len())
        .saturating_add(entry.cron.len())
        .saturating_add(entry.payload.len())
}
