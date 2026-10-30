use crate::domains::stream::protocol::{
    STREAM_READ_ITEM_FIXED_WIRE_OVERHEAD_BYTES, STREAM_READ_RESPONSE_ENVELOPE_OVERHEAD_BYTES,
};

/// Default Stream response budget, excluding the wire-2 header.
pub(crate) const MAX_STREAM_RESPONSE_PAYLOAD_BYTES: usize =
    crate::protocol::wire2::DEFAULT_MAX_MESSAGE_BYTES as usize
        - crate::protocol::wire2::header::HEADER_LEN;

/// The largest a read response's summed item bytes may be while still
/// leaving room for the response envelope under the configured message limit.
pub(super) fn stream_response_byte_ceiling(response_payload_limit: usize) -> usize {
    response_payload_limit.saturating_sub(STREAM_READ_RESPONSE_ENVELOPE_OVERHEAD_BYTES)
}

/// Resolve a client-requested `max_bytes` against the hard wire ceiling.
pub(super) fn bounded_max_bytes(max_bytes: Option<usize>, response_payload_limit: usize) -> usize {
    let ceiling = stream_response_byte_ceiling(response_payload_limit);
    max_bytes.map_or(ceiling, |requested| requested.min(ceiling))
}

/// Conservative worst-case wire bytes for one record encoded as an `Event`
/// item. `route_len` is the record's actual encoded route length in bytes.
pub(super) fn stream_record_wire_bytes(
    route_len: usize,
    body_len: usize,
    metadata_len: usize,
) -> usize {
    STREAM_READ_ITEM_FIXED_WIRE_OVERHEAD_BYTES
        .saturating_add(route_len)
        .saturating_add(body_len)
        .saturating_add(metadata_len)
}

/// Conservative worst-case wire bytes for one record encoded as a `Filtered`
/// marker: the route plus the same generous fixed overhead, with no body or
/// metadata. Charging a filter-excluded record its (far larger) `Event` cost
/// would let a body the client never receives stop - or reject - the read.
pub(super) fn stream_filtered_marker_wire_bytes(route_len: usize) -> usize {
    stream_record_wire_bytes(route_len, 0, 0)
}

/// Wire bytes for the item this record will actually become: the full `Event`
/// encoding when it passes the read filter, a cheap `Filtered` marker when it
/// does not. Charging a filter-excluded record its `Event` cost would let a
/// body the client never receives stop - or fail - the page.
pub(super) fn stream_read_item_wire_bytes(
    matches_filter: bool,
    route_len: usize,
    body_len: usize,
    metadata_len: usize,
) -> usize {
    if matches_filter {
        stream_record_wire_bytes(route_len, body_len, metadata_len)
    } else {
        stream_filtered_marker_wire_bytes(route_len)
    }
}

/// Byte length of `stream://{realm}/{area}/{resource}` without allocating.
pub(super) fn stream_route_len(realm: &str, area: &str, resource: &str) -> usize {
    "stream://".len() + realm.len() + 1 + area.len() + 1 + resource.len()
}

pub(super) enum WireBudgetDecision {
    /// The record fits; include it and continue.
    Include,
    /// The response is full; stop before this record and paginate.
    Stop,
}

/// Charge one item's wire bytes against a read response's running budget.
///
/// Shared by every posting-based read loop (realm-resource, global,
/// global-posting) so the "stop once full, but always make progress with a
/// lone oversized-for-`max_bytes` record" policy - and the hard-ceiling
/// handling for a record that can never fit *any* response - lives in one
/// place instead of being copy-pasted per loop.
///
/// `item_bytes` must be the cost of the item the caller will actually push:
/// `stream_record_wire_bytes` for an `Event`, `stream_filtered_marker_wire_bytes`
/// for a `Filtered` marker. Charging a filter-excluded record its full `Event`
/// cost would fail a read that only ever needed to send a cheap marker.
///
/// # Errors
///
/// Returns `Err` when `item_bytes` alone exceeds
/// `wire_byte_limit`, meaning no response could ever encode
/// this item even alone. Stream guarantees exact replay of committed history
/// (see `docs/development/domain-boundaries-spec.md`), so an event the client
/// asked for that cannot be sent must surface as an explicit, classifiable
/// error naming its offset - never as a marker that silently drops the
/// committed body from a rebuilt aggregate.
pub(super) fn charge_wire_budget(
    offset: u64,
    item_bytes: usize,
    bytes_read: usize,
    byte_limit: usize,
    wire_byte_limit: usize,
) -> Result<WireBudgetDecision, String> {
    if bytes_read.saturating_add(item_bytes) > byte_limit {
        if bytes_read > 0 {
            return Ok(WireBudgetDecision::Stop);
        }
        // A tight client-requested `max_bytes` still forces this lone item
        // through so pagination makes progress, as long as it fits in a wire
        // frame at all. Only an item that itself exceeds the hard wire
        // ceiling is rejected outright.
        if item_bytes > wire_byte_limit {
            return Err(format!(
                "ERR_READ_RESPONSE_TOO_LARGE: record at offset {offset} is {item_bytes} \
                 bytes, exceeding the {wire_byte_limit}-byte read response limit"
            ));
        }
    }
    Ok(WireBudgetDecision::Include)
}
