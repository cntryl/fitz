use bytes::{BufMut, Bytes};
use fitz_core::testkit::frames::{FrameBuilder, FrameParser};

#[inline]
fn u32_len(value: usize) -> u32 {
    u32::try_from(value)
        .unwrap_or_else(|_| panic!("transport frame length exceeds u32::MAX: {value}"))
}

fn msg_type_to_u8(msg_type: u16) -> u8 {
    msg_type.to_le_bytes()[0]
}

/// Build STREAM BEGIN frame (`msg_type` 600)
#[must_use]
pub fn build_stream_begin(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());
    buf.put_u8(0);

    let mut builder = FrameBuilder::new();
    builder.encode_field(600, &buf);
    builder.build()
}

/// Build STREAM APPEND frame (`msg_type` 601)
#[must_use]
pub fn build_stream_append(session_id: u64, expected_offset: u64, data: &[u8]) -> Vec<u8> {
    build_stream_append_with_metadata(session_id, expected_offset, data, None)
}

/// Build STREAM APPEND frame (`msg_type` 601) with optional metadata.
#[must_use]
pub fn build_stream_append_with_metadata(
    session_id: u64,
    expected_offset: u64,
    data: &[u8],
    metadata: Option<&[u8]>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(session_id);
    buf.put_u64(expected_offset);
    buf.put_u32(u32_len(data.len()));
    buf.put_slice(data);
    match metadata {
        Some(metadata) => {
            buf.put_u8(1);
            buf.put_u32(u32_len(metadata.len()));
            buf.put_slice(metadata);
        }
        None => buf.put_u8(0),
    }

    let mut builder = FrameBuilder::new();
    builder.encode_field(601, &buf);
    builder.build()
}

/// Build STREAM COMMIT frame (`msg_type` 602)
#[must_use]
pub fn build_stream_commit(session_id: u64, mode: u8) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(session_id);
    buf.put_u8(mode);

    let mut builder = FrameBuilder::new();
    builder.encode_field(602, &buf);
    builder.build()
}

/// Build STREAM ROLLBACK frame (`msg_type` 603).
#[must_use]
pub fn build_stream_rollback(session_id: u64) -> Vec<u8> {
    let mut builder = FrameBuilder::new();
    builder.encode_field(603, &session_id.to_be_bytes());
    builder.build()
}

/// Build standalone STREAM DROP (`msg_type` 610), mode 0=Buffered or 1=Sync.
#[must_use]
pub fn build_stream_drop(selector: &str, mode: u8) -> Vec<u8> {
    let mut payload = crate::wire::payload_codec::PayloadEncoder::new();
    payload.put_string(selector);
    payload.put_u8(mode);
    let mut builder = FrameBuilder::new();
    builder.encode_field(610, &payload.finish());
    builder.build()
}

/// Build STREAM READ frame (`msg_type` 604)
#[must_use]
pub fn build_stream_read(route: &str, start_offset: u64) -> Vec<u8> {
    build_stream_read_with_limit(route, start_offset, 1000)
}

/// Build STREAM READ frame (`msg_type` 604) with a caller-provided limit.
#[must_use]
pub fn build_stream_read_with_limit(route: &str, start_offset: u64, limit: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());
    buf.put_u64(start_offset);
    buf.put_u64(limit);
    buf.put_u8(0);

    let mut builder = FrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

/// Build STREAM READ with an optional integrity-protected cursor token.
#[must_use]
pub fn build_stream_read_with_cursor(
    route: &str,
    start_offset: u64,
    limit: u64,
    cursor_fingerprint: Option<u64>,
) -> Vec<u8> {
    build_stream_read_with_snapshot_cursor(route, start_offset, limit, cursor_fingerprint, None)
}

/// Build STREAM READ with the complete snapshot continuation metadata.
#[must_use]
pub fn build_stream_read_with_snapshot_cursor(
    route: &str,
    start_offset: u64,
    limit: u64,
    cursor_fingerprint: Option<u64>,
    captured_watermark: Option<u64>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());
    buf.put_u64(start_offset);
    buf.put_u64(limit);
    buf.put_u8(0); // max_bytes: None
    buf.put_u8(0); // filter: None
    match cursor_fingerprint {
        Some(fingerprint) => {
            buf.put_u8(1);
            buf.put_u64(fingerprint);
        }
        None => buf.put_u8(0),
    }
    match captured_watermark {
        Some(watermark) => {
            buf.put_u8(1);
            buf.put_u64(watermark);
        }
        None => buf.put_u8(0),
    }

    let mut builder = FrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

/// Build STREAM LAST frame (`msg_type` 605)
#[must_use]
pub fn build_stream_last(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = FrameBuilder::new();
    builder.encode_field(605, &buf);
    builder.build()
}

/// Build STREAM `GET_METADATA` frame (`msg_type` 606)
#[must_use]
pub fn build_stream_get_metadata(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = FrameBuilder::new();
    builder.encode_field(606, &buf);
    builder.build()
}

/// Build STREAM SUBSCRIBE frame (`msg_type` 607)
#[must_use]
pub fn build_stream_subscribe(route_pattern: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route_pattern.len()));
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = FrameBuilder::new();
    builder.encode_field(607, &buf);
    builder.build()
}

/// Build STREAM UNSUBSCRIBE frame (`msg_type` 608)
#[must_use]
pub fn build_stream_unsubscribe(route_pattern: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route_pattern.len()));
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = FrameBuilder::new();
    builder.encode_field(608, &buf);
    builder.build()
}

/// Parse STREAM response
#[must_use]
pub fn parse_stream_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = FrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        return (msg_type_to_u8(msg_type), status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Parse `session_id` from STREAM BEGIN response data
///
/// # Errors
/// Returns an error if the response indicates failure or is not structurally valid.
pub fn parse_stream_session_id(data: &[u8]) -> Result<u64, String> {
    use crate::wire::payload_codec::PayloadDecoder;

    let mut decoder = PayloadDecoder::new(data);
    let status = decoder.get_u8()?;
    if status != 0 {
        return Err(decoder.get_string()?);
    }

    let session_id = decoder.get_u64()?;
    decoder.get_bytes()?;
    if !decoder.is_complete() {
        return Err("Trailing data in stream BEGIN response".to_string());
    }

    Ok(session_id)
}

fn decode_stream_ok_data(payload: &[u8]) -> Result<Bytes, String> {
    use crate::wire::payload_codec::PayloadDecoder;

    let mut decoder = PayloadDecoder::new(payload);
    let status = decoder.get_u8()?;
    if status != 0 {
        let error = decoder
            .get_string()
            .unwrap_or_else(|_| format!("stream response failed with status {status}"));
        return Err(error);
    }

    let _session_id = decoder.get_optional_u64()?;
    let data = decoder.get_bytes()?;
    if !decoder.is_complete() {
        return Err("Trailing data in stream success response".to_string());
    }

    Ok(data)
}

fn skip_stream_wire_record(
    decoder: &mut crate::wire::payload_codec::PayloadDecoder<'_>,
) -> Result<(), String> {
    decoder.get_u64()?;
    decoder.get_optional_u64()?;
    decoder.get_optional_u64()?;
    decoder.skip_bytes()?;
    decoder.get_optional_bytes()?;
    decoder.get_u64()?;
    Ok(())
}

fn skip_stream_read_item(
    decoder: &mut crate::wire::payload_codec::PayloadDecoder<'_>,
) -> Result<(), String> {
    match decoder.get_u8()? {
        0 => skip_stream_wire_record(decoder),
        1 => {
            decoder.get_u64()?;
            decoder.get_u8()?;
            Ok(())
        }
        2 => {
            decoder.get_u64()?;
            decoder.get_u64()?;
            decoder.get_u8()?;
            Ok(())
        }
        other => Err(format!("Unknown stream read item kind: {other}")),
    }
}

/// Count records present in a STREAM READ payload.
///
/// # Errors
/// Returns an error if the payload is malformed.
pub fn count_stream_read_records_from_payload(payload: &[u8]) -> Result<usize, String> {
    use crate::wire::payload_codec::PayloadDecoder;

    let data = decode_stream_ok_data(payload)?;
    let mut decoder = PayloadDecoder::new(&data);
    let count = u32_to_usize(decoder.get_u32()?);

    for item_index in 0..count {
        decoder.get_string()?;
        if let Err(err) = skip_stream_read_item(&mut decoder) {
            let offset = decoder.offset();
            return Err(format!(
                "stream read item {} failed at offset {}: {} (next byte = {})",
                item_index,
                offset,
                err,
                decoder.peek_u8().unwrap_or(0)
            ));
        }
    }

    decoder.get_u64()?;
    decoder.get_optional_u64()?;
    decoder.get_optional_u64()?;
    decoder.get_u8()?;

    if !decoder.is_complete() {
        return Err("Trailing data in stream read response".to_string());
    }

    Ok(count)
}

/// Parse a STREAM READ response and count its records.
///
/// # Errors
/// Returns an error when the response payload is malformed.
pub fn parse_stream_read_record_count(response: &[u8]) -> Result<usize, String> {
    let (_msg_type, _status, payload) = parse_stream_response(response);
    count_stream_read_records_from_payload(&payload)
}

fn u32_to_usize(value: u32) -> usize {
    usize::try_from(value).expect("u32 fits usize")
}
