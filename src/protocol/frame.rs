//! Frame parsing and TLV helper utilities used by transports and tests.
//!
//! This module provides small, allocation-light helpers for building and
//! parsing the FTZ framing format used by the broker: a u32 BE length, a
//! one-byte frame type, one-byte flags, a u32 BE channel id, followed by a
//! concatenation of TLVs of the form [tag u8][len u16 BE][value bytes].
//!
pub use super::tags::*;
use std::convert::TryInto;
use std::str;

/// Errors returned by frame/TLV parsing helpers.
#[derive(Debug)]
pub enum Error {
    /// Buffer is too short to contain the requested header or TLV.
    Truncated,
    /// A TLV value expected to be UTF-8 was not valid UTF-8.
    BadUtf8,
    /// Generic invalid format or a verification failure (CRC mismatch).
    Invalid,
}

/// Parsed FTZ frame header fields.
#[derive(Clone, Debug)]
pub struct Header {
    /// Frame type (one of the `FRAME_*` constants in `protocol::tags`).
    pub frame_type: u8,
    /// Flags bitmask (see `protocol::tags::FLAG_*`).
    pub flags: u8,
    /// Logical channel id for multiplexed transports.
    pub channel_id: u32,
}

/// A view over a parsed frame: header + payload slice referencing the
/// original buffer.
#[derive(Clone, Debug)]
pub struct ParsedFrame<'a> {
    /// Parsed header fields.
    pub header: Header,
    /// Slice of the TLV payload (does not include the 10-byte frame header).
    pub payload: &'a [u8],
}

/// Append a TLV to `out` using the canonical [tag][len:u16 BE][value] format.
pub fn build_tlv(tag: u8, value: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value);
}

// --- Small reusable buffer pool to reduce per-request Vec allocations ---
use once_cell::sync::Lazy;
use std::sync::Mutex;

/// Simple fixed-size pool of Vec<u8>. Not intended to be a full-featured
/// allocator — just a tiny reuse cache for small payloads to reduce churn.
static BUF_POOL: Lazy<Mutex<Vec<Vec<u8>>>> = Lazy::new(|| {
    // Pre-warm with a few buffers to cover common concurrent workloads.
    let mut v = Vec::new();
    for _ in 0..8 {
        v.push(Vec::with_capacity(512));
    }
    Mutex::new(v)
});

/// Take a buffer from the global pool or allocate a new one with default capacity.
pub fn take_buf() -> Vec<u8> {
    if let Ok(mut pool) = BUF_POOL.lock() {
        pool.pop().unwrap_or_else(|| Vec::with_capacity(512))
    } else {
        // On poisoning/failure just allocate
        Vec::with_capacity(512)
    }
}

/// Return a buffer to the pool for reuse. Buffers that grew too large are dropped
/// to avoid unbounded memory use.
pub fn return_buf(mut buf: Vec<u8>) {
    // Clear contents but keep capacity for reuse
    buf.clear();
    if buf.capacity() > 8 * 1024 {
        // drop oversized buffers
        return;
    }
    if let Ok(mut pool) = BUF_POOL.lock() {
        pool.push(buf);
    }
}

/// A buffer wrapper that returns its internal Vec to the global pool on Drop
/// unless the buffer has been consumed via `into_vec`.
#[derive(Debug)]
pub struct PooledFrame {
    buf: Option<Vec<u8>>,
}

impl PooledFrame {
    /// Create a PooledFrame from an existing Vec<u8> (takes ownership)
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self { buf: Some(v) }
    }

    /// Consume self and return the inner Vec<u8>, preventing return to pool
    pub fn into_vec(mut self) -> Vec<u8> {
        self.buf.take().unwrap_or_default()
    }

    /// Borrow the frame as a byte slice
    pub fn as_slice(&self) -> &[u8] {
        self.buf.as_deref().unwrap_or(&[])
    }
}

impl AsRef<[u8]> for PooledFrame {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Drop for PooledFrame {
    fn drop(&mut self) {
        if let Some(b) = self.buf.take() {
            // Return buffer to pool (best-effort)
            if b.capacity() <= 8 * 1024 {
                if let Ok(mut pool) = BUF_POOL.lock() {
                    pool.push(b);
                }
            }
            // drop otherwise
        }
    }
}

/// Find the first TLV with `tag` in `buf` and return a slice pointing at the
/// value bytes. Returns `None` if not found. If a TLV appears truncated
/// (length exceeds buffer) this function returns `None` to indicate parse
/// failure.
pub fn find_tlv(buf: &[u8], tag: u8) -> Option<&[u8]> {
    let mut i = 0;
    while i + 3 <= buf.len() {
        let t = buf[i];
        let l = u16::from_be_bytes([buf[i + 1], buf[i + 2]]) as usize;
        i += 3;
        if i + l > buf.len() {
            return None;
        }
        if t == tag {
            return Some(&buf[i..i + l]);
        }
        i += l;
    }
    None
}

// Frame format: [Len u32 BE][Type u8][Flags u8][Channel u32 BE] + TLV payload
/// Build a full FTZ frame with 4-byte BE length prefix, 1-byte frame type,
/// 1-byte flags, 4-byte BE channel id, and the provided TLV payload.
pub fn build_frame(frame_type: u8, flags: u8, channel_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 1 + 1 + 4 + payload.len());
    out.extend_from_slice(&[0, 0, 0, 0]);
    out.push(frame_type);
    out.push(flags);
    out.extend_from_slice(&channel_id.to_be_bytes());
    out.extend_from_slice(payload);
    let total = out.len() as u32;
    out[0..4].copy_from_slice(&total.to_be_bytes());
    out
}

/// Parse a raw frame buffer into a `ParsedFrame` view. Returns `Error::Truncated`
/// when the buffer is too short for the 10-byte header or the indicated
/// length exceeds the supplied buffer. If a CRC32 TLV is present at the end
/// of the payload it will be validated and `Error::Invalid` will be returned
/// on mismatch.
pub fn parse_frame<'a>(buf: &'a [u8]) -> Result<ParsedFrame<'a>, Error> {
    if buf.len() < 10 {
        return Err(Error::Truncated);
    }
    let total_len = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    if total_len > buf.len() {
        return Err(Error::Truncated);
    }
    let frame_type = buf[4];
    let flags = buf[5];
    let channel_id = u32::from_be_bytes(buf[6..10].try_into().unwrap());
    let payload = &buf[10..total_len];
    // Optional CRC32 TLV at the end of payload; if present, verify over payload without this TLV
    if let Some((crc_off, crc_len)) = locate_last_tlv(payload, TAG_CRC32) {
        if crc_len == 4 {
            let provided = u32::from_be_bytes([
                payload[crc_off + 3],
                payload[crc_off + 4],
                payload[crc_off + 5],
                payload[crc_off + 6],
            ]);
            let computed = crc32fast::hash(&payload[..crc_off]);
            if computed != provided {
                return Err(Error::Invalid);
            }
        }
    }
    Ok(ParsedFrame {
        header: Header {
            frame_type,
            flags,
            channel_id,
        },
        payload,
    })
}

/// A lightweight borrow-based reference to fields parsed from a PUB frame.
pub struct PubRef<'a> {
    /// Route string (UTF-8) of the publish.
    pub route: &'a str,
    /// Message id string (UTF-8).
    pub id: &'a str,
    /// Opaque body bytes.
    pub body: &'a [u8],
}

/// Parse a `ParsedFrame` that contains a PUB payload and return a `PubRef`.
/// Returns `Error::Invalid` if required TLVs are missing or `Error::BadUtf8`
/// if route/id are not valid UTF-8.
pub fn parse_pub<'a>(frame: &ParsedFrame<'a>) -> Result<PubRef<'a>, Error> {
    let route_b = find_tlv(frame.payload, TAG_ROUTE).ok_or(Error::Invalid)?;
    let id_b = find_tlv(frame.payload, TAG_ID).ok_or(Error::Invalid)?;
    let body_b = find_tlv(frame.payload, TAG_BODY).ok_or(Error::Invalid)?;
    let route = str::from_utf8(route_b).map_err(|_| Error::BadUtf8)?;
    let id = str::from_utf8(id_b).map_err(|_| Error::BadUtf8)?;
    Ok(PubRef {
        route,
        id,
        body: body_b,
    })
}

/// Registration (REG) frame parsed contents. `is_subscribe` is true when this
/// REG indicates a subscription; false for unsubscribe.
pub struct RegRef<'a> {
    /// Route string referenced by the REG frame.
    pub route: &'a str,
    /// True when this REG is a subscribe action.
    pub is_subscribe: bool,
}

/// Parse a REG frame payload and return `RegRef` with membership intent.
pub fn parse_reg<'a>(frame: &ParsedFrame<'a>) -> Result<RegRef<'a>, Error> {
    let route_b = find_tlv(frame.payload, TAG_ROUTE).ok_or(Error::Invalid)?;
    let is_sub = find_tlv(frame.payload, TAG_SUBSCRIBE).is_some();
    let route = str::from_utf8(route_b).map_err(|_| Error::BadUtf8)?;
    Ok(RegRef {
        route,
        is_subscribe: is_sub,
    })
}

// Locate the last TLV with a specific tag; returns offset to the TLV start within buf and length of value
/// Find the last occurrence of a TLV with `tag` in `buf` and return the
/// offset pointing at the TLV start (the tag byte position) and the value
/// length. Used to locate trailing TLVs such as `TAG_CRC32`.
fn locate_last_tlv(buf: &[u8], tag: u8) -> Option<(usize, usize)> {
    let mut i = 0usize;
    let mut last: Option<(usize, usize)> = None;
    while i + 3 <= buf.len() {
        let t = buf[i];
        let l = u16::from_be_bytes([buf[i + 1], buf[i + 2]]) as usize;
        if i + 3 + l > buf.len() {
            break;
        }
        if t == tag {
            last = Some((i, l));
        }
        i += 3 + l;
    }
    last
}

/// Convenience helper to build a DAT notification frame carrying a
/// subscription notification. All fields are encoded as TLVs; optional
/// values are omitted when `None`.
pub fn build_notification_frame_ex(
    route: &str,
    id: Option<&str>,
    body: &[u8],
    reply_route: Option<&str>,
    seq: Option<u32>,
    stream_end: bool,
    channel_id: u32,
) -> Vec<u8> {
    let mut p = Vec::new();
    build_tlv(TAG_NOTIFICATION, &[], &mut p);
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut p);
    if let Some(i) = id {
        build_tlv(TAG_ID, i.as_bytes(), &mut p);
    }
    build_tlv(TAG_BODY, body, &mut p);
    if let Some(rr) = reply_route {
        build_tlv(TAG_ROUTE_REPLY, rr.as_bytes(), &mut p);
    }
    if let Some(s) = seq {
        build_tlv(TAG_SEQ, &s.to_be_bytes(), &mut p);
    }
    if stream_end {
        build_tlv(TAG_STREAM_END, &[], &mut p);
    }
    build_frame(FRAME_DAT, 0, channel_id, &p)
}

/// Decode a frame's TLVs into (route, payload, route_family)
/// Assumes frame header has already been parsed.
pub fn decode(bytes: Vec<u8>) -> Result<(String, Vec<u8>, crate::routing::RouteFamilyId), String> {
    let parsed = parse_frame(&bytes).map_err(|e| format!("parse error: {:?}", e))?;

    // Find route TLV
    let route_bytes = find_tlv(parsed.payload, TAG_ROUTE).ok_or("missing route TLV")?;
    let route = std::str::from_utf8(route_bytes)
        .map_err(|e| format!("invalid route UTF-8: {}", e))?
        .to_string();

    // The payload is everything except the route TLV
    // For simplicity, return the full payload for now
    let payload = parsed.payload.to_vec();

    // Route family from route parsing
    let route_family = crate::routing::RouteFamilyId::default(); // TODO: determine from route

    Ok((route, payload, route_family))
}

/// Make an error frame
pub fn make_error(channel_id: u32, err: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    build_tlv(TAG_ERR_MSG, err.as_bytes(), &mut payload);
    build_frame(FRAME_ERR, 0, channel_id, &payload)
}
