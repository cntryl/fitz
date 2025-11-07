//! Frame flags, TLV tag values and canonical frame type constants used by the wire
//! protocol implementation.
//!
//! Each `TAG_*` constant corresponds to an 8-bit TLV tag used inside FTZ frames.
//! The `FRAME_*` constants identify high-level frame types carried in the frame header.

// ---------------------------------------------------------------------------
// Frame header flags (bitmask)
// ---------------------------------------------------------------------------
/// Payload compressed (application-specific). When set, the receiver should
/// decompress the TLV payload before parsing TLVs.
pub const FLAG_COMPRESSED: u8 = 1 << 0;

/// Payload encrypted (application-specific). When set the receiver should
/// decrypt before parsing TLVs.
pub const FLAG_ENCRYPTED: u8 = 1 << 1;

/// When set, the sender requests an ACK frame from the receiver. Used for
/// flow control and at-least-once delivery windows.
pub const FLAG_ACK_REQUIRED: u8 = 1 << 2;

/// Final fragment flag for multi-part frames/streams.
pub const FLAG_FINAL: u8 = 1 << 3;

// ---------------------------------------------------------------------------
// Common TLVs
// ---------------------------------------------------------------------------
/// Route or topic for the message (UTF-8 string).
pub const TAG_ROUTE: u8 = 0x20;

/// Per-message identifier (UTF-8 string). Used for de-duplication and RPC
/// correlation when present.
pub const TAG_ID: u8 = 0x21;

/// Opaque body bytes for the message. The broker treats this as a byte blob;
/// the wire format does not interpret message contents as JSON.
pub const TAG_BODY: u8 = 0x22;

/// Optional reply route used for RPC-style request/response flows.
pub const TAG_ROUTE_REPLY: u8 = 0x23;

/// Sequence number for stream records (big-endian u32/u64 depending on use).
pub const TAG_SEQ: u8 = 0x24;

/// Stream end marker (empty TLV, presence indicates stream termination).
pub const TAG_STREAM_END: u8 = 0x25;

// ---------------------------------------------------------------------------
// Queue / lease related TLVs
// ---------------------------------------------------------------------------
/// Requested or granted lease/visibility in seconds (u32 encoded BE).
pub const TAG_LEASE: u8 = 0x76;

/// Delivery token (opaque) returned with DUTs for later lease-extend/consume
/// operations.
pub const TAG_DELIVERY_TOKEN: u8 = 0x77;

/// Per-message TTL override (u64 seconds BE). When omitted the queue default
/// TTL applies.
pub const TAG_TTL_SECS: u8 = 0x70;

// ---------------------------------------------------------------------------
// Subscription / notice TLVs
// ---------------------------------------------------------------------------
/// Subscribe indicator (empty TLV when present signals subscribe action).
pub const TAG_SUBSCRIBE: u8 = 0x90;

/// Unsubscribe indicator (empty TLV marks unsubscribe action).
pub const TAG_UNSUBSCRIBE: u8 = 0x91;

/// Notification marker used by DAT frames to indicate the payload is a
/// subscription notification.
pub const TAG_NOTIFICATION: u8 = 0x92;

// ---------------------------------------------------------------------------
// Stream (OCC and metadata) TLVs
// ---------------------------------------------------------------------------
/// Expected revision for conditional appends (u64 BE or sentinel).
pub const TAG_EXPECTED_REV: u8 = 0xA0;

/// Assigned revision returned by the server (u64 BE).
pub const TAG_ASSIGNED_REV: u8 = 0xA1;

/// First assigned revision in a batch append (u64 BE).
pub const TAG_FIRST_ASSIGNED_REV: u8 = 0xA2;

/// Optional metadata attached to stream events (opaque bytes; JSON/CBOR)
pub const TAG_METADATA: u8 = 0xA3;

/// Area sequence number for finalized stream events (u64 BE).
pub const TAG_AREA_SEQ: u8 = 0xB0;

/// Timestamp for stream events (u64 BE, epoch seconds).
pub const TAG_TIMESTAMP: u8 = 0xB1;

// ---------------------------------------------------------------------------
// Control / error / auth TLVs
// ---------------------------------------------------------------------------
/// Authorization bearer token (UTF-8 string). The broker will validate this
/// with the configured authn/authz provider.
pub const TAG_TOKEN: u8 = 0x10;

/// Numeric error code (u32 or u16 encoded as bytes; spec defines codes).
pub const TAG_ERR_CODE: u8 = 0x40;

/// Human readable error message (UTF-8 string).
pub const TAG_ERR_MSG: u8 = 0x41;

/// Optional request identifier (opaque) that can be echoed in replies.
pub const TAG_REQ_ID: u8 = 0x72;

/// Optional CRC32 TLV placed at the end of a payload; value is 4 bytes BE
/// containing the CRC computed over the payload that excludes this TLV.
pub const TAG_CRC32: u8 = 0xFE;

/// Proposed ACK window (u32 BE) supplied by clients in HELLO/CONNECT frames.
pub const TAG_ACK_WINDOW: u8 = 0x60;

// ---------------------------------------------------------------------------
// Frame type constants
// ---------------------------------------------------------------------------
/// Connection open / HELLO frame. Carries `TAG_TOKEN` and optional `TAG_ACK_WINDOW`.
pub const FRAME_CONN_OPEN: u8 = 0x01;

/// Connection close / AUTH frame. Often carries `TAG_TOKEN` for session auth.
pub const FRAME_CONN_CLOSE: u8 = 0x02;

/// Acknowledgement frame used to confirm processing of a prior frame.
pub const FRAME_ACK: u8 = 0x03;

/// Register frame (for subscribe/unsubscribe semantics).
pub const FRAME_REG: u8 = 0x05;

/// Generic request frame used for queue/lease operations.
pub const FRAME_REQ: u8 = 0x06;

/// Publish frame used to append messages to queues/streams or trigger notices.
pub const FRAME_PUB: u8 = 0x07;

/// Data frame that carries notifications or consumed messages back to clients.
pub const FRAME_DAT: u8 = 0x08;

/// Error frame for reporting protocol or application-level errors.
pub const FRAME_ERR: u8 = 0x0B;
