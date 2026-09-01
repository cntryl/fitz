//! Authoritative Error Code Allocation for All Fitz Domains
//!
//! See `CLIENT_SPEC.md` sections for each domain for the official error code allocations.
//! All domains use numeric error codes allocated in 100-block ranges.
//! Error codes are returned in domain-specific response envelopes as
//! `[u8 1][u32 code][u32 msg_len][msg]`.
//!
//! ## Backpressure Signaling (Per `CLIENT_SPEC` §Flow Control & Backpressure)
//! When a domain's internal queue is full, the broker returns a backpressure error:
//! - RPC: 6003 = `ERR_RPC_BACKPRESSURE` (client should retry with exponential backoff)
//! - Queue: 4005 = `ERR_QUEUE_FULL` (client should retry with exponential backoff)
//! - Other domains: connection may close if internal buffers overflow
//! - Notice PUBLISH: silently drops notifications on backpressure (fire-and-forget semantics, no response sent)

use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};

#[must_use]
pub fn encode_error_body(code: u16, message: &str) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_error_body_into(code, message, &mut enc)
}

pub fn encode_error_body_into(code: u16, message: &str, enc: &mut PayloadEncoder) -> Vec<u8> {
    enc.clear();
    enc.put_u8(1);
    enc.put_u32(u32::from(code));
    enc.put_string(message);
    enc.finish()
}

/// # Errors
///
/// Returns an error if the payload is not a valid Fitz error envelope.
pub fn decode_error_body(payload: &[u8]) -> Result<(u16, String), String> {
    let mut dec = PayloadDecoder::new(payload);
    if dec.get_u8()? != 1 {
        return Err("Payload is not an error envelope".to_string());
    }

    let code = dec.get_u32()?;
    if code > u32::from(u16::MAX) {
        return Err("Error code exceeds u16 range".to_string());
    }

    let message = dec.get_string()?;
    if !dec.is_complete() {
        return Err("Trailing data in error envelope".to_string());
    }

    Ok((
        u16::try_from(code).map_err(|_| "Error code exceeds u16 range".to_string())?,
        message,
    ))
}

/// KV domain error codes (per `CLIENT_SPEC` KV Domain section)
pub mod kv {
    pub const ERR_TRANSACTION_NOT_FOUND: u16 = 1001;
    pub const ERR_INVALID_MODE: u16 = 1002;
    pub const ERR_KEY_NOT_FOUND: u16 = 1003;
    pub const ERR_ISOLATION_CONFLICT: u16 = 1004;
    pub const ERR_WRITE_IN_READONLY: u16 = 1005;
    pub const ERR_KEY_EXISTS: u16 = 1006;
    pub const ERR_INVALID_ROUTE: u16 = 1007;
    pub const ERR_REALM_MISMATCH: u16 = 1008;
    pub const ERR_BACKEND_ERROR: u16 = 1009;
    pub const ERR_TRANSACTION_ABORTED: u16 = 1010;
    pub const ERR_UNAUTHORIZED: u16 = 1011; // AC-KV-010: Permission denied for KV operation
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 1012;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 1013;
    /// The domain mailbox was full, so the request was never accepted.
    ///
    /// Retryable: nothing was enqueued and nothing applied, so re-sending after
    /// a backoff is safe. Distinct from `ERR_BACKEND_ERROR`, which is fatal and
    /// says nothing about whether the request took effect.
    pub const ERR_BUSY: u16 = 1014;
}

/// Stream domain error codes (per `CLIENT_SPEC` Stream Domain section)
pub mod stream {
    pub const ERR_CONCURRENCY_CONFLICT: u16 = 2001;
    pub const ERR_SESSION_ALREADY_ACTIVE: u16 = 2002;
    pub const ERR_SESSION_NOT_FOUND: u16 = 2003;
    pub const ERR_INVALID_READ_BOUND: u16 = 2004;
    pub const ERR_RESOURCE_NOT_FOUND: u16 = 2005;
    pub const ERR_STREAM_FILTER_UNSUPPORTED_VERSION: u16 = 2006;
    pub const ERR_STREAM_FILTER_INVALID_PAYLOAD: u16 = 2007;
    pub const ERR_UNAUTHORIZED: u16 = 2009; // AC-STREAM-005: Permission denied for stream operation
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 2010;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 2011;
    pub const ERR_BACKEND_ERROR: u16 = 2012;
    /// A single record's wire-encoded size alone exceeds the maximum size of
    /// one broker response frame, so it cannot be returned by any read call.
    pub const ERR_READ_RESPONSE_TOO_LARGE: u16 = 2013;
    /// The domain mailbox was full, so the request was never accepted.
    ///
    /// Retryable: nothing was enqueued and nothing applied, so re-sending after
    /// a backoff is safe. Distinct from `ERR_BACKEND_ERROR`, which is fatal and
    /// says nothing about whether the request took effect.
    pub const ERR_BUSY: u16 = 2014;
}

/// Notice domain error codes (per `CLIENT_SPEC` Notice Domain section)
pub mod notice {
    pub const ERR_INVALID_ROUTE: u16 = 3001;
    pub const ERR_INVALID_PATTERN: u16 = 3002;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 3003;
    pub const ERR_TRANSPORT_CLOSED: u16 = 3004;
    pub const ERR_BACKEND_ERROR: u16 = 3005;
    pub const ERR_UNAUTHORIZED: u16 = 3009; // AC-NOTICE-009: Permission denied for notice operation
    /// The domain mailbox was full, so the request was never accepted.
    ///
    /// Retryable: nothing was enqueued and nothing applied, so re-sending after
    /// a backoff is safe. Distinct from `ERR_BACKEND_ERROR`, which is fatal and
    /// says nothing about whether the request took effect.
    pub const ERR_BUSY: u16 = 3006;
}

/// Queue domain error codes (per `CLIENT_SPEC` Queue Domain section)
pub mod queue {
    pub const ERR_INVALID_TOKEN: u16 = 4001; // AC-QUEUE-006: Invalid or wrong inflight token
    pub const ERR_INFLIGHT_EXPIRED: u16 = 4002;
    pub const ERR_MESSAGE_NOT_FOUND: u16 = 4003;
    pub const ERR_QUEUE_NOT_FOUND: u16 = 4004;
    pub const ERR_QUEUE_FULL: u16 = 4005; // Backpressure: queue at capacity, client should retry with backoff
    pub const ERR_BAD_REQUEST: u16 = 4006;
    pub const ERR_BACKEND_ERROR: u16 = 4007;
    pub const ERR_UNAUTHORIZED: u16 = 4009; // AC-QUEUE-008: Permission denied for queue operation
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 4010;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 4011;
}

/// Lease domain error codes (per `CLIENT_SPEC` Lease Domain section)
pub mod lease {
    pub const ERR_LEASE_HELD: u16 = 5001;
    pub const ERR_INVALID_FENCE: u16 = 5002;
    pub const ERR_LEASE_EXPIRED: u16 = 5003;
    pub const ERR_LEASE_NOT_FOUND: u16 = 5004;
    pub const ERR_INVALID_TOKEN: u16 = 5005; // AC-LEASE-009: Invalid or wrong lease token
    pub const ERR_TIMEOUT: u16 = 5006;
    pub const ERR_QUEUE_FULL: u16 = 5007;
    pub const ERR_BAD_REQUEST: u16 = 5008;
    pub const ERR_UNAUTHORIZED: u16 = 5009; // AC-LEASE-010: Permission denied for lease operation
    pub const ERR_INVALID_SUBSCRIPTION_ROUTE: u16 = 5010;
    pub const ERR_INVALID_LIST_CURSOR: u16 = 5011;
    pub const ERR_INVALID_LIST_PATTERN: u16 = 5012;
}

/// RPC domain error codes (per `CLIENT_SPEC` RPC Domain section)
pub mod rpc {
    pub const ERR_RPC_TIMEOUT: u16 = 6001;
    pub const ERR_WORKER_NOT_FOUND: u16 = 6002;
    pub const ERR_RPC_BACKPRESSURE: u16 = 6003; // Backpressure: queue at capacity, client should retry with backoff
    pub const ERR_ROUTE_NOT_REGISTERED: u16 = 6004; // AC-RPC-003: No workers registered for route
    pub const ERR_CORRELATION_NOT_FOUND: u16 = 6005;
    pub const ERR_RPC_INVALID_SEQUENCE: u16 = 6006; // Response chunks must start at seq=0 and advance contiguously
    pub const ERR_RPC_DUPLICATE_CORRELATION: u16 = 6007; // Caller attempted to reuse a live correlation ID
    pub const ERR_RPC_WRONG_WORKER: u16 = 6008; // Response or ACK came from a worker that does not own the request
    pub const ERR_UNAUTHORIZED: u16 = 6009; // AC-RPC-007, AC-RPC-008: Permission denied for RPC operation
    pub const ERR_BACKEND_ERROR: u16 = 6010;
    pub const ERR_INVALID_ROUTE: u16 = 6011;
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 6012;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 6013;
}

/// Schedule domain error codes (per `CLIENT_SPEC` Schedule Domain section)
pub mod schedule {
    pub const ERR_SCHEDULE_NOT_FOUND: u16 = 7001;
    pub const ERR_INVALID_CRON: u16 = 7002; // AC-SCHEDULE-002: Invalid cron expression syntax
    pub const ERR_SCHEDULE_LIMIT: u16 = 7003;
    pub const ERR_PARSE_ERROR: u16 = 7004;
    pub const ERR_INVALID_TARGET: u16 = 7005;
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 7006;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 7007;
    pub const ERR_INVALID_DELIVERY_MODE: u16 = 7008;
    pub const ERR_UNAUTHORIZED: u16 = 7009; // AC-SCHEDULE-008: Permission denied for schedule operation
                                            // Schedule had no generic backend/saturation code; every other domain has
                                            // one. Without it a busy schedule actor had to borrow a code that means
                                            // something else (e.g. "invalid cron"), which misdirects the client.
    pub const ERR_BACKEND_ERROR: u16 = 7010;
    // Distinct from 7010 on purpose. 7010 is classified retryable: it reports a
    // request the broker declined to service, so re-sending it is safe. A
    // deadline expiry is not that - the command was already accepted and may
    // still apply - so it needs a code clients will not auto-retry.
    pub const ERR_TIMEOUT: u16 = 7011;
}
