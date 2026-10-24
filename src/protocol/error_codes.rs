//! Authoritative error code allocation for all Fitz domains, wire format 2.
//!
//! The human mirror is `docs/clients/spec/error-codes.md`; the class and
//! typical effect of every code live in [`crate::protocol::wire2::catalog`].
//! Every error travels in the one envelope
//! `[u8 1][u16 code][u8 class][u8 effect][string message]`.
//!
//! Blocks are decimal thousands per domain. In every block, slots `x001`
//! through `x010` carry the same meaning: unauthorized, invalid route,
//! invalid pattern, registration limit, busy, backend, timeout, bad request,
//! not found, unsupported. Domain-specific codes start at `x050`.
//!
//! Backpressure is `BUSY` in every domain with `effect = NOT_APPLIED`: the
//! broker declined the request, nothing applied, re-sending after a backoff
//! is safe. Notice `PUBLISH` without `ACK_REQUESTED` is still dropped
//! silently under backpressure.

use crate::protocol::payload_codec::PayloadEncoder;
use crate::protocol::wire2::catalog;
use crate::protocol::wire2::envelope::{self, Effect, ErrorClass, Response, WireError};

/// Build the error a client sees for `code`, taking the class and typical
/// effect from the catalog. An unknown code is reported as a backend failure
/// with an indeterminate effect, the most conservative reading.
#[must_use]
pub fn wire_error(code: u16, message: &str) -> WireError {
    let (class, effect) = catalog::lookup(code)
        .map_or((ErrorClass::Backend, Effect::Indeterminate), |entry| {
            (entry.class, entry.effect)
        });
    debug_assert!(
        catalog::lookup(code).is_some(),
        "error code {code} is not in the catalog"
    );
    WireError {
        code,
        class,
        effect,
        message: message.to_owned(),
    }
}

#[must_use]
pub fn encode_error_body(code: u16, message: &str) -> Vec<u8> {
    envelope::encode_error(&wire_error(code, message))
}

/// Encode an error envelope into a reusable encoder, replacing its contents.
pub fn encode_error_body_into(code: u16, message: &str, enc: &mut PayloadEncoder) -> Vec<u8> {
    enc.clear();
    enc.put_raw(&encode_error_body(code, message));
    enc.finish()
}

/// Decode an error envelope to its code and message.
///
/// # Errors
///
/// Returns an error if the payload is not a valid error envelope.
pub fn decode_error_body(payload: &[u8]) -> Result<(u16, String), String> {
    match envelope::decode_response(payload).map_err(|error| error.to_string())? {
        Response::Error(error) => Ok((error.code, error.message)),
        Response::Success(_) => Err("Payload is not an error envelope".to_string()),
    }
}

/// Decode the full error envelope.
///
/// # Errors
///
/// Returns an error if the payload is not a valid error envelope.
pub fn decode_wire_error(payload: &[u8]) -> Result<WireError, String> {
    match envelope::decode_response(payload).map_err(|error| error.to_string())? {
        Response::Error(error) => Ok(error),
        Response::Success(_) => Err("Payload is not an error envelope".to_string()),
    }
}

/// Protocol and session block (0xxx): answered before a request reaches a domain.
pub mod protocol {
    pub const ERR_UNAUTHORIZED: u16 = 1;
    pub const ERR_BUSY: u16 = 5;
    pub const ERR_BAD_REQUEST: u16 = 8;
    pub const ERR_UNSUPPORTED: u16 = 10;
}

/// KV domain (block 1xxx).
pub mod kv {
    pub const ERR_UNAUTHORIZED: u16 = 1001;
    pub const ERR_INVALID_ROUTE: u16 = 1002;
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 1003;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 1004;
    pub const ERR_BUSY: u16 = 1005;
    pub const ERR_BACKEND_ERROR: u16 = 1006;
    pub const ERR_TIMEOUT: u16 = 1007;
    pub const ERR_BAD_REQUEST: u16 = 1008;
    pub const ERR_NOT_FOUND: u16 = 1009;
    pub const ERR_UNSUPPORTED: u16 = 1010;
    pub const ERR_TRANSACTION_NOT_FOUND: u16 = 1050;
    pub const ERR_INVALID_MODE: u16 = 1051;
    pub const ERR_KEY_NOT_FOUND: u16 = 1052;
    pub const ERR_ISOLATION_CONFLICT: u16 = 1053;
    pub const ERR_WRITE_IN_READONLY: u16 = 1054;
    pub const ERR_KEY_EXISTS: u16 = 1055;
    pub const ERR_REALM_MISMATCH: u16 = 1056;
    pub const ERR_TRANSACTION_ABORTED: u16 = 1057;
}

/// Stream domain (block 2xxx).
pub mod stream {
    pub const ERR_UNAUTHORIZED: u16 = 2001;
    pub const ERR_INVALID_ROUTE: u16 = 2002;
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 2003;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 2004;
    pub const ERR_BUSY: u16 = 2005;
    pub const ERR_BACKEND_ERROR: u16 = 2006;
    pub const ERR_TIMEOUT: u16 = 2007;
    pub const ERR_BAD_REQUEST: u16 = 2008;
    pub const ERR_NOT_FOUND: u16 = 2009;
    pub const ERR_UNSUPPORTED: u16 = 2010;
    pub const ERR_CONCURRENCY_CONFLICT: u16 = 2050;
    pub const ERR_SESSION_ALREADY_ACTIVE: u16 = 2051;
    pub const ERR_SESSION_NOT_FOUND: u16 = 2052;
    pub const ERR_INVALID_READ_BOUND: u16 = 2053;
    pub const ERR_RESOURCE_NOT_FOUND: u16 = 2054;
    pub const ERR_STREAM_FILTER_UNSUPPORTED_VERSION: u16 = 2055;
    pub const ERR_STREAM_FILTER_INVALID_PAYLOAD: u16 = 2056;
    /// A single record's wire-encoded size alone exceeds `max_message_bytes`.
    pub const ERR_READ_RESPONSE_TOO_LARGE: u16 = 2057;
}

/// Notice domain (block 3xxx).
pub mod notice {
    pub const ERR_UNAUTHORIZED: u16 = 3001;
    pub const ERR_INVALID_ROUTE: u16 = 3002;
    pub const ERR_INVALID_PATTERN: u16 = 3003;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 3004;
    pub const ERR_BUSY: u16 = 3005;
    pub const ERR_BACKEND_ERROR: u16 = 3006;
    pub const ERR_TIMEOUT: u16 = 3007;
    pub const ERR_BAD_REQUEST: u16 = 3008;
    pub const ERR_NOT_FOUND: u16 = 3009;
    pub const ERR_UNSUPPORTED: u16 = 3010;
    pub const ERR_TRANSPORT_CLOSED: u16 = 3050;
}

/// Queue domain (block 4xxx).
pub mod queue {
    pub const ERR_UNAUTHORIZED: u16 = 4001;
    pub const ERR_INVALID_ROUTE: u16 = 4002;
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 4003;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 4004;
    pub const ERR_BUSY: u16 = 4005;
    pub const ERR_BACKEND_ERROR: u16 = 4006;
    pub const ERR_TIMEOUT: u16 = 4007;
    pub const ERR_BAD_REQUEST: u16 = 4008;
    pub const ERR_NOT_FOUND: u16 = 4009;
    pub const ERR_UNSUPPORTED: u16 = 4010;
    pub const ERR_INVALID_TOKEN: u16 = 4050;
    pub const ERR_INFLIGHT_EXPIRED: u16 = 4051;
    pub const ERR_MESSAGE_NOT_FOUND: u16 = 4052;
    pub const ERR_QUEUE_NOT_FOUND: u16 = 4053;
}

/// Lease domain (block 5xxx).
pub mod lease {
    pub const ERR_UNAUTHORIZED: u16 = 5001;
    pub const ERR_INVALID_ROUTE: u16 = 5002;
    /// SUBSCRIBE and LIST selector grammar.
    pub const ERR_INVALID_PATTERN: u16 = 5003;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 5004;
    pub const ERR_BUSY: u16 = 5005;
    pub const ERR_BACKEND_ERROR: u16 = 5006;
    /// Broker-side deadline after acceptance; not the acquire wait.
    pub const ERR_TIMEOUT: u16 = 5007;
    pub const ERR_BAD_REQUEST: u16 = 5008;
    pub const ERR_NOT_FOUND: u16 = 5009;
    pub const ERR_UNSUPPORTED: u16 = 5010;
    pub const ERR_LEASE_HELD: u16 = 5050;
    pub const ERR_INVALID_FENCE: u16 = 5051;
    pub const ERR_LEASE_EXPIRED: u16 = 5052;
    pub const ERR_LEASE_NOT_FOUND: u16 = 5053;
    pub const ERR_INVALID_TOKEN: u16 = 5054;
    /// The caller's `wait_seconds` elapsed without a grant.
    pub const ERR_ACQUIRE_WAIT_EXPIRED: u16 = 5055;
    /// Too many pending waiters on the key; back off.
    pub const ERR_WAITERS_LIMIT: u16 = 5056;
    pub const ERR_INVALID_LIST_CURSOR: u16 = 5057;
}

/// RPC domain (block 6xxx).
pub mod rpc {
    pub const ERR_UNAUTHORIZED: u16 = 6001;
    pub const ERR_INVALID_ROUTE: u16 = 6002;
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 6003;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 6004;
    pub const ERR_BUSY: u16 = 6005;
    pub const ERR_BACKEND_ERROR: u16 = 6006;
    /// Accepted but not finished before its deadline; outcome unknown.
    pub const ERR_TIMEOUT: u16 = 6007;
    pub const ERR_BAD_REQUEST: u16 = 6008;
    pub const ERR_NOT_FOUND: u16 = 6009;
    pub const ERR_UNSUPPORTED: u16 = 6010;
    pub const ERR_WORKER_NOT_FOUND: u16 = 6050;
    pub const ERR_ROUTE_NOT_REGISTERED: u16 = 6051;
    pub const ERR_CORRELATION_NOT_FOUND: u16 = 6052;
    pub const ERR_RPC_INVALID_SEQUENCE: u16 = 6053;
    pub const ERR_RPC_DUPLICATE_CORRELATION: u16 = 6054;
    pub const ERR_RPC_WRONG_WORKER: u16 = 6055;
}

/// Schedule domain (block 7xxx).
pub mod schedule {
    pub const ERR_UNAUTHORIZED: u16 = 7001;
    pub const ERR_INVALID_ROUTE: u16 = 7002;
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 7003;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 7004;
    /// The saturation code; the broker declined the request, nothing applied.
    pub const ERR_BUSY: u16 = 7005;
    pub const ERR_BACKEND_ERROR: u16 = 7006;
    pub const ERR_TIMEOUT: u16 = 7007;
    pub const ERR_BAD_REQUEST: u16 = 7008;
    pub const ERR_NOT_FOUND: u16 = 7009;
    pub const ERR_UNSUPPORTED: u16 = 7010;
    pub const ERR_SCHEDULE_NOT_FOUND: u16 = 7050;
    pub const ERR_INVALID_CRON: u16 = 7051;
    pub const ERR_DEFINITION_LIMIT: u16 = 7052;
    pub const ERR_INVALID_TARGET: u16 = 7053;
    pub const ERR_INVALID_DELIVERY_MODE: u16 = 7054;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_a_coded_error_with_its_catalog_class() {
        // Arrange
        let body = encode_error_body(kv::ERR_ISOLATION_CONFLICT, "lost the race");

        // Act
        let decoded = decode_wire_error(&body).expect("error envelope");

        // Assert
        assert_eq!(decoded.code, 1053);
        assert_eq!(decoded.class, ErrorClass::Domain);
        assert_eq!(decoded.effect, Effect::NotApplied);
        assert_eq!(
            decode_error_body(&body),
            Ok((1053, "lost the race".to_owned()))
        );
    }

    #[test]
    fn should_give_every_named_constant_a_catalog_row() {
        // Arrange
        let named = [
            protocol::ERR_UNSUPPORTED,
            kv::ERR_TRANSACTION_ABORTED,
            stream::ERR_READ_RESPONSE_TOO_LARGE,
            notice::ERR_TRANSPORT_CLOSED,
            queue::ERR_QUEUE_NOT_FOUND,
            lease::ERR_INVALID_LIST_CURSOR,
            rpc::ERR_RPC_WRONG_WORKER,
            schedule::ERR_INVALID_DELIVERY_MODE,
            schedule::ERR_BUSY,
        ];

        // Act
        let missing = named
            .iter()
            .filter(|code| catalog::lookup(**code).is_none())
            .copied()
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(missing, Vec::<u16>::new());
    }
}
