//! Lease (distributed lock) domain client
//!
//! Provides ergonomic access to Fitz lease operations. Routes are opaque
//! strings — the client never parses or constructs them.
//!
//! ## Wire format (matches server's `lease_codec.rs`)
//!
//! ### Requests
//!
//! ACQUIRE (400): `[string route][string owner_id][u64 ttl_secs]`
//! RENEW   (401): `[string route][string owner_id][u64 fencing_token][u64 ttl_secs]`
//! RELEASE (402): `[string route][string owner_id][u64 fencing_token]`
//! QUERY   (403): `[string route]`
//!
//! ### Responses (all operations)
//!
//! Success: `[u8 0][u8 has_token][u64 token if has_token=1]`
//! Error:   `[u8 1][u32 error_len][error_msg UTF-8]`
//!
//! The `[u8 has_token][optional u64]` encoding matches
//! `PayloadEncoder::put_optional_u64` on the server.

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::error::{FitzError, Result};
use crate::protocol::message_type;

/// Result of a lease acquire or renew operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseGrant {
    /// Server-generated opaque fencing token. Must be passed to renew/release.
    pub fencing_token: u64,
}

/// Result of a lease query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseStatus {
    /// Lease is currently held. The server returns the fencing token.
    Held { fencing_token: u64 },
    /// Lease is free (no holder).
    Free,
}

/// Lease domain client.
///
/// Stateless handle — all route context is passed per-call so a single
/// `LeaseClient` can operate across any realm/area/resource.
pub struct LeaseClient {
    conn: SharedConnection,
}

impl LeaseClient {
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    /// Acquire a lease.
    ///
    /// `route` is a fully-qualified opaque route string, e.g.
    /// `"lease://prod/locks/leader-election"`.
    ///
    /// Returns the fencing token on success.
    pub fn acquire(&self, route: &str, owner_id: &str, ttl_secs: u64) -> Result<LeaseGrant> {
        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_string(owner_id);
        enc.put_u64(ttl_secs);

        let resp = self
            .conn
            .send_request(message_type::LEASE_ACQUIRE, &enc.finish())?;

        let token = decode_success_response(&resp)?
            .ok_or_else(|| FitzError::Protocol("ACQUIRE response missing fencing token".into()))?;

        Ok(LeaseGrant {
            fencing_token: token,
        })
    }

    /// Renew an existing lease.
    ///
    /// `fencing_token` must match the token returned by a previous acquire/renew.
    /// Returns the (possibly new) fencing token.
    pub fn renew(
        &self,
        route: &str,
        owner_id: &str,
        fencing_token: u64,
        ttl_secs: u64,
    ) -> Result<LeaseGrant> {
        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_string(owner_id);
        enc.put_u64(fencing_token);
        enc.put_u64(ttl_secs);

        let resp = self
            .conn
            .send_request(message_type::LEASE_RENEW, &enc.finish())?;

        let token = decode_success_response(&resp)?
            .ok_or_else(|| FitzError::Protocol("RENEW response missing fencing token".into()))?;

        Ok(LeaseGrant {
            fencing_token: token,
        })
    }

    /// Release a lease.
    ///
    /// `fencing_token` must match the current token.
    pub fn release(&self, route: &str, owner_id: &str, fencing_token: u64) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_string(owner_id);
        enc.put_u64(fencing_token);

        let resp = self
            .conn
            .send_request(message_type::LEASE_RELEASE, &enc.finish())?;

        decode_success_response(&resp)?;
        Ok(())
    }

    /// Query lease status.
    ///
    /// Returns whether the lease is held (with fencing token) or free.
    pub fn query(&self, route: &str) -> Result<LeaseStatus> {
        let mut enc = PayloadEncoder::new();
        enc.put_string(route);

        let resp = self
            .conn
            .send_request(message_type::LEASE_QUERY, &enc.finish())?;

        match decode_success_response(&resp)? {
            Some(token) => Ok(LeaseStatus::Held {
                fencing_token: token,
            }),
            None => Ok(LeaseStatus::Free),
        }
    }
}

/// Decode the standard lease response format.
///
/// Server wire format (from `lease_codec::encode_response` +
/// `PayloadEncoder::put_optional_u64`):
///
///   Success: `[u8 0][u8 has_token][u64 token if has_token=1]`
///   Error:   `[u8 1][u32 error_len][error_msg UTF-8]`
///
/// Returns `Ok(Some(token))` for success with token, `Ok(None)` for
/// success without token (e.g. RELEASE), or `Err` for server errors.
fn decode_success_response(buf: &[u8]) -> Result<Option<u64>> {
    if buf.is_empty() {
        return Err(FitzError::Codec("Empty response".into()));
    }

    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;

    match status {
        0 => {
            // Success — optional u64 token (put_optional_u64 format)
            let has_token = dec.get_u8()?;
            if has_token == 1 {
                let token = dec.get_u64()?;
                Ok(Some(token))
            } else {
                Ok(None)
            }
        }
        1 => {
            // Error — [u32 len][UTF-8 msg]
            let msg = dec.get_string()?;
            Err(FitzError::DomainError(msg))
        }
        _ => Err(FitzError::Protocol(format!(
            "Unknown response status byte: {}",
            status
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_success_with_token() {
        // [status=0][has_token=1][u64 token=42]
        let mut buf = vec![0x00, 0x01];
        buf.extend_from_slice(&42u64.to_be_bytes());
        let result = decode_success_response(&buf).unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn should_decode_success_without_token() {
        // [status=0][has_token=0]
        let buf = vec![0x00, 0x00];
        let result = decode_success_response(&buf).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn should_decode_error_response() {
        // [status=1][u32 len=9]["Not found"]
        let msg = b"Not found";
        let mut buf = vec![0x01];
        buf.extend_from_slice(&(msg.len() as u32).to_be_bytes());
        buf.extend_from_slice(msg);
        let err = decode_success_response(&buf).unwrap_err();
        assert!(err.to_string().contains("Not found"));
    }

    #[test]
    fn should_encode_acquire_request() {
        let mut enc = PayloadEncoder::new();
        enc.put_string("lease://prod/locks/leader");
        enc.put_string("node-1");
        enc.put_u64(30);
        let payload = enc.finish();

        // Verify round-trip
        let mut dec = PayloadDecoder::new(&payload);
        assert_eq!(dec.get_string().unwrap(), "lease://prod/locks/leader");
        assert_eq!(dec.get_string().unwrap(), "node-1");
        assert_eq!(dec.get_u64().unwrap(), 30);
        assert!(dec.is_empty());
    }

    #[test]
    fn should_encode_renew_request() {
        let mut enc = PayloadEncoder::new();
        enc.put_string("lease://prod/locks/leader");
        enc.put_string("node-1");
        enc.put_u64(12345); // fencing token
        enc.put_u64(60); // ttl
        let payload = enc.finish();

        let mut dec = PayloadDecoder::new(&payload);
        assert_eq!(dec.get_string().unwrap(), "lease://prod/locks/leader");
        assert_eq!(dec.get_string().unwrap(), "node-1");
        assert_eq!(dec.get_u64().unwrap(), 12345);
        assert_eq!(dec.get_u64().unwrap(), 60);
        assert!(dec.is_empty());
    }

    #[test]
    fn should_encode_release_request() {
        let mut enc = PayloadEncoder::new();
        enc.put_string("lease://prod/locks/leader");
        enc.put_string("node-1");
        enc.put_u64(12345);
        let payload = enc.finish();

        let mut dec = PayloadDecoder::new(&payload);
        assert_eq!(dec.get_string().unwrap(), "lease://prod/locks/leader");
        assert_eq!(dec.get_string().unwrap(), "node-1");
        assert_eq!(dec.get_u64().unwrap(), 12345);
        assert!(dec.is_empty());
    }

    #[test]
    fn should_encode_query_request() {
        let mut enc = PayloadEncoder::new();
        enc.put_string("lease://prod/locks/leader");
        let payload = enc.finish();

        let mut dec = PayloadDecoder::new(&payload);
        assert_eq!(dec.get_string().unwrap(), "lease://prod/locks/leader");
        assert!(dec.is_empty());
    }

    #[test]
    fn should_reject_empty_response() {
        let err = decode_success_response(&[]).unwrap_err();
        assert!(err.to_string().contains("Empty"));
    }

    #[test]
    fn should_reject_unknown_status_byte() {
        let err = decode_success_response(&[0x02]).unwrap_err();
        assert!(err.to_string().contains("Unknown"));
    }
}
