//! KV (Key-Value) domain client
//!
//! Provides transactional key-value operations. Routes are opaque strings —
//! the client never parses or constructs them.
//!
//! ## Wire format (matches server's `kv_codec.rs`)
//!
//! ### Requests
//!
//! BEGIN    (100): `[string route][u8 mode][u8 durable]`
//! COMMIT   (101): `[u64 tx_id][string route]`
//! ROLLBACK (102): `[u64 tx_id][string route]`
//! GET      (103): `[u64 tx_id][string route][bytes key]`
//! PUT      (104): `[u64 tx_id][string route][bytes key][bytes value]`
//! DELETE   (106): `[u64 tx_id][string route][bytes key]`
//!
//! ### Responses
//!
//! BeginOk:    `[u8 0][u64 tx_id]`
//! Ok (empty): `[u8 0]`
//! GetResult:  `[u8 0][u8 found][u32 value_len][value bytes]`
//! Error:      `[u8 1][u32 error_len][error_msg UTF-8]`

use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::connection::SharedConnection;
use crate::error::{FitzError, Result};
use crate::protocol::{message_type, TransactionMode};

/// KV domain client.
///
/// Stateless handle — all route context is passed per-call so a single
/// `KvClient` can operate across any realm/area/resource.
pub struct KvClient {
    conn: SharedConnection,
}

impl KvClient {
    pub fn new(conn: SharedConnection) -> Self {
        Self { conn }
    }

    /// Begin a transaction.
    ///
    /// `route` is a fully-qualified opaque route string, e.g.
    /// `"kv://prod/app/users"`.
    ///
    /// Returns a `KvTransaction` which holds the tx_id and route internally.
    pub fn begin(&self, route: &str, mode: TransactionMode) -> Result<KvTransaction> {
        let mut enc = PayloadEncoder::new();
        enc.put_string(route);
        enc.put_u8(mode as u8);
        enc.put_u8(0); // durable: false

        let resp = self
            .conn
            .send_request(message_type::KV_BEGIN, &enc.finish())?;

        // Server: [u8 0][u64 tx_id] on success
        let mut dec = PayloadDecoder::new(&resp);
        let status = dec.get_u8()?;
        if status == 1 {
            let msg = dec.get_string()?;
            return Err(FitzError::DomainError(msg));
        }
        let tx_id = dec.get_u64()?;

        Ok(KvTransaction {
            conn: self.conn.clone(),
            tx_id,
            route: route.to_string(),
        })
    }
}

/// A live KV transaction.
///
/// Created by `KvClient::begin()`. Holds the server-assigned `tx_id` and
/// the route internally so callers only need to pass key/value data.
///
/// Consuming methods (`commit`, `rollback`) take `self` by value to prevent
/// reuse after finalization.
pub struct KvTransaction {
    conn: SharedConnection,
    tx_id: u64,
    route: String,
}

impl KvTransaction {
    /// Get a value by key.
    ///
    /// Returns `Ok(None)` when the key does not exist.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut enc = PayloadEncoder::new();
        enc.put_u64(self.tx_id);
        enc.put_string(&self.route);
        enc.put_bytes(key);

        let resp = self
            .conn
            .send_request(message_type::KV_GET, &enc.finish())?;

        // Server: [u8 0][u8 found][u32 len][value bytes]
        let mut dec = PayloadDecoder::new(&resp);
        let status = dec.get_u8()?;
        if status == 1 {
            let msg = dec.get_string()?;
            return Err(FitzError::DomainError(msg));
        }
        let found = dec.get_u8()? != 0;
        let value = dec.get_bytes()?;

        if found && !value.is_empty() {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Put a key-value pair.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_u64(self.tx_id);
        enc.put_string(&self.route);
        enc.put_bytes(key);
        enc.put_bytes(value);

        let resp = self
            .conn
            .send_request(message_type::KV_PUT, &enc.finish())?;

        decode_ok_response(&resp)
    }

    /// Delete a key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_u64(self.tx_id);
        enc.put_string(&self.route);
        enc.put_bytes(key);

        let resp = self
            .conn
            .send_request(message_type::KV_DELETE, &enc.finish())?;

        decode_ok_response(&resp)
    }

    /// Commit the transaction (consumes self).
    pub fn commit(self) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_u64(self.tx_id);
        enc.put_string(&self.route);

        let resp = self
            .conn
            .send_request(message_type::KV_COMMIT, &enc.finish())?;

        decode_ok_response(&resp)
    }

    /// Rollback the transaction (consumes self).
    pub fn rollback(self) -> Result<()> {
        let mut enc = PayloadEncoder::new();
        enc.put_u64(self.tx_id);
        enc.put_string(&self.route);

        let resp = self
            .conn
            .send_request(message_type::KV_ROLLBACK, &enc.finish())?;

        decode_ok_response(&resp)
    }
}

/// Decode a simple Ok / Error response.
///
/// Server wire format:
///   Ok:    `[u8 0]`
///   Error: `[u8 1][u32 len][error_msg UTF-8]`
fn decode_ok_response(buf: &[u8]) -> Result<()> {
    if buf.is_empty() {
        return Err(FitzError::Codec("Empty response".into()));
    }
    let mut dec = PayloadDecoder::new(buf);
    let status = dec.get_u8()?;
    match status {
        0 => Ok(()),
        1 => {
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
    fn should_decode_begin_ok_response() {
        // [status=0][u64 tx_id=7]
        let mut buf = vec![0x00];
        buf.extend_from_slice(&7u64.to_be_bytes());

        let mut dec = PayloadDecoder::new(&buf);
        let status = dec.get_u8().unwrap();
        assert_eq!(status, 0);
        let tx_id = dec.get_u64().unwrap();
        assert_eq!(tx_id, 7);
    }

    #[test]
    fn should_decode_simple_ok_response() {
        let buf = vec![0x00];
        decode_ok_response(&buf).unwrap();
    }

    #[test]
    fn should_decode_error_response() {
        let msg = b"tx not found";
        let mut buf = vec![0x01];
        buf.extend_from_slice(&(msg.len() as u32).to_be_bytes());
        buf.extend_from_slice(msg);
        let err = decode_ok_response(&buf).unwrap_err();
        assert!(err.to_string().contains("tx not found"));
    }

    #[test]
    fn should_decode_get_result_found() {
        // [status=0][found=1][u32 len=5][value bytes]
        let mut buf = vec![0x00, 0x01];
        buf.extend_from_slice(&(5u32).to_be_bytes());
        buf.extend_from_slice(b"alice");

        let mut dec = PayloadDecoder::new(&buf);
        let status = dec.get_u8().unwrap();
        assert_eq!(status, 0);
        let found = dec.get_u8().unwrap();
        assert_eq!(found, 1);
        let value = dec.get_bytes().unwrap();
        assert_eq!(value, b"alice");
    }

    #[test]
    fn should_decode_get_result_not_found() {
        // [status=0][found=0][u32 len=0]
        let mut buf = vec![0x00, 0x00];
        buf.extend_from_slice(&(0u32).to_be_bytes());

        let mut dec = PayloadDecoder::new(&buf);
        let status = dec.get_u8().unwrap();
        assert_eq!(status, 0);
        let found = dec.get_u8().unwrap();
        assert_eq!(found, 0);
        let value = dec.get_bytes().unwrap();
        assert!(value.is_empty());
    }

    #[test]
    fn should_encode_begin_request() {
        let mut enc = PayloadEncoder::new();
        enc.put_string("kv://prod/app/users");
        enc.put_u8(TransactionMode::ReadWrite as u8);
        enc.put_u8(0);
        let payload = enc.finish();

        let mut dec = PayloadDecoder::new(&payload);
        assert_eq!(dec.get_string().unwrap(), "kv://prod/app/users");
        assert_eq!(dec.get_u8().unwrap(), 1); // ReadWrite
        assert_eq!(dec.get_u8().unwrap(), 0); // durable
        assert!(dec.is_empty());
    }

    #[test]
    fn should_encode_get_request() {
        let mut enc = PayloadEncoder::new();
        enc.put_u64(42); // tx_id
        enc.put_string("kv://prod/app/users");
        enc.put_bytes(b"user:1");
        let payload = enc.finish();

        let mut dec = PayloadDecoder::new(&payload);
        assert_eq!(dec.get_u64().unwrap(), 42);
        assert_eq!(dec.get_string().unwrap(), "kv://prod/app/users");
        assert_eq!(dec.get_bytes().unwrap(), b"user:1");
        assert!(dec.is_empty());
    }

    #[test]
    fn should_reject_empty_response() {
        let err = decode_ok_response(&[]).unwrap_err();
        assert!(err.to_string().contains("Empty"));
    }
}
