//! KV (Key-Value) domain client

use crate::error::{FitzError, Result};
use crate::codec::{TlvEncoder, encode_message_frame, decode_message_frame};
use crate::connection::FitzConnection;
use crate::protocol::{message_type, TransactionMode};
use std::sync::{Arc, Mutex};

/// Strip the TLV header from a response frame and return just the payload bytes.
fn strip_tlv_header(frame: &[u8]) -> Result<&[u8]> {
    if frame.is_empty() {
        return Ok(frame);
    }
    let (_msg_type, payload_start) = decode_message_frame(frame)?;
    Ok(&frame[payload_start..])
}

/// KV request types
pub enum KvRequest {
    Begin {
        resource: String,
        mode: TransactionMode,
        durable: bool,
    },
    Get {
        tx_id: u64,
        key: Vec<u8>,
    },
    Put {
        tx_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        tx_id: u64,
        key: Vec<u8>,
    },
    Commit {
        tx_id: u64,
    },
    Rollback {
        tx_id: u64,
    },
}

impl KvRequest {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut enc = TlvEncoder::new();
        
        match self {
            KvRequest::Begin { resource, mode, durable } => {
                enc.put_string(resource);
                enc.put_u8(*mode as u8);
                enc.put_u8(if *durable { 1 } else { 0 });
                Ok(enc.finish())
            }
            KvRequest::Get { tx_id, key } => {
                enc.put_u64(*tx_id);
                enc.put_bytes(key);
                Ok(enc.finish())
            }
            KvRequest::Put { tx_id, key, value } => {
                enc.put_u64(*tx_id);
                enc.put_bytes(key);
                enc.put_bytes(value);
                Ok(enc.finish())
            }
            KvRequest::Delete { tx_id, key } => {
                enc.put_u64(*tx_id);
                enc.put_bytes(key);
                Ok(enc.finish())
            }
            KvRequest::Commit { tx_id } => {
                enc.put_u64(*tx_id);
                Ok(enc.finish())
            }
            KvRequest::Rollback { tx_id } => {
                enc.put_u64(*tx_id);
                Ok(enc.finish())
            }
        }
    }
}

/// KV response types
pub enum KvResponse {
    TransactionId(u64),
    GetResult { found: bool, value: Option<Vec<u8>> },
    Ok,
    Error(String),
}

impl KvResponse {
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.is_empty() {
            // Empty response means OK
            return Ok(KvResponse::Ok);
        }

        // Try to decode as u64 (transaction ID) — only for BEGIN responses
        if buf.len() == 8 {
            let tx_id = u64::from_be_bytes([
                buf[0], buf[1], buf[2], buf[3],
                buf[4], buf[5], buf[6], buf[7],
            ]);
            return Ok(KvResponse::TransactionId(tx_id));
        }

        // Try to decode as GET response: [u8 found][u32 len][value]
        // The found flag is 0 or 1; an error's first byte would be 0 only
        // if the error message length fits in the upper 3 bytes (very large).
        if !buf.is_empty() && (buf[0] == 0 || buf[0] == 1) {
            let found_bool = buf[0] != 0;
            if buf.len() == 1 {
                // found=false with no value payload
                return Ok(KvResponse::GetResult {
                    found: found_bool,
                    value: None,
                });
            }
            if buf.len() >= 5 {
                let value_len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                if 5 + value_len == buf.len() {
                    let value = buf[5..].to_vec();
                    return Ok(KvResponse::GetResult {
                        found: found_bool,
                        value: if value.is_empty() { None } else { Some(value) },
                    });
                }
            }
        }

        // Try to decode as error: [u32 error_len][error_msg_bytes]
        if buf.len() >= 4 {
            let error_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if 4 + error_len == buf.len() {
                if let Ok(msg) = std::str::from_utf8(&buf[4..]) {
                    return Ok(KvResponse::Error(msg.to_string()));
                }
            }
        }

        Ok(KvResponse::Ok)
    }
}

pub struct KvClient {
    conn: Arc<Mutex<FitzConnection>>,
    realm: String,
    route_family: u64,
}

impl KvClient {
    pub fn new(conn: Arc<Mutex<FitzConnection>>, realm: String, route_family: u64) -> Self {
        Self {
            conn,
            realm,
            route_family,
        }
    }

    /// Begin a transaction
    pub fn begin(&self, area: &str, resource: &str, mode: TransactionMode) -> Result<KvTransaction> {
        let resource_path = format!("{}/{}/{}", self.realm, area, resource);
        
        let req = KvRequest::Begin {
            resource: resource_path,
            mode,
            durable: false,
        };

        let payload = req.encode()?;
        let frame = encode_message_frame(message_type::KV_BEGIN, &payload);

        let mut conn = self.conn.lock().unwrap();
        conn.send_frame(&frame)?;
        let resp_frame = conn.recv_frame()?;
        let payload = strip_tlv_header(&resp_frame)?;

        match KvResponse::decode(payload)? {
            KvResponse::TransactionId(tx_id) => {
                Ok(KvTransaction {
                    conn: self.conn.clone(),
                    tx_id,
                })
            }
            KvResponse::Error(e) => Err(FitzError::Protocol(e)),
            _ => Err(FitzError::Protocol("Expected transaction ID".to_string())),
        }
    }
}

pub struct KvTransaction {
    conn: Arc<Mutex<FitzConnection>>,
    tx_id: u64,
}

impl KvTransaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let req = KvRequest::Get {
            tx_id: self.tx_id,
            key: key.to_vec(),
        };

        let payload = req.encode()?;
        let frame = encode_message_frame(message_type::KV_GET, &payload);

        let mut conn = self.conn.lock().unwrap();
        conn.send_frame(&frame)?;
        let resp_frame = conn.recv_frame()?;
        let payload = strip_tlv_header(&resp_frame)?;

        match KvResponse::decode(payload)? {
            KvResponse::GetResult { found, value } => {
                if found {
                    Ok(value)
                } else {
                    Ok(None)
                }
            }
            KvResponse::Error(e) => Err(FitzError::Protocol(e)),
            _ => Err(FitzError::Protocol("Expected GET result".to_string())),
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let req = KvRequest::Put {
            tx_id: self.tx_id,
            key: key.to_vec(),
            value: value.to_vec(),
        };

        let payload = req.encode()?;
        let frame = encode_message_frame(message_type::KV_PUT, &payload);

        let mut conn = self.conn.lock().unwrap();
        conn.send_frame(&frame)?;
        let resp_frame = conn.recv_frame()?;
        let payload = strip_tlv_header(&resp_frame)?;

        match KvResponse::decode(payload)? {
            KvResponse::Ok => Ok(()),
            KvResponse::Error(e) => Err(FitzError::Protocol(e)),
            _ => Err(FitzError::Protocol("Expected OK response for PUT".to_string())),
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let req = KvRequest::Delete {
            tx_id: self.tx_id,
            key: key.to_vec(),
        };

        let payload = req.encode()?;
        let frame = encode_message_frame(message_type::KV_DELETE, &payload);

        let mut conn = self.conn.lock().unwrap();
        conn.send_frame(&frame)?;
        let resp_frame = conn.recv_frame()?;
        let payload = strip_tlv_header(&resp_frame)?;

        match KvResponse::decode(payload)? {
            KvResponse::Ok => Ok(()),
            KvResponse::Error(e) => Err(FitzError::Protocol(e)),
            _ => Err(FitzError::Protocol("Expected OK response for DELETE".to_string())),
        }
    }

    pub fn commit(self) -> Result<()> {
        let req = KvRequest::Commit { tx_id: self.tx_id };

        let payload = req.encode()?;
        let frame = encode_message_frame(message_type::KV_COMMIT, &payload);

        let mut conn = self.conn.lock().unwrap();
        conn.send_frame(&frame)?;
        let resp_frame = conn.recv_frame()?;
        let payload = strip_tlv_header(&resp_frame)?;

        match KvResponse::decode(payload)? {
            KvResponse::Ok => Ok(()),
            KvResponse::Error(e) => Err(FitzError::Protocol(e)),
            _ => Err(FitzError::Protocol("Expected OK response for COMMIT".to_string())),
        }
    }

    pub fn rollback(self) -> Result<()> {
        let req = KvRequest::Rollback { tx_id: self.tx_id };

        let payload = req.encode()?;
        let frame = encode_message_frame(message_type::KV_ROLLBACK, &payload);

        let mut conn = self.conn.lock().unwrap();
        conn.send_frame(&frame)?;
        let resp_frame = conn.recv_frame()?;
        let payload = strip_tlv_header(&resp_frame)?;

        match KvResponse::decode(payload)? {
            KvResponse::Ok => Ok(()),
            KvResponse::Error(e) => Err(FitzError::Protocol(e)),
            _ => Err(FitzError::Protocol("Expected OK response for ROLLBACK".to_string())),
        }
    }
}
