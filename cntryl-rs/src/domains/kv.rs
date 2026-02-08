//! KV (Key-Value) domain client

use crate::error::{FitzError, Result};
use crate::codec::{TlvEncoder, TlvDecoder, encode_message_frame};
use crate::connection::FitzConnection;
use crate::protocol::{message_type, TransactionMode};
use std::sync::{Arc, Mutex};

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

        let mut dec = TlvDecoder::new(buf);

        // Try to decode as u64 (transaction ID)
        if buf.len() == 8 {
            if let Ok(tx_id) = dec.get_u64() {
                return Ok(KvResponse::TransactionId(tx_id));
            }
        }

        // Try to decode as GET response: [u8 found][u32 len][value]
        if !dec.is_empty() {
            let mut dec = TlvDecoder::new(buf);
            if let Ok(found) = dec.get_u8() {
                let found_bool = found != 0;
                if !dec.is_empty() {
                    if let Ok(value) = dec.get_bytes() {
                        return Ok(KvResponse::GetResult {
                            found: found_bool,
                            value: Some(value),
                        });
                    }
                } else {
                    return Ok(KvResponse::GetResult {
                        found: found_bool,
                        value: None,
                    });
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

        match KvResponse::decode(&resp_frame)? {
            KvResponse::TransactionId(tx_id) => {
                Ok(KvTransaction {
                    conn: self.conn.clone(),
                    tx_id,
                })
            }
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

        match KvResponse::decode(&resp_frame)? {
            KvResponse::GetResult { found, value } => {
                if found {
                    Ok(value)
                } else {
                    Ok(None)
                }
            }
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

        match KvResponse::decode(&resp_frame)? {
            KvResponse::Ok => Ok(()),
            _ => Err(FitzError::Protocol("Expected OK response".to_string())),
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

        match KvResponse::decode(&resp_frame)? {
            KvResponse::Ok => Ok(()),
            _ => Err(FitzError::Protocol("Expected OK response".to_string())),
        }
    }

    pub fn commit(self) -> Result<()> {
        let req = KvRequest::Commit { tx_id: self.tx_id };

        let payload = req.encode()?;
        let frame = encode_message_frame(message_type::KV_COMMIT, &payload);

        let mut conn = self.conn.lock().unwrap();
        conn.send_frame(&frame)?;
        let resp_frame = conn.recv_frame()?;

        match KvResponse::decode(&resp_frame)? {
            KvResponse::Ok => Ok(()),
            _ => Err(FitzError::Protocol("Expected OK response".to_string())),
        }
    }

    pub fn rollback(self) -> Result<()> {
        let req = KvRequest::Rollback { tx_id: self.tx_id };

        let payload = req.encode()?;
        let frame = encode_message_frame(message_type::KV_ROLLBACK, &payload);

        let mut conn = self.conn.lock().unwrap();
        conn.send_frame(&frame)?;
        let resp_frame = conn.recv_frame()?;

        match KvResponse::decode(&resp_frame)? {
            KvResponse::Ok => Ok(()),
            _ => Err(FitzError::Protocol("Expected OK response".to_string())),
        }
    }
}
