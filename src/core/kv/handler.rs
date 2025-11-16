// KV domain handler - routes all kv:// operations

use super::service::KvService;
use super::types::KvOperation;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::protocol::tags::{TAG_BODY, TAG_ERR_MSG, TAG_ID};
use crate::storage::traits::KvStore;
use std::sync::Arc;

/// Simple struct to hold parsed `TAG_ID`/`TAG_BODY` for KV operations
#[derive(Debug, Clone)]
pub struct TlvKeyValue {
    pub key: Option<String>,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct KvDomain {
    service: Arc<KvService>,
}

impl KvDomain {
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            service: Arc::new(KvService::new(kv_store)),
        }
    }

    /// Expose shared service for direct benching without TLV overhead
    pub fn get_service(&self) -> Arc<KvService> {
        Arc::clone(&self.service)
    }

    /// Parse TLV body to extract key (TAG_ID) and value (TAG_BODY)
    /// Supports extended length encoding (255 = 4-byte length follows)
    /// Parse TLV body to extract key (TAG_ID) and value (TAG_BODY)
    /// Supports extended length encoding (255 = 4-byte length follows)
    fn parse_tlv_body(body: &[u8]) -> TlvKeyValue {
        let mut key = None;
        let mut value = None;
        let mut i = 0;

        while i < body.len() {
            if i + 2 > body.len() {
                break;
            }

            let tag = body[i];
            let len_byte = body[i + 1];
            i += 2;

            // Handle extended length encoding
            let len = if len_byte == 255 {
                // Extended length: next 4 bytes contain the actual length
                if i + 4 > body.len() {
                    break;
                }
                let len_bytes = [body[i], body[i + 1], body[i + 2], body[i + 3]];
                i += 4;
                u32::from_be_bytes(len_bytes) as usize
            } else {
                len_byte as usize
            };

            if i + len > body.len() {
                break;
            }

            let data = &body[i..i + len];
            i += len;

            match tag {
                TAG_ID => {
                    if let Ok(s) = String::from_utf8(data.to_vec()) {
                        key = Some(s);
                    }
                }
                TAG_BODY => {
                    value = Some(data.to_vec());
                }
                _ => {} // Ignore unknown tags
            }
        }

        TlvKeyValue { key, value }
    }

    /// Build TLV response with body or error
    /// For larger bodies, we encode them in chunks or use a better length encoding
    fn build_tlv_response(result: Result<Option<Vec<u8>>, String>) -> Vec<u8> {
        let mut response = Vec::new();

        match result {
            Ok(Some(body)) => {
                // Success with body - handle multi-byte length encoding
                response.push(TAG_BODY);

                if body.len() <= 255 {
                    // Single byte length for small bodies
                    response.push(body.len() as u8);
                    response.extend_from_slice(&body);
                } else {
                    // For larger bodies, we need extended TLV encoding
                    // Use 255 as marker for extended length, followed by 4-byte length
                    response.push(255);
                    let len = body.len() as u32;
                    response.extend_from_slice(&len.to_be_bytes());
                    response.extend_from_slice(&body);
                }
            }
            Ok(None) => {
                // Success with no body
                response.push(TAG_BODY);
                response.push(0);
            }
            Err(err_msg) => {
                // Error
                response.push(TAG_ERR_MSG);
                let msg_bytes = err_msg.as_bytes();
                if msg_bytes.len() <= 255 {
                    response.push(msg_bytes.len() as u8);
                    response.extend_from_slice(msg_bytes);
                } else {
                    // Truncate long error messages
                    response.push(255);
                    response.extend_from_slice(&msg_bytes[..255]);
                }
            }
        }

        response
    }
}

// NOTE: Default impl commented out due to midge KvStore trait changes
// The trait requires ColumnFamilyHandle parameters that this mock doesn't provide
// Mock store implementations are currently not compatible with the midge trait
/*
impl Default for KvDomain {
    fn default() -> Self {
        // For tests - use a mock store
        use crate::storage::traits::KvTransaction;
        use cntryl_midge::{MidgeError, MidgeResult};
        use bytes::Bytes;

        struct MockStore;
        impl KvStore for MockStore {
            fn put(&self, _key: &[u8], _value: &[u8]) -> MidgeResult<()> {
                Ok(())
            }
            fn get(&self, _key: &[u8]) -> MidgeResult<Option<Bytes>> {
                Ok(None)
            }
            fn delete(&self, _key: &[u8]) -> MidgeResult<()> {
                Ok(())
            }
            fn put_batch(&self, _writes: Vec<(Vec<u8>, Vec<u8>)>) -> MidgeResult<()> {
                Ok(())
            }
            fn delete_batch(&self, _keys: Vec<Vec<u8>>) -> MidgeResult<()> {
                Ok(())
            }
            fn scan(&self, _start: &[u8], _end: &[u8]) -> MidgeResult<Vec<(Bytes, Bytes)>> {
                Ok(vec![])
            }
            fn flush(&self) -> MidgeResult<()> {
                Ok(())
            }
            fn begin_transaction(&self) -> MidgeResult<Box<dyn KvTransaction>> {
                Err(MidgeError::InvalidOperation("Transactions not supported in mock".to_string()))
            }
        }

        Self::new(Arc::new(MockStore))
    }
}
*/

impl Domain for KvDomain {
    fn handle(&self, request: DomainContext) -> DomainResponse {
        // Determine operation from route
        let operation = match KvOperation::from_route(&request.route) {
            Ok(op) => op,
            Err(err) => {
                return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    Self::build_tlv_response(Err(err)),
                ));
            }
        };

        // Parse TLV body for key and value
        let kv = Self::parse_tlv_body(&request.payload);
        let key = kv.key;
        let value = kv.value;

        // Parse realm and area from route
        let _realm = match request.route.realm.as_deref() {
            Some(r) => r,
            None => {
                return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    Self::build_tlv_response(Err("Missing realm in route".to_string())),
                ));
            }
        };
        let _area = match request.route.area.as_deref() {
            Some(a) => a,
            None => {
                return DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
                    Self::build_tlv_response(Err("Missing area in route".to_string())),
                ));
            }
        };

        // Use shared service instance (bench- and cache-friendly)
        let service = Arc::clone(&self.service);

        // Handle the operation
        let result = service
            .handle_operation(operation, &request.route_str, key, value);

        // Build and return response
        DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(
            Self::build_tlv_response(result),
        ))
    }
}
