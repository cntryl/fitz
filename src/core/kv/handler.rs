// KV domain handler - routes all kv:// operations
//
// This handler is the protocol adapter layer between the engine and KV service.
// Responsibilities:
// - Parse TLV payload to extract key (TAG_ID) and value (TAG_BODY)
// - Determine operation from route
// - Call appropriate service method
// - Build TLV response frames
//
// The handler has NO business logic - that lives in KvService.

use super::service::KvService;
use super::types::KvOperation;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::core::parsing::{response::ResponseBuilder, tlv};
use crate::protocol::tags::{TAG_BODY, TAG_ID};
use crate::storage::traits::KvStore;
use std::sync::Arc;

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
                return DomainResponse::Error(err);
            }
        };

        // Validate route structure
        if request.route.realm.is_none() {
            return DomainResponse::Error("Missing realm in route".to_string());
        }
        if request.route.area.is_none() {
            return DomainResponse::Error("Missing area in route".to_string());
        }

        // Parse TLV payload for key and value
        let key = tlv::parse_string_owned(&request.payload, TAG_ID);
        let value = tlv::parse_bytes_owned(&request.payload, TAG_BODY);

        // Call service to handle the operation
        let result = self
            .service
            .handle_operation(operation, &request.route_str, key, value);

        // Build response based on service result
        match result {
            Ok(Some(body)) => {
                // Success with body
                DomainResponse::Frame(
                    ResponseBuilder::new()
                        .add_bytes(TAG_BODY, &body)
                        .build_frame(),
                )
            }
            Ok(None) => {
                // Success with no body (e.g., PUT, DELETE)
                DomainResponse::Ok
            }
            Err(err) => {
                // Error from service
                DomainResponse::Error(err)
            }
        }
    }
}
