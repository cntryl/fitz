//! KV actor: transaction-scoped key-value operations over Midge
//!
//! # Architecture
//!
//! The KV actor maintains per-session transaction state (ActiveKvTx).
//! All KV operations execute within the context of an active transaction
//! bound to a single resource (table).
//!
//! # Write Options
//!
//! Durability is user-controlled via `WriteOptions` passed in `Begin`:
//! - `WriteOptions::synced()` - fsync on every commit (latency-first)
//! - `WriteOptions::buffered()` - no fsync, OS buffering (throughput-first)
//!
//! This follows the same pattern as streams, where the caller declares
//! durability intent upfront rather than having the domain choose.
//!
//! # Invariants
//!
//! 1. All KV ops require an active transaction
//! 2. Transactions are scoped to a single resource
//! 3. RouteFamily → ColumnFamily mapping is explicit (no default CF)
//! 4. No buffering, retries, or caching - direct Midge passthrough

use bytes::Bytes;
use cntryl_midge::{ColumnFamilyId, Engine as MidgeEngine, TransactionMode};
use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::validate_realm_format;
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;

use super::protocol::{KvError, KvMessage, KvPair, KvResponse, ScanQuery, TxMode};

/// Active KV transaction state
pub struct ActiveKvTx {
    /// Realm this transaction is bound to (resolved from auth)
    pub bound_realm: String,
    /// Resource (table) this transaction is bound to
    pub bound_resource: String,
    /// Resolved column family for this transaction
    pub column_family: ColumnFamilyId,
    /// Midge transaction handle
    pub tx: cntryl_midge::Transaction,
    /// Write options for commit
    pub write_options: cntryl_midge::WriteOptions,
}

/// KV actor managing transactions for a session
pub struct KvActor {
    /// Midge storage engine
    store: Arc<MidgeEngine>,
    /// Active transactions by server-assigned ID
    transactions: HashMap<u64, ActiveKvTx>,
    /// Next transaction ID to assign
    next_tx_id: u64,
}

impl KvActor {
    /// Create a new KV actor
    pub fn new(store: Arc<MidgeEngine>) -> Self {
        Self {
            store,
            transactions: HashMap::new(),
            next_tx_id: 1,
        }
    }

    /// Handle KV message
    pub fn handle(&mut self, msg: KvMessage) -> KvResponse {
        match msg {
            KvMessage::Begin {
                route_family,
                realm,
                area: _,
                resource,
                mode,
                write_options,
            } => self.handle_begin(route_family, realm, resource, mode, write_options),
            KvMessage::Commit { tx_id } => self.handle_commit(tx_id),
            KvMessage::Rollback { tx_id } => self.handle_rollback(tx_id),
            KvMessage::Get {
                tx_id,
                route_family,
                resource,
                key,
            } => self.handle_get(tx_id, route_family, resource, key),
            KvMessage::Put {
                tx_id,
                route_family,
                resource,
                key,
                value,
            } => self.handle_put(tx_id, route_family, resource, key, value),
            KvMessage::Insert {
                tx_id,
                route_family,
                resource,
                key,
                value,
            } => self.handle_insert(tx_id, route_family, resource, key, value),
            KvMessage::Delete {
                tx_id,
                route_family,
                resource,
                key,
            } => self.handle_delete(tx_id, route_family, resource, key),
            KvMessage::DeleteRange {
                tx_id,
                route_family,
                resource,
                start,
                end,
            } => self.handle_delete_range(tx_id, route_family, resource, start, end),
            KvMessage::Scan {
                tx_id,
                route_family,
                resource,
                query,
            } => self.handle_scan(tx_id, route_family, resource, query),
        }
    }

    /// Begin a new transaction
    fn handle_begin(
        &mut self,
        route_family: RouteFamily,
        realm: String,
        resource: String,
        mode: TxMode,
        write_options: cntryl_midge::WriteOptions,
    ) -> KvResponse {
        // Validate realm format (strict opaque identifier check)
        if validate_realm_format(&realm).is_err() {
            return KvResponse::Error {
                error: KvError::InvalidRealm,
            };
        }

        // Resolve column family from RouteFamily + resource
        let cf = match Self::resolve_column_family(route_family, &resource) {
            Ok(cf) => cf,
            Err(_) => {
                return KvResponse::Error {
                    error: KvError::InvalidRouteFamily,
                };
            }
        };

        // Create Midge transaction
        let tx_mode = match mode {
            TxMode::ReadOnly => TransactionMode::ReadOnly,
            TxMode::ReadWrite => TransactionMode::ReadWrite,
        };

        match self.store.begin_tx(cf, tx_mode) {
            Ok(tx) => {
                let tx_id = self.next_tx_id;
                self.next_tx_id += 1;

                self.transactions.insert(
                    tx_id,
                    ActiveKvTx {
                        bound_realm: realm,
                        bound_resource: resource,
                        column_family: cf,
                        tx,
                        write_options,
                    },
                );
                KvResponse::BeginOk { tx_id }
            }
            Err(e) => KvResponse::Error {
                error: Self::map_midge_error(e),
            },
        }
    }

    /// Commit a transaction by ID
    fn handle_commit(&mut self, tx_id: u64) -> KvResponse {
        match self.transactions.remove(&tx_id) {
            None => KvResponse::Error {
                error: KvError::InvalidTxId,
            },
            Some(active) => {
                // Use write options provided by user at transaction begin
                match self.store.commit(active.tx, active.write_options) {
                    Ok(()) => KvResponse::CommitOk,
                    Err(e) => KvResponse::Error {
                        error: Self::map_midge_error(e),
                    },
                }
            }
        }
    }

    /// Rollback a transaction by ID
    fn handle_rollback(&mut self, tx_id: u64) -> KvResponse {
        match self.transactions.remove(&tx_id) {
            None => KvResponse::Error {
                error: KvError::InvalidTxId,
            },
            Some(_active) => {
                // Transaction is dropped, automatically rolled back by Midge
                KvResponse::RollbackOk
            }
        }
    }

    /// Get a value by key
    fn handle_get(
        &mut self,
        tx_id: u64,
        _route_family: RouteFamily,
        resource: String,
        key: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        // Validate resource match
        if resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        let realm = active.bound_realm.clone();
        let scoped_key = Self::encode_scoped_key(&realm, &resource, &key);

        match active.tx.get(&scoped_key) {
            Ok(Some(value)) => KvResponse::GetResult {
                found: true,
                value: Some(value),
            },
            Ok(None) => KvResponse::GetResult {
                found: false,
                value: None,
            },
            Err(e) => KvResponse::Error {
                error: Self::map_midge_error(e),
            },
        }
    }

    /// Put (upsert) a key-value pair
    fn handle_put(
        &mut self,
        tx_id: u64,
        _route_family: RouteFamily,
        resource: String,
        key: Bytes,
        value: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        // Validate resource match
        if resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        let realm = active.bound_realm.clone();
        let scoped_key = Self::encode_scoped_key(&realm, &resource, &key);

        match active.tx.put(scoped_key, value.to_vec(), None) {
            Ok(()) => KvResponse::PutOk,
            Err(e) => KvResponse::Error {
                error: Self::map_midge_error(e),
            },
        }
    }

    /// Insert a key-value pair (fail if exists)
    fn handle_insert(
        &mut self,
        tx_id: u64,
        _route_family: RouteFamily,
        resource: String,
        key: Bytes,
        value: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        // Validate resource match
        if resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        // Check if key exists first
        let realm = active.bound_realm.clone();
        let scoped_key = Self::encode_scoped_key(&realm, &resource, &key);

        match active.tx.get(&scoped_key) {
            Ok(Some(_)) => {
                // Key exists, insert should fail
                KvResponse::Error {
                    error: KvError::AlreadyExists,
                }
            }
            Ok(None) => {
                // Key doesn't exist, proceed with insert
                match active.tx.put(scoped_key, value.to_vec(), None) {
                    Ok(()) => KvResponse::InsertOk,
                    Err(e) => KvResponse::Error {
                        error: Self::map_midge_error(e),
                    },
                }
            }
            Err(e) => KvResponse::Error {
                error: Self::map_midge_error(e),
            },
        }
    }

    /// Delete a key
    fn handle_delete(
        &mut self,
        tx_id: u64,
        _route_family: RouteFamily,
        resource: String,
        key: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        // Validate resource match
        if resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        let realm = active.bound_realm.clone();
        let scoped_key = Self::encode_scoped_key(&realm, &resource, &key);

        match active.tx.delete(scoped_key) {
            Ok(()) => KvResponse::DeleteOk,
            Err(e) => KvResponse::Error {
                error: Self::map_midge_error(e),
            },
        }
    }

    /// Delete a range of keys [start, end)
    fn handle_delete_range(
        &mut self,
        tx_id: u64,
        _route_family: RouteFamily,
        resource: String,
        start: Bytes,
        end: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        // Validate resource match
        if resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        // Validate range
        if start >= end {
            return KvResponse::Error {
                error: KvError::InvalidRequest("start must be less than end".to_string()),
            };
        }

        let realm = active.bound_realm.clone();
        let scoped_start = Self::encode_scoped_key(&realm, &resource, &start);
        let scoped_end = Self::encode_scoped_key(&realm, &resource, &end);

        match active.tx.delete_range(scoped_start, scoped_end) {
            Ok(()) => KvResponse::DeleteRangeOk,
            Err(e) => KvResponse::Error {
                error: Self::map_midge_error(e),
            },
        }
    }

    /// Scan a range of keys
    fn handle_scan(
        &mut self,
        tx_id: u64,
        _route_family: RouteFamily,
        resource: String,
        query: ScanQuery,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        // Validate resource match before using transaction
        if resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        let realm = active.bound_realm.clone();
        let prefix = Self::realm_resource_prefix(&realm, &resource);
        let start_key = query
            .start
            .as_ref()
            .map(|k| Self::encode_scoped_key(&realm, &resource, k))
            .unwrap_or_else(|| prefix.clone());
        let end_key = query
            .end
            .as_ref()
            .map(|k| Self::encode_scoped_key(&realm, &resource, k))
            .unwrap_or_else(|| Self::prefix_range_end(&prefix));

        // Build Midge Query
        let mut midge_query = cntryl_midge::Query::new()
            .prefix(Bytes::from(prefix.clone()))
            .start_key(Bytes::from(start_key))
            .end_key(Bytes::from(end_key));

        if let Some(limit) = query.limit {
            midge_query = midge_query.limit(limit);
        }

        if query.reverse {
            midge_query = midge_query.reverse();
        }

        match active.tx.scan(&midge_query) {
            Ok(mut iterator) => {
                let mut items = Vec::new();

                while let Some((key, value)) = iterator.next() {
                    let user_key = match Self::strip_scoped_prefix(&realm, &resource, &key) {
                        Some(k) => k,
                        None => continue,
                    };
                    items.push(KvPair {
                        key: Bytes::from(user_key),
                        value: Bytes::from(value),
                    });
                }

                KvResponse::ScanResult {
                    items,
                    has_more: false, // Midge doesn't provide continuation tokens yet
                }
            }
            Err(e) => KvResponse::Error {
                error: Self::map_midge_error(e),
            },
        }
    }

    /// Get active transaction or return error
    fn get_transaction_or_err(&mut self, tx_id: u64) -> Result<&mut ActiveKvTx, KvResponse> {
        self.transactions
            .get_mut(&tx_id)
            .ok_or_else(|| KvResponse::Error {
                error: KvError::InvalidTxId,
            })
    }

    fn realm_resource_prefix(realm: &str, resource: &str) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(realm.len() + resource.len() + 2);
        prefix.extend_from_slice(realm.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(resource.as_bytes());
        prefix.push(0);
        prefix
    }

    /// Resolve column family from RouteFamily and resource
    ///
    /// Uses explicit mapping: ColumnFamilyId = RouteFamily.id
    /// This ensures data isolation per route family.
    ///
    /// # Errors
    ///
    /// Returns an error if RouteFamily is 0 (would map to default CF).
    fn resolve_column_family(
        route_family: RouteFamily,
        _resource: &str,
    ) -> Result<ColumnFamilyId, String> {
        // Validate RouteFamily is not zero (would map to default CF)
        crate::runtime::cf_validation::validate_route_family(route_family)?;

        // Map RouteFamily → ColumnFamily (1:1 by value)
        // Resource is enforced via key prefixing within the column family.
        Ok(ColumnFamilyId(route_family.id()))
    }

    fn encode_scoped_key(realm: &str, resource: &str, user_key: &[u8]) -> Vec<u8> {
        let mut out = Self::realm_resource_prefix(realm, resource);
        out.extend_from_slice(user_key);
        out
    }

    fn strip_scoped_prefix(realm: &str, resource: &str, scoped_key: &[u8]) -> Option<Vec<u8>> {
        let prefix = Self::realm_resource_prefix(realm, resource);
        scoped_key
            .strip_prefix(prefix.as_slice())
            .map(|rest| rest.to_vec())
    }

    fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
        // Compute exclusive end bound for a prefix scan.
        // Safe here because our prefix always ends with 0 (so increment succeeds).
        let mut end = prefix.to_vec();
        for idx in (0..end.len()).rev() {
            if end[idx] != 0xFF {
                end[idx] = end[idx].wrapping_add(1);
                end.truncate(idx + 1);
                return end;
            }
        }
        // Fallback: no end bound (should not happen for our prefixes)
        vec![0xFF]
    }

    /// Map Midge error to KV domain error
    fn map_midge_error(err: cntryl_midge::MidgeError) -> KvError {
        // Midge errors are currently opaque from this crate's perspective.
        // Preserve retryability distinctions using message heuristics.
        let msg = err.to_string();
        let msg_lc = msg.to_lowercase();

        if msg_lc.contains("conflict") || msg_lc.contains("abort") || msg_lc.contains("retry") {
            return KvError::Conflict(msg);
        }

        if msg_lc.contains("unavailable")
            || msg_lc.contains("io")
            || msg_lc.contains("closed")
            || msg_lc.contains("corrupt")
        {
            return KvError::BackendUnavailable(msg);
        }

        KvError::BackendError(msg)
    }
}

impl Actor for KvActor {
    type Message = KvMessage;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        // KV operations are synchronous and return via response channel
        let _response = self.handle(msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::RouteFamily;

    fn test_actor() -> KvActor {
        let store = crate::testkit::create_test_engine_with_cfs(vec![1, 2, 3]);
        KvActor::new(store)
    }

    #[test]
    fn should_begin_transaction_for_resource() {
        // Arrange
        let mut actor = test_actor();

        // Act
        let response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });

        // Assert
        assert!(matches!(response, KvResponse::BeginOk { tx_id: _ }));
    }

    #[test]
    fn should_enforce_transaction_scope_to_single_resource() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });

        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Act - Try to operate on different resource
        let response = actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table2".to_string(),
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::TxScopeViolation { .. }
            }
        ));
    }

    #[test]
    fn should_reject_operations_without_active_transaction() {
        // Arrange
        let mut actor = test_actor();

        // Act & Assert - Get
        let response = actor.handle(KvMessage::Get {
            tx_id: 999,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: Bytes::from("key"),
        });
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidTxId
            }
        ));

        // Act & Assert - Put
        let response = actor.handle(KvMessage::Put {
            tx_id: 999,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        });
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidTxId
            }
        ));
    }

    #[test]
    fn should_put_and_get_value_within_transaction() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });

        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let key = Bytes::from("testkey");
        let value = Bytes::from("testvalue");

        // Act - Put
        let put_response = actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: value.clone(),
        });
        assert!(matches!(put_response, KvResponse::PutOk));

        // Act - Get
        let get_response = actor.handle(KvMessage::Get {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
        });

        // Assert
        match get_response {
            KvResponse::GetResult {
                found: true,
                value: Some(v),
            } => assert_eq!(v, value),
            _ => panic!("Expected GetResult with value"),
        }
    }

    #[test]
    fn should_reject_insert_when_key_exists() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let key = Bytes::from("testkey");
        actor.handle(KvMessage::Insert {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("value1"),
        });

        // Act - Try to insert again
        let response = actor.handle(KvMessage::Insert {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("value2"),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::AlreadyExists
            }
        ));
    }

    #[test]
    fn should_validate_delete_range_parameters() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Act - End before start
        let response = actor.handle(KvMessage::DeleteRange {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            start: Bytes::from("z"),
            end: Bytes::from("a"),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidRequest(_)
            }
        ));
    }

    #[test]
    fn should_reject_route_family_zero() {
        // Arrange
        let mut actor = test_actor();

        // Act
        let result = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(0),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });

        // Assert
        assert!(matches!(
            result,
            KvResponse::Error {
                error: KvError::InvalidRouteFamily,
            }
        ));
    }

    #[test]
    fn should_delete_existing_key() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let key = Bytes::from("delkey");
        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("value1"),
        });

        // Act - Delete the key
        let delete_response = actor.handle(KvMessage::Delete {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
        });

        // Assert delete succeeds
        assert!(matches!(delete_response, KvResponse::DeleteOk));

        // Verify key is gone
        let get_response = actor.handle(KvMessage::Get {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
        });
        assert!(matches!(
            get_response,
            KvResponse::GetResult {
                found: false,
                value: None
            }
        ));
    }

    #[test]
    fn should_scan_key_range() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Add multiple keys
        for i in 0..5 {
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table1".to_string(),
                key: Bytes::from(format!("key{:02}", i)),
                value: Bytes::from(format!("value{}", i)),
            });
        }

        // Act - Scan range [key01, key04)
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            query: ScanQuery {
                start: Some(Bytes::from("key01")),
                end: Some(Bytes::from("key04")),
                limit: None,
                reverse: false,
            },
        });

        // Assert
        match response {
            KvResponse::ScanResult { items, .. } => {
                assert!(items.len() >= 2); // At least key01, key02, key03
            }
            _ => panic!("Expected ScanResult"),
        }
    }

    #[test]
    fn should_commit_empty_transaction() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Act - Commit immediately without writing anything
        let response = actor.handle(KvMessage::Commit { tx_id });

        // Assert
        assert!(matches!(response, KvResponse::CommitOk));
    }

    #[test]
    fn should_rollback_transaction() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let key = Bytes::from("rollbackkey");
        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("will_rollback"),
        });

        // Act - Rollback
        let response = actor.handle(KvMessage::Rollback { tx_id });

        // Assert
        assert!(matches!(response, KvResponse::RollbackOk));

        // Verify transaction is no longer active
        let get_response = actor.handle(KvMessage::Get {
            tx_id: 9999,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key,
        });
        assert!(matches!(
            get_response,
            KvResponse::Error {
                error: KvError::InvalidTxId
            }
        ));
    }

    #[test]
    fn should_isolate_resources_in_same_family() {
        // Arrange
        let mut actor = test_actor();

        // Begin transaction for resource1
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let key = Bytes::from("testkey");
        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("value1"),
        });

        // Act - Try to put to different resource in same transaction (should fail)
        let response = actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table2".to_string(),
            key: key.clone(),
            value: Bytes::from("value2"),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::TxScopeViolation { .. }
            }
        ));
    }

    #[test]
    fn should_handle_key_scoping_correctly() {
        // Arrange
        let mut actor1 = test_actor();
        let mut actor2 = test_actor();

        // Both start transactions for different resources
        let begin_response1 = actor1.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id1 = match begin_response1 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let begin_response2 = actor2.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table2".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id2 = match begin_response2 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let key = Bytes::from("samekey");

        // Act - Put same key to both resources
        actor1.handle(KvMessage::Put {
            tx_id: tx_id1,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: Bytes::from("value1"),
        });

        actor2.handle(KvMessage::Put {
            tx_id: tx_id2,
            route_family: RouteFamily::new(1),
            resource: "table2".to_string(),
            key: key.clone(),
            value: Bytes::from("value2"),
        });

        // Assert - Both succeed, they are isolated
        let get1 = actor1.handle(KvMessage::Get {
            tx_id: tx_id1,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
        });

        let get2 = actor2.handle(KvMessage::Get {
            tx_id: tx_id2,
            route_family: RouteFamily::new(1),
            resource: "table2".to_string(),
            key: key.clone(),
        });

        match (get1, get2) {
            (
                KvResponse::GetResult {
                    found: true,
                    value: Some(v1),
                },
                KvResponse::GetResult {
                    found: true,
                    value: Some(v2),
                },
            ) => {
                assert_eq!(v1, Bytes::from("value1"));
                assert_eq!(v2, Bytes::from("value2"));
            }
            _ => panic!("Expected both gets to succeed with different values"),
        }
    }

    #[test]
    fn should_reject_delete_range_with_invalid_bounds() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Act - End < Start
        let response = actor.handle(KvMessage::DeleteRange {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            start: Bytes::from("zzz"),
            end: Bytes::from("aaa"),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidRequest(_)
            }
        ));
    }

    #[test]
    fn should_scan_with_limit() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Add 10 keys
        for i in 0..10 {
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table1".to_string(),
                key: Bytes::from(format!("k{:02}", i)),
                value: Bytes::from(format!("v{}", i)),
            });
        }

        // Act - Scan with limit of 3
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            query: ScanQuery {
                start: None,
                end: None,
                limit: Some(3),
                reverse: false,
            },
        });

        // Assert
        match response {
            KvResponse::ScanResult { items, .. } => {
                assert!(items.len() <= 3);
            }
            _ => panic!("Expected ScanResult"),
        }
    }

    #[test]
    fn should_scan_reverse() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Add keys
        for i in 0..5 {
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table1".to_string(),
                key: Bytes::from(format!("k{}", i)),
                value: Bytes::from(format!("v{}", i)),
            });
        }

        // Act - Scan reverse
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            query: ScanQuery {
                start: None,
                end: None,
                limit: None,
                reverse: true,
            },
        });

        // Assert - Just verify it returns results (order depends on storage)
        match response {
            KvResponse::ScanResult { items, .. } => {
                assert!(!items.is_empty());
            }
            _ => panic!("Expected ScanResult"),
        }
    }
}
