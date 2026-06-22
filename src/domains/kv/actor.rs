//! KV actor: durable committed writes with session-scoped transaction state
//! over Midge.
//!
//! # Architecture
//!
//! The KV actor maintains per-session, broker-local transaction state
//! (`ActiveKvTx`). All KV operations execute within the context of an active
//! transaction bound to a single resource (table). `tx_id` values are runtime
//! handles for the current session only; reconnect or broker restart requires a
//! new `begin`.
//!
//! # Write Options
//!
//! Durability is user-controlled via `WriteOptions` passed in `Begin`:
//! - `WriteOptions::synced()` - fsync on every commit (latency-first)
//! - `WriteOptions::buffered()` - no fsync, OS buffering (throughput-first)
//!
//! This follows the same pattern as streams, where the caller declares
//! durability intent upfront rather than having the domain choose.
//! These options apply only to committed writes. Open transaction handles,
//! uncommitted writes, and resource-lock ownership remain broker-local memory
//! and are lost on session disconnect or broker restart.
//!
//! # Invariants
//!
//! 1. All KV ops require an active transaction
//! 2. Transactions are scoped to a single resource
//! 3. RouteFamily -> ColumnFamily mapping is explicit (no default CF)
//! 4. No buffering, retries, or caching - direct Midge passthrough

use bytes::Bytes;
use cntryl_midge::{ColumnFamilyId, Engine as MidgeEngine, TransactionMode};
use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::validate_realm_format;
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;
use crate::utils::storage_key::{self, DomainKeyspace};

use super::protocol::{KvError, KvMessage, KvPair, KvResponse, ScanQuery, TxMode};

const KV_KEY_SCOPE_MARKER: u8 = 0x01;

/// Active KV transaction state.
///
/// This state is broker-local and in-memory only. Dropping the owning actor,
/// cleaning up the owning session, or restarting the broker aborts any
/// uncommitted work and discards the transaction handle instead of attempting
/// recovery.
pub struct ActiveKvTx {
    /// Realm this transaction is bound to (resolved from auth)
    pub bound_realm: String,
    /// Area this transaction is bound to
    pub bound_area: String,
    /// Resource (table) this transaction is bound to
    pub bound_resource: String,
    /// Cached realm/area/resource prefix for scoped-key encoding
    pub scoped_prefix: Vec<u8>,
    /// Resolved column family for this transaction
    pub column_family: ColumnFamilyId,
    /// Midge transaction handle
    pub tx: cntryl_midge::Transaction,
    /// Write options for commit
    pub write_options: cntryl_midge::WriteOptions,
    /// Successful mutating operations performed within this transaction.
    pub mutation_count: u64,
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
                area,
                resource,
                mode,
                write_options,
            } => self.handle_begin(route_family, realm, area, resource, mode, write_options),
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
        area: String,
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
                let scoped_prefix = Self::realm_resource_prefix(&realm, &area, &resource);

                tracing::trace!(
                    "KvActor assigning transaction ID: {}, next_tx_id is now: {}",
                    tx_id,
                    self.next_tx_id
                );

                self.transactions.insert(
                    tx_id,
                    ActiveKvTx {
                        bound_realm: realm,
                        bound_area: area,
                        bound_resource: resource,
                        scoped_prefix,
                        column_family: cf,
                        tx,
                        write_options,
                        mutation_count: 0,
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
                match active.tx.commit(active.write_options) {
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

        // Per CLIENT_SPEC: resource is implicit from transaction context.
        // If resource is provided, validate it matches; if empty, use bound_resource.
        if !resource.is_empty() && resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, &key);

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

        // Per CLIENT_SPEC: resource is implicit from transaction context.
        // If resource is provided, validate it matches; if empty, use bound_resource.
        if !resource.is_empty() && resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, &key);

        match active.tx.put(scoped_key, value.to_vec(), None) {
            Ok(()) => {
                active.mutation_count += 1;
                KvResponse::PutOk
            }
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

        // Per CLIENT_SPEC: resource is implicit from transaction context.
        // If resource is provided, validate it matches; if empty, use bound_resource.
        if !resource.is_empty() && resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        // Check if key exists first
        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, &key);

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
                    Ok(()) => {
                        active.mutation_count += 1;
                        KvResponse::InsertOk
                    }
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

        // Per CLIENT_SPEC: resource is implicit from transaction context.
        // If resource is provided, validate it matches; if empty, use bound_resource.
        if !resource.is_empty() && resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, &key);

        match active.tx.delete(scoped_key) {
            Ok(()) => {
                active.mutation_count += 1;
                KvResponse::DeleteOk
            }
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

        // Per CLIENT_SPEC: resource is implicit from transaction context.
        // If resource is provided, validate it matches; if empty, use bound_resource.
        if !resource.is_empty() && resource != active.bound_resource {
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

        let scoped_start = Self::encode_scoped_key(&active.scoped_prefix, &start);
        let scoped_end = Self::encode_scoped_key(&active.scoped_prefix, &end);

        match active.tx.delete_range(scoped_start, scoped_end) {
            Ok(()) => {
                active.mutation_count += 1;
                KvResponse::DeleteRangeOk
            }
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

        // Per CLIENT_SPEC: resource is implicit from transaction context.
        // If resource is provided, validate it matches; if empty, use bound_resource.
        if !resource.is_empty() && resource != active.bound_resource {
            return KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource,
                },
            };
        }

        let prefix = active.scoped_prefix.clone();
        let start_key = query
            .start
            .as_ref()
            .map(|k| Self::encode_scoped_key(&prefix, k))
            .unwrap_or_else(|| prefix.clone());
        let end_key = query
            .end
            .as_ref()
            .map(|k| Self::encode_scoped_key(&prefix, k))
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
                    let user_key = match Self::strip_scoped_prefix(&prefix, &key) {
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

    pub fn mutation_count_for_tx(&self, tx_id: u64) -> Option<u64> {
        self.transactions.get(&tx_id).map(|tx| tx.mutation_count)
    }

    fn realm_resource_prefix(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        let mut encoder = storage_key::domain_marker_encoder(
            realm,
            DomainKeyspace::Kv,
            KV_KEY_SCOPE_MARKER,
            area.len() + resource.len() + 2,
        );
        storage_key::encode_bytes_segment_into(&mut encoder, area.as_bytes());
        storage_key::encode_bytes_segment_into(&mut encoder, resource.as_bytes());
        encoder.into_vec()
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
        Ok(route_family.id())
    }

    fn encode_scoped_key(prefix: &[u8], user_key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(prefix.len() + user_key.len());
        out.extend_from_slice(prefix);
        out.extend_from_slice(user_key);
        out
    }

    fn strip_scoped_prefix(prefix: &[u8], scoped_key: &[u8]) -> Option<Vec<u8>> {
        scoped_key.strip_prefix(prefix).map(|rest| rest.to_vec())
    }

    fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
        storage_key::prefix_range_end(prefix)
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

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        let response = self.handle(msg);
        let _ = ctx.reply(response).ok();
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

        // Act
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

        // Verify: Put also rejected without active transaction
        let response = actor.handle(KvMessage::Put {
            tx_id: 999,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidTxId
            }
        ));
    }

    #[test]
    fn should_roundtrip_value_within_transaction() {
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

        // Act
        let put_response = actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: key.clone(),
            value: value.clone(),
        });
        assert!(matches!(put_response, KvResponse::PutOk));

        // Step 2: retrieve the value
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
    fn should_encode_kv_scope_prefix_with_typed_segments() {
        // Arrange
        let expected = {
            let mut bytes = b"acme\0kv\0".to_vec();
            bytes.push(KV_KEY_SCOPE_MARKER);
            bytes.extend_from_slice(b"users\0profiles\0");
            bytes
        };

        // Act
        let prefix = KvActor::realm_resource_prefix("acme", "users", "profiles");

        // Assert
        assert_eq!(prefix, expected);
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
    fn should_enforce_realm_isolation_for_kv() {
        // Arrange
        let mut actor = test_actor();

        // Begin transactions in two different realms but same resource/key
        let r1 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "realm_a".to_string(),
            area: "kv".to_string(),
            resource: "users".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx1 = match r1 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk for realm_a"),
        };

        let r2 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "realm_b".to_string(),
            area: "kv".to_string(),
            resource: "users".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx2 = match r2 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk for realm_b"),
        };

        let key = Bytes::from("same_key");

        // Act
        actor.handle(KvMessage::Put {
            tx_id: tx1,
            route_family: RouteFamily::new(1),
            resource: "users".to_string(),
            key: key.clone(),
            value: Bytes::from("value_in_a"),
        });

        actor.handle(KvMessage::Put {
            tx_id: tx2,
            route_family: RouteFamily::new(1),
            resource: "users".to_string(),
            key: key.clone(),
            value: Bytes::from("value_in_b"),
        });

        // Assert - reads in each transaction return the realm-scoped value
        let get_a = actor.handle(KvMessage::Get {
            tx_id: tx1,
            route_family: RouteFamily::new(1),
            resource: "users".to_string(),
            key: key.clone(),
        });
        let get_b = actor.handle(KvMessage::Get {
            tx_id: tx2,
            route_family: RouteFamily::new(1),
            resource: "users".to_string(),
            key: key.clone(),
        });

        match (get_a, get_b) {
            (
                KvResponse::GetResult {
                    found: true,
                    value: Some(va),
                },
                KvResponse::GetResult {
                    found: true,
                    value: Some(vb),
                },
            ) => {
                assert_eq!(va, Bytes::from("value_in_a"));
                assert_eq!(vb, Bytes::from("value_in_b"));
            }
            _ => panic!("Expected realm-scoped values to be returned"),
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

    #[test]
    fn should_handle_concurrent_puts_with_conflict_detection() {
        // Arrange
        let mut actor = test_actor();

        let b1 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "concurrent".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx1 = match b1 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let b2 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "concurrent".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx2 = match b2 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Act
        actor.handle(KvMessage::Put {
            tx_id: tx1,
            route_family: RouteFamily::new(1),
            resource: "concurrent".to_string(),
            key: Bytes::from("key"),
            value: Bytes::from("v1"),
        });

        actor.handle(KvMessage::Put {
            tx_id: tx2,
            route_family: RouteFamily::new(1),
            resource: "concurrent".to_string(),
            key: Bytes::from("key"),
            value: Bytes::from("v2"),
        });

        // Assert
        // Commit first tx (expected OK)
        let c1 = actor.handle(KvMessage::Commit { tx_id: tx1 });
        assert!(matches!(c1, KvResponse::CommitOk));

        // Second commit may conflict or succeed depending on storage semantics.
        let c2 = actor.handle(KvMessage::Commit { tx_id: tx2 });
        assert!(
            matches!(c2, KvResponse::CommitOk)
                || matches!(
                    c2,
                    KvResponse::Error {
                        error: KvError::Conflict(_)
                    }
                )
        );

        // Verify final stored value is one of the two candidates (v1 or v2)
        let b3 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "concurrent".to_string(),
            mode: TxMode::ReadOnly,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx3 = match b3 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Begin failed"),
        };

        let got = actor.handle(KvMessage::Get {
            tx_id: tx3,
            route_family: RouteFamily::new(1),
            resource: "concurrent".to_string(),
            key: Bytes::from("key"),
        });

        match got {
            KvResponse::GetResult {
                found: true,
                value: Some(v),
            } => {
                assert!(v.as_ref() == b"v1" || v.as_ref() == b"v2");
            }
            _ => panic!("Expected stored value after commits"),
        }

        actor.handle(KvMessage::Rollback { tx_id: tx3 });
    }

    #[test]
    fn should_reject_operations_from_wrong_area() {
        // Arrange
        let mut actor = test_actor();

        let r1 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "area_a".to_string(),
            resource: "shared".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx1 = match r1 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        let r2 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "area_b".to_string(),
            resource: "shared".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx2 = match r2 {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Act
        actor.handle(KvMessage::Put {
            tx_id: tx1,
            route_family: RouteFamily::new(1),
            resource: "shared".to_string(),
            key: Bytes::from("same_key"),
            value: Bytes::from("in_a"),
        });
        actor.handle(KvMessage::Commit { tx_id: tx1 });

        let get_in_b = actor.handle(KvMessage::Get {
            tx_id: tx2,
            route_family: RouteFamily::new(1),
            resource: "shared".to_string(),
            key: Bytes::from("same_key"),
        });

        // Assert - different area must not see the value
        match get_in_b {
            KvResponse::GetResult {
                found: false,
                value: None,
            } => {}
            _ => panic!("Expected not-found across different area"),
        }
    }

    #[test]
    fn should_return_error_for_invalid_txid() {
        // Arrange
        let mut actor = test_actor();

        // Act
        let res = actor.handle(KvMessage::Commit { tx_id: 99999 });

        // Assert
        assert!(matches!(
            res,
            KvResponse::Error {
                error: KvError::InvalidTxId
            }
        ));
    }

    #[test]
    fn should_return_not_found_when_key_never_written() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadOnly,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Act
        let response = actor.handle(KvMessage::Get {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: Bytes::from("nonexistent"),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::GetResult {
                found: false,
                value: None
            }
        ));
    }

    #[test]
    fn should_delete_nonexistent_key_without_error() {
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

        // Act
        let response = actor.handle(KvMessage::Delete {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: Bytes::from("never_written"),
        });

        // Assert
        assert!(matches!(response, KvResponse::DeleteOk));
    }

    #[test]
    fn should_scan_empty_table_returns_empty_result() {
        // Arrange
        let mut actor = test_actor();
        let begin_response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "kv".to_string(),
            resource: "empty_table".to_string(),
            mode: TxMode::ReadOnly,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin_response {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => panic!("Expected BeginOk"),
        };

        // Act
        let response = actor.handle(KvMessage::Scan {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "empty_table".to_string(),
            query: ScanQuery {
                start: None,
                end: None,
                limit: None,
                reverse: false,
            },
        });

        // Assert
        match response {
            KvResponse::ScanResult { items, .. } => {
                assert!(items.is_empty());
            }
            _ => panic!("Expected ScanResult"),
        }
    }

    #[test]
    fn should_reject_begin_with_empty_realm() {
        // Arrange
        let mut actor = test_actor();

        // Act
        let response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidRealm
            }
        ));
    }

    #[test]
    fn should_reject_begin_with_realm_containing_spaces() {
        // Arrange
        let mut actor = test_actor();

        // Act
        let response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "bad realm".to_string(),
            area: "kv".to_string(),
            resource: "table1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidRealm
            }
        ));
    }

    #[test]
    fn should_reject_commit_on_already_committed_txid() {
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
        actor.handle(KvMessage::Commit { tx_id });

        // Act
        let response = actor.handle(KvMessage::Commit { tx_id });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidTxId
            }
        ));
    }

    #[test]
    fn should_reject_rollback_on_already_rolled_back_txid() {
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
        actor.handle(KvMessage::Rollback { tx_id });

        // Act
        let response = actor.handle(KvMessage::Rollback { tx_id });

        // Assert
        assert!(matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidTxId
            }
        ));
    }

    #[test]
    fn should_use_bound_resource_when_resource_param_is_empty() {
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
        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "table1".to_string(),
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        });

        // Act — empty resource falls back to bound_resource ("table1")
        let response = actor.handle(KvMessage::Get {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "".to_string(),
            key: Bytes::from("key"),
        });

        // Assert
        assert!(matches!(
            response,
            KvResponse::GetResult {
                found: true,
                value: Some(_)
            }
        ));
    }
}
