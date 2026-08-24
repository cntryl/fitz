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
//! Clients select a durability class via `WriteOptions` passed in `Begin`:
//! - `WriteOptions::synced()` - fsync on every commit (latency-first)
//! - `WriteOptions::buffered()` - no fsync, OS buffering (throughput-first)
//!
//! The broker may map canonical sync or buffered requests to an
//! operator-configured policy within the requested class (for example, cloud
//! strict or cloud asynchronous writes). The client controls the class, while
//! deployment configuration controls the concrete policy within that class.
//! These options apply only to committed writes. Open transaction handles,
//! uncommitted writes, and resource-lock ownership remain broker-local memory
//! and are lost on session disconnect or broker restart.
//!
//! # Invariants
//!
//! 1. All KV ops require an active transaction
//! 2. Transactions are scoped to a single resource
//! 3. `RouteFamily` -> `ColumnFamily` mapping is explicit (no default CF)
//! 4. No buffering, retries, or caching - direct Midge passthrough

use bytes::Bytes;
use cntryl_midge::{ColumnFamilyId, Engine as MidgeEngine, TransactionMode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::auth::validate_realm_format;
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::utils::storage_key::{self, DomainKeyspace};

use super::protocol::{KvError, KvMessage, KvPair, KvResourceScope, KvResponse, ScanQuery, TxMode};

mod errors;
mod inventory;
mod keys;

use inventory::KvInventoryDelta;

const KV_KEY_SCOPE_MARKER: u8 = 0x01;
const KV_INVENTORY_SCOPE_MARKER: u8 = 0x02;
const KV_INVENTORY_VALUE_VERSION: u8 = 1;
const KV_INVENTORY_VALUE_LEN: usize = 18;
const KV_INVENTORY_RECORD_COUNT_RANGE: std::ops::Range<usize> = 2..10;
const KV_INVENTORY_STORAGE_BYTES_RANGE: std::ops::Range<usize> = 10..18;
const MAX_SCAN_ITEMS: usize = 1_024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct KvInventoryEstimate {
    pub estimated_record_count: u64,
    pub estimated_storage_bytes: u64,
    pub estimate_complete: bool,
}

impl Default for KvInventoryEstimate {
    fn default() -> Self {
        Self {
            estimated_record_count: 0,
            estimated_storage_bytes: 0,
            estimate_complete: true,
        }
    }
}

/// Active KV transaction state.
///
/// This state is broker-local and in-memory only. Dropping the owning actor,
/// cleaning up the owning session, or restarting the broker aborts any
/// uncommitted work and discards the transaction handle instead of attempting
/// recovery. The broker also force-rolls back transactions that remain idle
/// beyond its configured transaction TTL so abandoned writers cannot retain a
/// resource lock indefinitely.
pub struct ActiveKvTx {
    /// Complete route scope this transaction is bound to.
    pub scope: KvResourceScope,
    /// Client-declared transaction access mode.
    pub mode: TxMode,
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
    last_activity: Instant,
    inventory_delta: KvInventoryDelta,
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
                scope,
                mode,
                write_options,
            } => self.handle_begin(scope, mode, write_options),
            KvMessage::Commit { tx_id, scope } => self.handle_commit(tx_id, &scope),
            KvMessage::Rollback { tx_id, scope } => self.handle_rollback(tx_id, &scope),
            KvMessage::Get { tx_id, scope, key } => self.handle_get(tx_id, &scope, &key),
            KvMessage::Put {
                tx_id,
                scope,
                key,
                value,
            } => self.handle_put(tx_id, &scope, &key, &value),
            KvMessage::Insert {
                tx_id,
                scope,
                key,
                value,
            } => self.handle_insert(tx_id, &scope, &key, &value),
            KvMessage::Delete { tx_id, scope, key } => self.handle_delete(tx_id, &scope, &key),
            KvMessage::DeleteRange {
                tx_id,
                scope,
                start,
                end,
            } => self.handle_delete_range(tx_id, &scope, &start, &end),
            KvMessage::Scan {
                tx_id,
                scope,
                query,
            } => self.handle_scan(tx_id, &scope, &query),
        }
    }

    /// Begin a new transaction
    fn handle_begin(
        &mut self,
        scope: KvResourceScope,
        mode: TxMode,
        write_options: cntryl_midge::WriteOptions,
    ) -> KvResponse {
        // Validate realm format (strict opaque identifier check)
        if validate_realm_format(&scope.realm).is_err() {
            return KvResponse::Error {
                error: KvError::InvalidRealm,
            };
        }

        // Resolve column family from RouteFamily + resource
        let Ok(cf) = Self::resolve_column_family(scope.route_family, &scope.resource) else {
            return KvResponse::Error {
                error: KvError::InvalidRouteFamily,
            };
        };

        // Create Midge transaction
        let tx_mode = match mode {
            TxMode::ReadOnly => TransactionMode::ReadOnly,
            TxMode::ReadWrite => TransactionMode::ReadWrite,
        };

        let Some(next_tx_id) = self.next_tx_id.checked_add(1) else {
            return KvResponse::Error {
                error: KvError::InvalidRequest("transaction ID space exhausted".to_string()),
            };
        };

        match self.store.begin_tx(cf, tx_mode) {
            Ok(tx) => {
                let tx_id = self.next_tx_id;
                self.next_tx_id = next_tx_id;
                let scoped_prefix =
                    Self::realm_resource_prefix(&scope.realm, &scope.area, &scope.resource);

                tracing::trace!(
                    "KvActor assigning transaction ID: {}, next_tx_id is now: {}",
                    tx_id,
                    self.next_tx_id
                );

                self.transactions.insert(
                    tx_id,
                    ActiveKvTx {
                        scope,
                        mode,
                        scoped_prefix,
                        column_family: cf,
                        tx,
                        write_options,
                        mutation_count: 0,
                        last_activity: Instant::now(),
                        inventory_delta: KvInventoryDelta::default(),
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
    fn handle_commit(&mut self, tx_id: u64, scope: &KvResourceScope) -> KvResponse {
        let Some(active) = self.transactions.get(&tx_id) else {
            return KvResponse::Error {
                error: KvError::InvalidTxId,
            };
        };
        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

        match self.transactions.remove(&tx_id) {
            None => KvResponse::Error {
                error: KvError::InvalidTxId,
            },
            Some(mut active) => {
                let inventory_scope = active.scope.clone();
                let inventory_column_family = active.column_family;
                let inventory_delta = std::mem::take(&mut active.inventory_delta);
                let inventory_write_options = Self::inventory_write_options(active.write_options);
                // Use write options provided by user at transaction begin
                match active.tx.commit(active.write_options) {
                    Ok(()) => {
                        if let Err(error) = Self::apply_inventory_delta(
                            &self.store,
                            inventory_column_family,
                            &inventory_scope,
                            &inventory_delta,
                            inventory_write_options,
                        ) {
                            tracing::warn!(?error, "KV inventory estimate update failed");
                        }
                        KvResponse::CommitOk
                    }
                    Err(e) => KvResponse::Error {
                        error: Self::map_midge_error(e),
                    },
                }
            }
        }
    }

    /// Rollback a transaction by ID
    fn handle_rollback(&mut self, tx_id: u64, scope: &KvResourceScope) -> KvResponse {
        let Some(active) = self.transactions.get(&tx_id) else {
            return KvResponse::Error {
                error: KvError::InvalidTxId,
            };
        };
        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

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
    fn handle_get(&mut self, tx_id: u64, scope: &KvResourceScope, key: &Bytes) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, key);

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
        scope: &KvResourceScope,
        key: &Bytes,
        value: &Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, key);
        match active.tx.put(scoped_key, value.to_vec(), None) {
            Ok(()) => {
                active.mutation_count = active.mutation_count.saturating_add(1);
                active.inventory_delta.mark_incomplete();
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
        scope: &KvResourceScope,
        key: &Bytes,
        value: &Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

        // Check if key exists first
        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, key);

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
                        active.mutation_count = active.mutation_count.saturating_add(1);
                        active.inventory_delta.record_key_change(
                            key,
                            None,
                            Some(key.len() + value.len()),
                        );
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
    fn handle_delete(&mut self, tx_id: u64, scope: &KvResourceScope, key: &Bytes) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, key);
        match active.tx.delete(scoped_key) {
            Ok(()) => {
                active.mutation_count = active.mutation_count.saturating_add(1);
                active.inventory_delta.mark_incomplete();
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
        scope: &KvResourceScope,
        start: &Bytes,
        end: &Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

        // Validate range
        if start >= end {
            return KvResponse::Error {
                error: KvError::InvalidRequest("start must be less than end".to_string()),
            };
        }

        let scoped_start = Self::encode_scoped_key(&active.scoped_prefix, start);
        let scoped_end = Self::encode_scoped_key(&active.scoped_prefix, end);

        match active.tx.delete_range(scoped_start, scoped_end) {
            Ok(()) => {
                active.mutation_count = active.mutation_count.saturating_add(1);
                active.inventory_delta.mark_incomplete();
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
        scope: &KvResourceScope,
        query: &ScanQuery,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, scope) {
            return response;
        }

        let prefix = active.scoped_prefix.clone();
        let Some((midge_query, effective_limit)) = Self::build_scan_query(&prefix, query) else {
            return KvResponse::ScanResult {
                items: Vec::new(),
                has_more: false,
            };
        };
        match active.tx.scan(&midge_query) {
            Ok(iterator) => Self::collect_scan_items(iterator, &prefix, effective_limit),
            Err(e) => KvResponse::Error {
                error: Self::map_midge_error(e),
            },
        }
    }

    fn build_scan_query(prefix: &[u8], query: &ScanQuery) -> Option<(cntryl_midge::Query, usize)> {
        let (start_key, end_key) = Self::scan_bounds(prefix, query)?;
        let effective_limit = query.limit.unwrap_or(MAX_SCAN_ITEMS).min(MAX_SCAN_ITEMS);
        let mut midge_query = cntryl_midge::Query::new()
            .prefix(Bytes::copy_from_slice(prefix))
            .start_key(Bytes::from(start_key))
            .end_key(Bytes::from(end_key))
            .limit(effective_limit.saturating_add(1));
        if query.reverse {
            midge_query = midge_query.reverse();
        }
        Some((midge_query, effective_limit))
    }

    fn collect_scan_items(
        iterator: cntryl_midge::ScanIterator<'_>,
        prefix: &[u8],
        effective_limit: usize,
    ) -> KvResponse {
        let mut items = Vec::new();
        for entry in iterator {
            let (key, value) = match entry {
                Ok(row) => row,
                Err(error) => {
                    return KvResponse::Error {
                        error: Self::map_midge_error(error),
                    };
                }
            };
            let Some(user_key) = Self::strip_scoped_prefix(prefix, &key) else {
                continue;
            };
            items.push(KvPair {
                key: Bytes::from(user_key),
                value,
            });
        }
        let has_more = items.len() > effective_limit;
        items.truncate(effective_limit);
        KvResponse::ScanResult { items, has_more }
    }

    /// Get active transaction or return error
    fn get_transaction_or_err(&mut self, tx_id: u64) -> Result<&mut ActiveKvTx, KvResponse> {
        let transaction = self
            .transactions
            .get_mut(&tx_id)
            .ok_or_else(|| KvResponse::Error {
                error: KvError::InvalidTxId,
            })?;
        transaction.last_activity = Instant::now();
        Ok(transaction)
    }

    pub(crate) fn expire_idle_transactions(&mut self, ttl: Duration) -> Vec<u64> {
        let now = Instant::now();
        let expired: Vec<_> = self
            .transactions
            .iter()
            .filter_map(|(tx_id, transaction)| {
                (now.saturating_duration_since(transaction.last_activity) >= ttl).then_some(*tx_id)
            })
            .collect();
        for tx_id in &expired {
            self.transactions.remove(tx_id);
        }
        expired
    }

    pub(crate) fn rollback_transaction(&mut self, tx_id: u64) -> bool {
        self.transactions.remove(&tx_id).is_some()
    }

    #[must_use]
    pub fn mutation_count_for_tx(&self, tx_id: u64) -> Option<u64> {
        self.transactions.get(&tx_id).map(|tx| tx.mutation_count)
    }

    #[must_use]
    pub fn resource_scope_for_tx(&self, tx_id: u64) -> Option<(u64, String, String, String)> {
        self.transactions.get(&tx_id).map(|tx| {
            (
                u64::from(tx.column_family),
                tx.scope.realm.clone(),
                tx.scope.area.clone(),
                tx.scope.resource.clone(),
            )
        })
    }

    #[must_use]
    pub fn active_transaction_scopes(&self) -> Vec<(u64, u64, String, String, String)> {
        self.transactions
            .iter()
            .map(|(tx_id, tx)| {
                (
                    *tx_id,
                    u64::from(tx.column_family),
                    tx.scope.realm.clone(),
                    tx.scope.area.clone(),
                    tx.scope.resource.clone(),
                )
            })
            .collect()
    }

    #[must_use]
    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    fn validate_operation_scope(
        active: &ActiveKvTx,
        scope: &KvResourceScope,
    ) -> Result<(), KvResponse> {
        if scope.route_family != active.scope.route_family {
            return Err(KvResponse::Error {
                error: KvError::InvalidRouteFamily,
            });
        }
        if scope.realm != active.scope.realm {
            return Err(KvResponse::Error {
                error: KvError::RealmMismatch,
            });
        }
        if scope.area != active.scope.area || scope.resource != active.scope.resource {
            return Err(KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: format!("{}/{}", active.scope.area, active.scope.resource),
                    actual: format!("{}/{}", scope.area, scope.resource),
                },
            });
        }

        Ok(())
    }

    fn scan_bounds(prefix: &[u8], query: &ScanQuery) -> Option<(Vec<u8>, Vec<u8>)> {
        if let (Some(start), Some(end)) = (&query.start, &query.end) {
            let interval_is_empty = if query.reverse {
                start <= end
            } else {
                start >= end
            };
            if interval_is_empty {
                return None;
            }
        }

        if query.reverse {
            let lower = query.end.as_ref().map_or_else(
                || prefix.to_vec(),
                |key| Self::immediate_successor(Self::encode_scoped_key(prefix, key)),
            );
            let upper = query.start.as_ref().map_or_else(
                || Self::prefix_range_end(prefix),
                |key| Self::immediate_successor(Self::encode_scoped_key(prefix, key)),
            );
            Some((lower, upper))
        } else {
            let lower = query.start.as_ref().map_or_else(
                || prefix.to_vec(),
                |key| Self::encode_scoped_key(prefix, key),
            );
            let upper = query.end.as_ref().map_or_else(
                || Self::prefix_range_end(prefix),
                |key| Self::encode_scoped_key(prefix, key),
            );
            Some((lower, upper))
        }
    }

    fn immediate_successor(mut key: Vec<u8>) -> Vec<u8> {
        key.push(0);
        key
    }

    pub(crate) fn realm_resource_prefix(realm: &str, area: &str, resource: &str) -> Vec<u8> {
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

    pub(crate) fn inventory_metadata_key(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        let mut encoder = storage_key::domain_marker_encoder(
            realm,
            DomainKeyspace::Kv,
            KV_INVENTORY_SCOPE_MARKER,
            area.len() + resource.len() + 2,
        );
        storage_key::encode_bytes_segment_into(&mut encoder, area.as_bytes());
        storage_key::encode_bytes_segment_into(&mut encoder, resource.as_bytes());
        encoder.into_vec()
    }

    pub(crate) fn parse_inventory_metadata_key(key: &[u8]) -> Option<(String, String, String)> {
        let (realm, suffix) = storage_key::split_domain_key(key, DomainKeyspace::Kv)?;
        if suffix.first().copied()? != KV_INVENTORY_SCOPE_MARKER {
            return None;
        }

        let mut parts = suffix[1..].split(|byte| *byte == lexkey::LexKey::SEPARATOR);
        let area = parts.next()?;
        let resource = parts.next()?;
        let trailing = parts.next();
        if area.is_empty() || resource.is_empty() || trailing != Some(&[]) || parts.next().is_some()
        {
            return None;
        }

        Some((
            realm.to_string(),
            String::from_utf8(area.to_vec()).ok()?,
            String::from_utf8(resource.to_vec()).ok()?,
        ))
    }

    pub(crate) fn encode_inventory_estimate(estimate: KvInventoryEstimate) -> Vec<u8> {
        let mut out = Vec::with_capacity(KV_INVENTORY_VALUE_LEN);
        out.push(KV_INVENTORY_VALUE_VERSION);
        out.push(u8::from(estimate.estimate_complete));
        out.extend_from_slice(&estimate.estimated_record_count.to_be_bytes());
        out.extend_from_slice(&estimate.estimated_storage_bytes.to_be_bytes());
        out
    }

    pub(crate) fn decode_inventory_estimate(bytes: &[u8]) -> Result<KvInventoryEstimate, String> {
        if bytes.len() != KV_INVENTORY_VALUE_LEN
            || bytes.first().copied() != Some(KV_INVENTORY_VALUE_VERSION)
        {
            return Err("invalid KV inventory metadata value".to_string());
        }

        let estimated_record_count = u64::from_be_bytes(
            bytes[KV_INVENTORY_RECORD_COUNT_RANGE]
                .try_into()
                .map_err(|_| "invalid KV inventory record count".to_string())?,
        );
        let estimated_storage_bytes = u64::from_be_bytes(
            bytes[KV_INVENTORY_STORAGE_BYTES_RANGE]
                .try_into()
                .map_err(|_| "invalid KV inventory storage estimate".to_string())?,
        );

        Ok(KvInventoryEstimate {
            estimated_record_count,
            estimated_storage_bytes,
            estimate_complete: bytes[1] != 0,
        })
    }

    /// Map the durability class of a just-committed transaction to write
    /// options safe for the derived, best-effort inventory-estimate commit.
    ///
    /// Inventory updates always want throughput-first durability, but must
    /// stay in the same local/cloud storage class as the primary commit:
    /// Midge rejects `sync()`/`buffered()` outright when the engine is
    /// cloud-backed (see `effective_wal_durability_policy` in
    /// `cntryl_midge`), so hardcoding `buffered()` here would fail on every
    /// mutating commit once storage is cloud-backed.
    fn inventory_write_options(
        committed: cntryl_midge::WriteOptions,
    ) -> cntryl_midge::WriteOptions {
        if committed.is_cloud_async() || committed.is_cloud_strict() {
            cntryl_midge::WriteOptions::cloud_async()
        } else {
            cntryl_midge::WriteOptions::buffered()
        }
    }

    fn apply_inventory_delta(
        store: &MidgeEngine,
        column_family: ColumnFamilyId,
        scope: &KvResourceScope,
        inventory_delta: &KvInventoryDelta,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<(), KvError> {
        if inventory_delta.is_empty() {
            return Ok(());
        }

        let key = Self::inventory_metadata_key(&scope.realm, &scope.area, &scope.resource);
        let mut tx = store
            .begin_tx(column_family, TransactionMode::ReadWrite)
            .map_err(Self::map_midge_error)?;
        let mut estimate = tx
            .get(&key)
            .map_err(Self::map_midge_error)?
            .as_deref()
            .map(Self::decode_inventory_estimate)
            .transpose()
            .map_err(KvError::BackendError)?
            .unwrap_or_default();

        for change in inventory_delta.key_changes.values() {
            match (change.before_bytes, change.after_bytes) {
                (None, Some(after)) => {
                    estimate.estimated_record_count =
                        estimate.estimated_record_count.saturating_add(1);
                    estimate.estimated_storage_bytes = estimate
                        .estimated_storage_bytes
                        .saturating_add(after as u64);
                }
                (Some(before), None) => {
                    estimate.estimated_record_count =
                        estimate.estimated_record_count.saturating_sub(1);
                    estimate.estimated_storage_bytes = estimate
                        .estimated_storage_bytes
                        .saturating_sub(before as u64);
                }
                (Some(before), Some(after)) => {
                    if after >= before {
                        estimate.estimated_storage_bytes = estimate
                            .estimated_storage_bytes
                            .saturating_add((after - before) as u64);
                    } else {
                        estimate.estimated_storage_bytes = estimate
                            .estimated_storage_bytes
                            .saturating_sub((before - after) as u64);
                    }
                }
                (None, None) => {}
            }
        }

        if inventory_delta.estimate_incomplete {
            estimate.estimate_complete = false;
        }

        tx.put(key, Self::encode_inventory_estimate(estimate), None)
            .map_err(Self::map_midge_error)?;
        tx.commit(write_options).map_err(Self::map_midge_error)
    }
}

impl Actor for KvActor {
    type Message = KvMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        let response = self.handle(msg);
        let _ = ctx.reply(response);
    }
}

#[cfg(test)]
mod tests;
