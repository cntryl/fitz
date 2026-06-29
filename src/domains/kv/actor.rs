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
const KV_INVENTORY_SCOPE_MARKER: u8 = 0x02;
const KV_INVENTORY_VALUE_VERSION: u8 = 1;

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

#[derive(Clone, Copy, Debug)]
struct KvKeyInventoryChange {
    before_bytes: Option<usize>,
    after_bytes: Option<usize>,
}

#[derive(Default)]
struct KvInventoryDelta {
    key_changes: HashMap<Vec<u8>, KvKeyInventoryChange>,
    estimate_incomplete: bool,
}

impl KvInventoryDelta {
    fn is_empty(&self) -> bool {
        self.key_changes.is_empty() && !self.estimate_incomplete
    }

    fn mark_incomplete(&mut self) {
        self.estimate_incomplete = true;
    }

    fn record_key_change(
        &mut self,
        user_key: &[u8],
        before_bytes: Option<usize>,
        after_bytes: Option<usize>,
    ) {
        self.key_changes
            .entry(user_key.to_vec())
            .and_modify(|change| change.after_bytes = after_bytes)
            .or_insert(KvKeyInventoryChange {
                before_bytes,
                after_bytes,
            });
    }
}

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
    fn handle_commit(&mut self, tx_id: u64) -> KvResponse {
        match self.transactions.remove(&tx_id) {
            None => KvResponse::Error {
                error: KvError::InvalidTxId,
            },
            Some(mut active) => {
                if let Err(error) = Self::apply_inventory_delta(&mut active) {
                    return KvResponse::Error { error };
                }
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
        route_family: RouteFamily,
        resource: String,
        key: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, route_family, &resource) {
            return response;
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
        route_family: RouteFamily,
        resource: String,
        key: Bytes,
        value: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, route_family, &resource) {
            return response;
        }

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, &key);
        let before_bytes = match active.tx.get(&scoped_key) {
            Ok(value) => value.as_ref().map(|value| key.len() + value.len()),
            Err(e) => {
                return KvResponse::Error {
                    error: Self::map_midge_error(e),
                };
            }
        };
        let after_bytes = Some(key.len() + value.len());

        match active.tx.put(scoped_key, value.to_vec(), None) {
            Ok(()) => {
                active.mutation_count += 1;
                active
                    .inventory_delta
                    .record_key_change(&key, before_bytes, after_bytes);
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
        route_family: RouteFamily,
        resource: String,
        key: Bytes,
        value: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, route_family, &resource) {
            return response;
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
                        active.inventory_delta.record_key_change(
                            &key,
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
    fn handle_delete(
        &mut self,
        tx_id: u64,
        route_family: RouteFamily,
        resource: String,
        key: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, route_family, &resource) {
            return response;
        }

        let scoped_key = Self::encode_scoped_key(&active.scoped_prefix, &key);
        let before_bytes = match active.tx.get(&scoped_key) {
            Ok(value) => value.as_ref().map(|value| key.len() + value.len()),
            Err(e) => {
                return KvResponse::Error {
                    error: Self::map_midge_error(e),
                };
            }
        };

        match active.tx.delete(scoped_key) {
            Ok(()) => {
                active.mutation_count += 1;
                active
                    .inventory_delta
                    .record_key_change(&key, before_bytes, None);
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
        route_family: RouteFamily,
        resource: String,
        start: Bytes,
        end: Bytes,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, route_family, &resource) {
            return response;
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
        route_family: RouteFamily,
        resource: String,
        query: ScanQuery,
    ) -> KvResponse {
        let active = match self.get_transaction_or_err(tx_id) {
            Ok(tx) => tx,
            Err(err) => return err,
        };

        if let Err(response) = Self::validate_operation_scope(active, route_family, &resource) {
            return response;
        }

        let prefix = active.scoped_prefix.clone();
        let start_key = query
            .start
            .as_ref()
            .map_or_else(|| prefix.clone(), |k| Self::encode_scoped_key(&prefix, k));
        let end_key = query.end.as_ref().map_or_else(
            || Self::prefix_range_end(&prefix),
            |k| Self::encode_scoped_key(&prefix, k),
        );

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

    #[must_use]
    pub fn mutation_count_for_tx(&self, tx_id: u64) -> Option<u64> {
        self.transactions.get(&tx_id).map(|tx| tx.mutation_count)
    }

    #[must_use]
    pub fn resource_scope_for_tx(&self, tx_id: u64) -> Option<(u64, String, String, String)> {
        self.transactions.get(&tx_id).map(|tx| {
            (
                tx.column_family as u64,
                tx.bound_realm.clone(),
                tx.bound_area.clone(),
                tx.bound_resource.clone(),
            )
        })
    }

    fn validate_operation_scope(
        active: &ActiveKvTx,
        route_family: RouteFamily,
        resource: &str,
    ) -> Result<(), KvResponse> {
        let column_family =
            Self::resolve_column_family(route_family, resource).map_err(|_| KvResponse::Error {
                error: KvError::InvalidRouteFamily,
            })?;

        if column_family != active.column_family {
            return Err(KvResponse::Error {
                error: KvError::InvalidRouteFamily,
            });
        }

        // Per CLIENT_SPEC: resource is implicit from transaction context.
        // If resource is provided, validate it matches; if empty, use bound_resource.
        if !resource.is_empty() && resource != active.bound_resource {
            return Err(KvResponse::Error {
                error: KvError::TxScopeViolation {
                    expected: active.bound_resource.clone(),
                    actual: resource.to_string(),
                },
            });
        }

        Ok(())
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
        let mut out = Vec::with_capacity(18);
        out.push(KV_INVENTORY_VALUE_VERSION);
        out.push(u8::from(estimate.estimate_complete));
        out.extend_from_slice(&estimate.estimated_record_count.to_be_bytes());
        out.extend_from_slice(&estimate.estimated_storage_bytes.to_be_bytes());
        out
    }

    pub(crate) fn decode_inventory_estimate(bytes: &[u8]) -> Result<KvInventoryEstimate, String> {
        if bytes.len() != 18 || bytes.first().copied() != Some(KV_INVENTORY_VALUE_VERSION) {
            return Err("invalid KV inventory metadata value".to_string());
        }

        let estimated_record_count = u64::from_be_bytes(
            bytes[2..10]
                .try_into()
                .map_err(|_| "invalid KV inventory record count".to_string())?,
        );
        let estimated_storage_bytes = u64::from_be_bytes(
            bytes[10..18]
                .try_into()
                .map_err(|_| "invalid KV inventory storage estimate".to_string())?,
        );

        Ok(KvInventoryEstimate {
            estimated_record_count,
            estimated_storage_bytes,
            estimate_complete: bytes[1] != 0,
        })
    }

    fn apply_inventory_delta(active: &mut ActiveKvTx) -> Result<(), KvError> {
        if active.inventory_delta.is_empty() {
            return Ok(());
        }

        let key = Self::inventory_metadata_key(
            &active.bound_realm,
            &active.bound_area,
            &active.bound_resource,
        );
        let mut estimate = active
            .tx
            .get(&key)
            .map_err(Self::map_midge_error)?
            .as_deref()
            .map(Self::decode_inventory_estimate)
            .transpose()
            .map_err(KvError::BackendError)?
            .unwrap_or_default();

        for change in active.inventory_delta.key_changes.values() {
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

        if active.inventory_delta.estimate_incomplete {
            estimate.estimate_complete = false;
        }

        active
            .tx
            .put(key, Self::encode_inventory_estimate(estimate), None)
            .map_err(Self::map_midge_error)
    }

    /// Resolve column family from RouteFamily and resource
    ///
    /// Uses explicit mapping: ColumnFamilyId = RouteFamily.id
    /// This ensures data isolation per route family.
    ///
    /// # Errors
    ///
    /// Returns an error if RouteFamily is 0 (would map to default CF).
    pub(crate) fn resolve_column_family(
        route_family: RouteFamily,
        _resource: &str,
    ) -> Result<ColumnFamilyId, String> {
        // Validate RouteFamily is not zero (would map to default CF)
        crate::runtime::cf_validation::validate_route_family(route_family)?;

        // Map RouteFamily → ColumnFamily (1:1 by value)
        // Resource is enforced via key prefixing within the column family.
        Ok(route_family.id())
    }

    pub(crate) fn encode_scoped_key(prefix: &[u8], user_key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(prefix.len() + user_key.len());
        out.extend_from_slice(prefix);
        out.extend_from_slice(user_key);
        out
    }

    pub(crate) fn strip_scoped_prefix(prefix: &[u8], scoped_key: &[u8]) -> Option<Vec<u8>> {
        scoped_key.strip_prefix(prefix).map(|rest| rest.to_vec())
    }

    pub(crate) fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
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
mod tests;
