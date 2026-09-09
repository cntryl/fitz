//! KV transaction state machine over Midge.
//!
//! Committed values are durable according to the transaction write policy.
//! Open transaction handles and uncommitted mutations are session-scoped,
//! broker-local state and disappear on cleanup or restart.

use std::collections::HashMap;
use std::time::Instant;

use crate::prelude::Actor;
use crate::runtime::actor::Context;

use super::protocol::{KvMessage, KvResourceScope, KvResponse, TxMode};

mod introspection;
mod inventory_delta;
mod key_layout;
mod mutations;
mod scan;
mod transaction_access;
mod transactions;

use inventory_delta::KvInventoryDelta;
#[cfg(test)]
use key_layout::KV_KEY_SCOPE_MARKER;
#[cfg(test)]
use scan::MAX_SCAN_ITEMS;

#[cfg(test)]
use super::protocol::{KvError, ScanQuery};
#[cfg(test)]
use bytes::Bytes;

/// One live transaction bound to a single KV resource.
struct ActiveKvTx {
    scope: KvResourceScope,
    scoped_prefix: Vec<u8>,
    column_family: u32,
    tx: super::store::KvTransaction,
    mode: TxMode,
    write_policy: crate::domains::WritePolicy,
    mutation_count: u64,
    last_activity: Instant,
    inventory_delta: KvInventoryDelta,
}

/// Session-scoped KV transaction state.
pub struct KvActor {
    store: super::store::KvStore,
    transactions: HashMap<u64, ActiveKvTx>,
    next_tx_id: u64,
}

impl KvActor {
    #[must_use]
    #[allow(private_bounds)]
    pub fn new(store: impl Into<super::store::KvStore>) -> Self {
        Self {
            store: store.into(),
            transactions: HashMap::new(),
            next_tx_id: 1,
        }
    }

    pub fn handle(&mut self, message: KvMessage) -> KvResponse {
        match message {
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
}

impl Actor for KvActor {
    type Message = KvMessage;

    fn receive(&mut self, message: Self::Message, context: &mut Context<Self>) {
        let response = self.handle(message);
        let _ = context.reply(response);
    }
}

#[cfg(test)]
mod tests;
