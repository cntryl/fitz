//! Range scans with item, pagination, and wire-size bounds.

use super::KvActor;
use crate::domains::kv::{KvError, KvPair, KvResourceScope, KvResponse, ScanQuery};
use bytes::Bytes;

pub(super) const MAX_SCAN_ITEMS: usize = 1_024;

impl KvActor {
    pub(super) fn handle_scan(
        &mut self,
        tx_id: u64,
        scope: &KvResourceScope,
        query: &ScanQuery,
    ) -> KvResponse {
        let active = match self.scoped_transaction_or_err(tx_id, scope) {
            Ok(tx) => tx,
            Err(error) => return error,
        };

        let prefix = active.scoped_prefix.clone();
        let Some((midge_query, effective_limit)) = Self::build_scan_query(&prefix, query) else {
            return KvResponse::ScanResult {
                items: Vec::new(),
                has_more: false,
            };
        };
        match active.tx.scan(&midge_query) {
            Ok(iterator) => Self::collect_scan_items(iterator, &prefix, effective_limit),
            Err(error) => KvResponse::Error {
                error: Self::map_midge_error(&error),
            },
        }
    }

    fn build_scan_query(prefix: &[u8], query: &ScanQuery) -> Option<(cntryl_midge::Query, usize)> {
        let (start_key, end_key) = Self::scan_bounds(prefix, query)?;
        let effective_limit = query
            .limit
            .filter(|&limit| limit > 0)
            .unwrap_or(MAX_SCAN_ITEMS)
            .min(MAX_SCAN_ITEMS);
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

    fn truncated_key_for_error(key: &[u8]) -> String {
        const MAX_ECHOED_KEY_BYTES: usize = 64;

        let head = &key[..key.len().min(MAX_ECHOED_KEY_BYTES)];
        let rendered = String::from_utf8_lossy(head);
        if key.len() > MAX_ECHOED_KEY_BYTES {
            format!("{rendered}...")
        } else {
            rendered.into_owned()
        }
    }

    fn collect_scan_items(
        iterator: cntryl_midge::ScanIterator<'_>,
        prefix: &[u8],
        effective_limit: usize,
    ) -> KvResponse {
        let ceiling = crate::domains::kv::scan_wire_budget::kv_scan_response_byte_ceiling();
        let mut items: Vec<KvPair> = Vec::new();
        let mut used = 0usize;
        let mut has_more = false;
        let mut unresumable_boundary: Option<Bytes> = None;
        for entry in iterator {
            let (key, value) = match entry {
                Ok(row) => row,
                Err(error) => {
                    return KvResponse::Error {
                        error: Self::map_midge_error(&error),
                    };
                }
            };
            let Some(user_key) = Self::strip_scoped_prefix(prefix, &key) else {
                continue;
            };
            if let Some(boundary) = unresumable_boundary.as_ref() {
                return KvResponse::Error {
                    error: KvError::InvalidRequest(format!(
                        "scan key {} ({} bytes) cannot become a continuation start_key without \
                         itself exceeding the request wire limit",
                        Self::truncated_key_for_error(boundary),
                        boundary.len()
                    )),
                };
            }
            if items.len() >= effective_limit {
                has_more = true;
                break;
            }
            let cost = crate::domains::kv::scan_wire_budget::kv_scan_item_wire_bytes(
                user_key.len(),
                value.len(),
            );
            let unresumable = user_key.len()
                > crate::domains::kv::scan_wire_budget::kv_scan_continuation_max_key_bytes();
            if used.saturating_add(cost) > ceiling {
                if items.is_empty() {
                    return KvResponse::Error {
                        error: KvError::InvalidRequest(format!(
                            "scan pair {} ({} byte key) is {cost} wire bytes, exceeding the \
                             {ceiling}-byte limit a scan response can return",
                            Self::truncated_key_for_error(&user_key),
                            user_key.len()
                        )),
                    };
                }
                has_more = true;
                break;
            }
            used = used.saturating_add(cost);
            items.push(KvPair {
                key: Bytes::from(user_key),
                value,
            });
            unresumable_boundary = if unresumable {
                items.last().map(|item| item.key.clone())
            } else {
                None
            };
        }
        KvResponse::ScanResult { items, has_more }
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
                |key| {
                    let scoped = Self::encode_scoped_key(prefix, key);
                    if query.start_exclusive {
                        scoped
                    } else {
                        Self::immediate_successor(scoped)
                    }
                },
            );
            Some((lower, upper))
        } else {
            let lower = query.start.as_ref().map_or_else(
                || prefix.to_vec(),
                |key| {
                    let scoped = Self::encode_scoped_key(prefix, key);
                    if query.start_exclusive {
                        Self::immediate_successor(scoped)
                    } else {
                        scoped
                    }
                },
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
}
