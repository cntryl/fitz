//! Patterned Lease `LIST`: a bounded, paginated read of the current held-lease
//! inventory matching a selector (issue #219 §2).
//!
//! `LIST` never mutates ownership. An exact selector is a direct keyed
//! `BTreeMap` lookup — a zero-or-one read, never paginated. A wildcard scan
//! is materialized eagerly in one atomic pass over exactly the requesting
//! family's key range (bounded by `LEASE_LIST_MAX_CANDIDATES_PER_SCAN`) and
//! stored as a true point-in-time snapshot; pages are then drained off it
//! without ever re-reading `core.leases`. A cursor from a different
//! selector, `RouteFamily`, unknown/evicted snapshot, wrong offset, or a
//! prior broker lifetime fails explicitly (`InvalidListCursor`) rather than
//! silently narrowing or restarting the read. See `LeaseListSnapshot` in
//! `model.rs` for the full consistency contract.

use super::model::{Instant, LeaseDomainRuntime, LeaseListSnapshot, Ordering};
use crate::domains::lease::protocol::{
    LeaseKey, LeaseListCursor, LeaseListItem, LeaseResponse, LEASE_LIST_DEFAULT_PAGE_SIZE,
    LEASE_LIST_MAX_CANDIDATES_PER_SCAN, LEASE_LIST_MAX_PAGE_SIZE,
    LEASE_LIST_MAX_RETAINED_BYTES_PER_SESSION, LEASE_LIST_MAX_RETAINED_BYTES_TOTAL,
    LEASE_LIST_MAX_RETAINED_ITEMS_TOTAL, LEASE_LIST_MAX_SNAPSHOTS, LEASE_LIST_PAGE_BYTE_BUDGET,
};
use crate::runtime::routing::{Route, RouteFamily};

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Wire-encoded byte cost of one item, matching `encode_list_page_into` in
/// `src/protocol/lease_codec.rs` exactly: four length-prefixed fields plus
/// three fixed-width ones.
fn encoded_item_bytes(item: &LeaseListItem) -> usize {
    4 + item.route.as_str().len()
        + 4
        + item.owner_id.len()
        + 8 // holder_incarnation
        + 4
        + item.acquired_at.len()
        + 8 // expires_in_secs
        + 4 // renewals
}

/// Exclusive upper bound (smallest key strictly outside `family_id`) for a
/// `BTreeMap<LeaseKey, _>` range scoped to one family. `None` when
/// `family_id` is already the largest possible family, in which case the
/// range has no upper bound at all.
fn family_upper_bound(family_id: RouteFamily) -> Option<LeaseKey> {
    family_id.id().checked_add(1).map(|next| LeaseKey {
        family: RouteFamily::new(next),
        realm: String::new(),
        area: String::new(),
        resource: String::new(),
    })
}

fn family_lower_bound(family_id: RouteFamily) -> LeaseKey {
    LeaseKey {
        family: family_id,
        realm: String::new(),
        area: String::new(),
        resource: String::new(),
    }
}

impl LeaseDomainRuntime<'_> {
    pub(super) fn handle_list(
        &self,
        family_id: RouteFamily,
        pattern_route: &Route,
        cursor: Option<LeaseListCursor>,
        limit: Option<u32>,
        session_id: u64,
    ) -> LeaseResponse {
        let page_size = limit
            .unwrap_or(LEASE_LIST_DEFAULT_PAGE_SIZE)
            .clamp(1, LEASE_LIST_MAX_PAGE_SIZE) as usize;

        match cursor {
            Some(cursor) => {
                self.continue_list_scan(family_id, pattern_route, cursor, page_size, session_id)
            }
            None => self.start_list_scan(family_id, pattern_route, page_size, session_id),
        }
    }

    fn start_list_scan(
        &self,
        family_id: RouteFamily,
        pattern_route: &Route,
        page_size: usize,
        session_id: u64,
    ) -> LeaseResponse {
        // Exact selectors are a zero-or-one keyed read: parse straight to a
        // LeaseKey and look it up directly rather than scanning and
        // pattern-matching the whole family (issue #219 review: exact LIST
        // must use the same keyed-lookup path QUERY does).
        if let Some(key) = LeaseKey::from_route(family_id, pattern_route) {
            let now = Instant::now();
            let item = self
                .core
                .leases
                .lock()
                .get(&key)
                .filter(|state| state.expiry > now)
                .map(|state| self.to_list_item(&key, state, now));
            return LeaseResponse::ListPage {
                items: item.into_iter().collect(),
                next_cursor: None,
            };
        }

        let pattern = match crate::runtime::DomainKind::Lease
            .descriptor()
            .compile_registration_pattern(pattern_route.as_str())
        {
            Ok(pattern) => pattern,
            Err(message) => return LeaseResponse::InvalidListPattern(message),
        };

        // Materialize the full, exact match set in one atomic pass — this
        // is what makes the scan a true point-in-time snapshot (issue #219
        // §2). A family whose candidate count exceeds the bound fails
        // outright rather than silently doing unbounded work or filling
        // across multiple actor messages, which would let concurrent
        // mutation of the not-yet-examined tail affect the outcome.
        let now = Instant::now();
        let lower = family_lower_bound(family_id);
        let upper = family_upper_bound(family_id);
        let mut items: Vec<LeaseListItem> = Vec::new();
        let mut captured_bytes = 0_usize;
        {
            use std::ops::Bound::{Excluded, Included, Unbounded};
            let leases = self.core.leases.lock();
            let range = leases.range((Included(lower), upper.map_or(Unbounded, Excluded)));
            for (examined, (key, state)) in range.enumerate() {
                if examined >= LEASE_LIST_MAX_CANDIDATES_PER_SCAN {
                    return LeaseResponse::Error(format!(
                        "selector matched too many candidates (over {LEASE_LIST_MAX_CANDIDATES_PER_SCAN}) to scan in one pass; narrow the selector"
                    ));
                }
                // Expiry is checked here (not just at Tick) so a
                // not-yet-swept expired entry is never reported as held,
                // matching Query.
                if state.expiry > now && pattern.matches_str(key.to_route().as_str()) {
                    let item = self.to_list_item(key, state, now);
                    captured_bytes = captured_bytes.saturating_add(encoded_item_bytes(&item));
                    // One page is returned immediately; only the remainder
                    // is retained. Cap materialization at that page plus the
                    // per-session retained-byte ceiling so the temporary
                    // vector itself cannot grow toward the candidate-count
                    // bound using maximum-sized items.
                    if captured_bytes
                        > LEASE_LIST_MAX_RETAINED_BYTES_PER_SESSION
                            .saturating_add(LEASE_LIST_PAGE_BYTE_BUDGET)
                    {
                        return LeaseResponse::Error(
                            "selector matched too much inventory to snapshot; narrow the selector"
                                .to_string(),
                        );
                    }
                    items.push(item);
                }
            }
        }
        // core.leases's BTreeMap orders by (family, realm, area, resource),
        // which is exactly route order within one family — already sorted.

        self.store_and_serve(
            family_id,
            pattern_route.as_str(),
            items,
            page_size,
            session_id,
        )
    }

    fn continue_list_scan(
        &self,
        family_id: RouteFamily,
        pattern_route: &Route,
        cursor: LeaseListCursor,
        page_size: usize,
        session_id: u64,
    ) -> LeaseResponse {
        let mut snapshots = self.core.list_snapshots.lock();
        let matches = snapshots.get(&cursor.snapshot_id).is_some_and(|snapshot| {
            snapshot.session_id == session_id
                && snapshot.family_id == family_id
                && snapshot.pattern_route == pattern_route.as_str()
                // The offset must be exactly the next expected one, not
                // merely in bounds: an opaque continuation is not a
                // client-editable seek position (issue #219 review).
                && cursor.offset == snapshot.served_count
        });
        if !matches {
            // A mismatched request (wrong session/family/pattern/offset)
            // never consumes the snapshot: a legitimate retry with the
            // correct request can still resolve it. A snapshot ID that
            // genuinely doesn't exist falls into the same `false` case via
            // `get` returning `None`.
            return LeaseResponse::InvalidListCursor;
        }
        let snapshot = snapshots
            .get_mut(&cursor.snapshot_id)
            .expect("checked present and matching above");
        snapshot.last_touched_at = Instant::now();

        let end = Self::budgeted_page_end(&snapshot.items, page_size);
        let drained_bytes = snapshot.items[..end]
            .iter()
            .map(encoded_item_bytes)
            .sum::<usize>();
        let page: Vec<_> = snapshot.items.drain(..end).collect();
        snapshot.retained_bytes = snapshot.retained_bytes.saturating_sub(drained_bytes);
        snapshot.served_count = snapshot
            .served_count
            .saturating_add(usize_to_u32_saturating(end));

        if snapshot.items.is_empty() {
            snapshots.remove(&cursor.snapshot_id);
            LeaseResponse::ListPage {
                items: page,
                next_cursor: None,
            }
        } else {
            let next_cursor = Some(LeaseListCursor {
                snapshot_id: cursor.snapshot_id,
                offset: snapshot.served_count,
            });
            LeaseResponse::ListPage {
                items: page,
                next_cursor,
            }
        }
    }

    /// Slices a byte- and count-bounded first page out of a freshly
    /// materialized item set, then either finishes (no cursor, nothing
    /// retained) or stores the remainder as a snapshot for continuation.
    fn store_and_serve(
        &self,
        family_id: RouteFamily,
        pattern_route: &str,
        mut items: Vec<LeaseListItem>,
        page_size: usize,
        session_id: u64,
    ) -> LeaseResponse {
        let end = Self::budgeted_page_end(&items, page_size);
        if end >= items.len() {
            return LeaseResponse::ListPage {
                items,
                next_cursor: None,
            };
        }

        let remainder = items.split_off(end);
        let page = items;
        let served_count = usize_to_u32_saturating(page.len());
        let retained_bytes = remainder.iter().map(encoded_item_bytes).sum::<usize>();

        let Ok(snapshot_id) = self.core.next_list_snapshot_id.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| current.checked_add(1),
        ) else {
            // Snapshot ID space exhausted: serve this page without a
            // cursor rather than issuing one that can never resolve.
            return LeaseResponse::ListPage {
                items: page,
                next_cursor: None,
            };
        };

        let mut snapshots = self.core.list_snapshots.lock();
        if !Self::admit_snapshot(&mut snapshots, session_id, remainder.len(), retained_bytes) {
            return LeaseResponse::Error(
                "too many concurrent LIST scans in progress; retry later".to_string(),
            );
        }
        snapshots.insert(
            snapshot_id,
            LeaseListSnapshot {
                session_id,
                family_id,
                pattern_route: pattern_route.to_string(),
                items: remainder,
                retained_bytes,
                served_count,
                last_touched_at: Instant::now(),
            },
        );
        drop(snapshots);

        LeaseResponse::ListPage {
            items: page,
            next_cursor: Some(LeaseListCursor {
                snapshot_id,
                offset: served_count,
            }),
        }
    }

    /// Bounds total memory retained by outstanding `LIST` scans (issue #219
    /// §8) by both snapshot count and total not-yet-served item count:
    /// evicts the least-recently-touched snapshot (never one a client is
    /// actively mid-page through more recently than another) until the new
    /// snapshot of `incoming_items` fits, or reports it cannot be admitted.
    fn admit_snapshot(
        snapshots: &mut std::collections::HashMap<u64, LeaseListSnapshot>,
        session_id: u64,
        incoming_items: usize,
        incoming_bytes: usize,
    ) -> bool {
        if incoming_bytes > LEASE_LIST_MAX_RETAINED_BYTES_PER_SESSION
            || incoming_bytes > LEASE_LIST_MAX_RETAINED_BYTES_TOTAL
            || incoming_items > LEASE_LIST_MAX_RETAINED_ITEMS_TOTAL
        {
            return false;
        }
        loop {
            let total_items: usize = snapshots
                .values()
                .map(|snapshot| snapshot.items.len())
                .sum();
            let total_bytes = snapshots
                .values()
                .map(|snapshot| snapshot.retained_bytes)
                .sum::<usize>();
            let session_bytes = snapshots
                .values()
                .filter(|snapshot| snapshot.session_id == session_id)
                .map(|snapshot| snapshot.retained_bytes)
                .sum::<usize>();
            let fits = snapshots.len() < LEASE_LIST_MAX_SNAPSHOTS
                && total_items.saturating_add(incoming_items)
                    <= LEASE_LIST_MAX_RETAINED_ITEMS_TOTAL
                && total_bytes.saturating_add(incoming_bytes)
                    <= LEASE_LIST_MAX_RETAINED_BYTES_TOTAL
                && session_bytes.saturating_add(incoming_bytes)
                    <= LEASE_LIST_MAX_RETAINED_BYTES_PER_SESSION;
            if fits {
                return true;
            }
            let Some(&oldest_id) = snapshots
                .iter()
                .min_by_key(|(_, snapshot)| snapshot.last_touched_at)
                .map(|(id, _)| id)
            else {
                // Nothing left to evict and still over budget: the
                // incoming snapshot alone exceeds the total bound.
                return false;
            };
            snapshots.remove(&oldest_id);
            crate::observability::counter_inc("fitz_lease_list_snapshot_evictions_total");
        }
    }

    /// Bounds one page by both item count (`page_size`) and encoded byte
    /// size (`LEASE_LIST_PAGE_BYTE_BUDGET`), returning the exclusive end
    /// index into `items`. Always includes at least one item when one is
    /// available, even if it alone exceeds the byte budget — omitting a
    /// match entirely is not an option. In practice no single item can
    /// exceed the budget: `LEASE_MAX_OWNER_ID_BYTES` bounds every variable-
    /// length field this response encodes.
    fn budgeted_page_end(items: &[LeaseListItem], page_size: usize) -> usize {
        let capped = page_size.min(items.len());
        let mut end = 0;
        let mut bytes = 0_usize;
        while end < capped {
            let item_bytes = encoded_item_bytes(&items[end]);
            if end > 0 && bytes.saturating_add(item_bytes) > LEASE_LIST_PAGE_BYTE_BUDGET {
                break;
            }
            bytes = bytes.saturating_add(item_bytes);
            end += 1;
        }
        end
    }

    fn to_list_item(
        &self,
        key: &LeaseKey,
        state: &super::model::SinkLeaseState,
        now: Instant,
    ) -> LeaseListItem {
        LeaseListItem {
            route: key.to_route(),
            owner_id: crate::domains::lease::protocol::logical_owner_id(&state.owner_id)
                .to_string(),
            holder_incarnation: crate::domains::lease::protocol::holder_incarnation(
                &self.core.holder_incarnation_hasher,
                state.owner_session_id,
            ),
            acquired_at: state.acquired_at.clone(),
            expires_in_secs: state.expiry.saturating_duration_since(now).as_secs(),
            renewals: u32::try_from(state.renewals).unwrap_or(u32::MAX),
        }
    }

    /// Removes every `LIST` snapshot owned by `session_id` (disconnect
    /// cleanup — issue #219 §8: an abandoned scan must not outlive the
    /// session that started it).
    pub(super) fn remove_list_snapshots_for_session(&self, session_id: u64) {
        self.core
            .list_snapshots
            .lock()
            .retain(|_, snapshot| snapshot.session_id != session_id);
    }

    /// Removes every `LIST` snapshot idle longer than
    /// `LEASE_LIST_SNAPSHOT_IDLE_TTL_SECS`, as a backstop for a session that
    /// fetches one page and then never returns for the rest without ever
    /// disconnecting.
    pub(super) fn sweep_idle_list_snapshots(&self) {
        let ttl = std::time::Duration::from_secs(
            crate::domains::lease::protocol::LEASE_LIST_SNAPSHOT_IDLE_TTL_SECS,
        );
        let now = Instant::now();
        self.core
            .list_snapshots
            .lock()
            .retain(|_, snapshot| now.saturating_duration_since(snapshot.last_touched_at) < ttl);
    }
}
