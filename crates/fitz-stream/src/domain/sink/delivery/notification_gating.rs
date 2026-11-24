use super::super::model::{StreamFamilyState, StreamResourceScope};
use super::record_drops;
use crate::domain::protocol::BatchCommitted;
use crate::domain::route_grammar::StreamRouteShape;
use fitz_core::runtime::routing::{route_exact_triplet, route_quad, RouteTriplet};
use fitz_core::runtime::DomainPublishEvent;
use serde::Deserialize;
use std::sync::Arc;

const MAINTENANCE_SCOPES: usize = 16;

#[derive(Clone, Copy, Deserialize)]
pub(super) struct CommitOffsets {
    #[serde(rename = "first_global_offset")]
    pub(super) first_global: u64,
    #[serde(rename = "last_area_offset")]
    last_area: u64,
    #[serde(rename = "last_realm_offset")]
    last_realm: u64,
    #[serde(rename = "last_global_offset")]
    last_global: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PublishedEventKind {
    Committed,
}

#[derive(Deserialize)]
struct PublishedCommit {
    #[serde(rename = "event")]
    _kind: PublishedEventKind,
    #[serde(flatten)]
    offsets: CommitOffsets,
}

impl From<&BatchCommitted> for CommitOffsets {
    fn from(batch: &BatchCommitted) -> Self {
        Self {
            first_global: batch.first_global_offset,
            last_area: batch.last_area_offset,
            last_realm: batch.last_realm_offset,
            last_global: batch.last_global_offset,
        }
    }
}

pub(super) struct CommitEvent {
    pub(super) local_commit: bool,
    pub(super) event: DomainPublishEvent,
    pub(super) resource: StreamResourceScope,
    pub(super) offsets: CommitOffsets,
    #[cfg(feature = "benchkit")]
    pub(super) dispatch_started: Option<std::time::Instant>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum VisibilityScope {
    Area(String, String),
    Realm(String),
    Global,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Frontier {
    Resource,
    Area,
    Realm,
    Global,
}

impl Frontier {
    fn for_selector(selector: StreamRouteShape<'_>) -> Self {
        match selector {
            StreamRouteShape::Resource { .. } => Self::Resource,
            StreamRouteShape::Area { .. } => Self::Area,
            StreamRouteShape::RealmFilterResource { .. } | StreamRouteShape::Realm { .. } => {
                Self::Realm
            }
            _ => Self::Global,
        }
    }

    fn for_commit(self, event: &CommitEvent) -> Option<(VisibilityScope, u64)> {
        match self {
            Self::Resource => None,
            Self::Area => Some((
                VisibilityScope::Area(event.resource.realm.clone(), event.resource.area.clone()),
                event.offsets.last_area,
            )),
            Self::Realm => Some((
                VisibilityScope::Realm(event.resource.realm.clone()),
                event.offsets.last_realm,
            )),
            Self::Global => Some((VisibilityScope::Global, event.offsets.last_global)),
        }
    }
}

impl StreamFamilyState {
    pub(in crate::domain::sink) fn handle_domain_publish(&mut self, event: &DomainPublishEvent) {
        if event.family_id != self.family {
            return;
        }
        let route = event.route.as_str();
        if let Some(parts) = route_exact_triplet(route).filter(|_| !route.contains('*')) {
            if let Some(canonical) = route.strip_prefix("stream://") {
                self.enqueue_commit_notifications(event, canonical, &parts, None);
            }
        } else if let Some(parts) = route_quad(route)
            .filter(|parts| parts.resource == "*" && parts.operation == "watermark")
        {
            if parts.area == "*" {
                self.recheck_parked(&VisibilityScope::Realm(parts.realm.to_owned()));
            } else {
                self.recheck_parked(&VisibilityScope::Area(
                    parts.realm.to_owned(),
                    parts.area.to_owned(),
                ));
            }
            self.recheck_parked(&VisibilityScope::Global);
        }
    }

    pub(in crate::domain::sink) fn handle_committed_batch(
        &mut self,
        event: &DomainPublishEvent,
        batch: &BatchCommitted,
    ) {
        let concrete_key = event
            .route
            .as_str()
            .strip_prefix("stream://")
            .expect("Stream commit scheme");
        let parts = route_exact_triplet(event.route.as_str()).expect("commit has a resource route");
        self.enqueue_commit_notifications(event, concrete_key, &parts, Some(batch.into()));
    }

    fn enqueue_commit_notifications(
        &mut self,
        event: &DomainPublishEvent,
        concrete_key: &str,
        parts: &RouteTriplet<'_>,
        local_offsets: Option<CommitOffsets>,
    ) {
        #[cfg(feature = "benchkit")]
        let dispatch_started = crate::stream_dispatch::start();
        let mut matches = Vec::new();
        let mut add_match =
            |selector: &Arc<str>, ids: &std::collections::BTreeSet<u64>, frontier| {
                let cutoff = *ids.last().expect("empty registration buckets are removed");
                matches.push((selector.clone(), cutoff, ids.len(), frontier));
            };
        if self.subscriptions.has_wildcards() {
            self.subscriptions
                .for_each_match(parts, |selector, ids, shape| {
                    add_match(selector, ids, Frontier::for_selector(shape));
                });
        } else if let Some((selector, ids)) = self.subscriptions.bucket(concrete_key) {
            add_match(selector, ids, Frontier::Resource);
        }
        let has_parked = self.delivery.has_parked_work();
        // The successful local write advanced both scope counters atomically
        // with the records and postings. Global visibility has its own fence.
        let area = (has_parked || matches.iter().any(|entry| entry.3 == Frontier::Area))
            .then(|| {
                self.availability(
                    &VisibilityScope::Area(parts.realm.to_owned(), parts.area.to_owned()),
                    &matches,
                    Frontier::Area,
                    local_offsets.map(|offsets| offsets.last_area),
                )
            })
            .flatten();
        let realm = (has_parked || matches.iter().any(|entry| entry.3 == Frontier::Realm))
            .then(|| {
                self.availability(
                    &VisibilityScope::Realm(parts.realm.to_owned()),
                    &matches,
                    Frontier::Realm,
                    local_offsets.map(|offsets| offsets.last_realm),
                )
            })
            .flatten();
        let global = self.availability(&VisibilityScope::Global, &matches, Frontier::Global, None);
        if matches.is_empty() {
            return;
        }

        let offsets = local_offsets.or_else(|| {
            serde_json::from_slice::<PublishedCommit>(&event.payload)
                .ok()
                .map(|commit| commit.offsets)
        });
        let Some(offsets) = offsets else {
            record_drops(matches.iter().map(|(_, _, count, _)| count).sum());
            return;
        };
        let event = Arc::new(CommitEvent {
            local_commit: local_offsets.is_some(),
            event: event.clone(),
            offsets,
            #[cfg(feature = "benchkit")]
            dispatch_started,
            resource: StreamResourceScope {
                family: event.family_id,
                realm: parts.realm.to_owned(),
                area: parts.area.to_owned(),
                resource: parts.resource.to_owned(),
            },
        });
        for (selector, cutoff, count, frontier) in matches {
            let watermark = match frontier {
                Frontier::Resource => None,
                Frontier::Area => area,
                Frontier::Realm => realm,
                Frontier::Global => global,
            };
            let parked = frontier
                .for_commit(&event)
                .filter(|(_, offset)| watermark.is_none_or(|visible| *offset > visible));
            if !self
                .delivery
                .enqueue(event.clone(), selector, cutoff, count, parked)
            {
                record_drops(count);
            }
        }
    }

    fn availability(
        &mut self,
        scope: &VisibilityScope,
        matches: &[(Arc<str>, u64, usize, Frontier)],
        frontier: Frontier,
        committed_through: Option<u64>,
    ) -> Option<u64> {
        if !self.delivery.has_parked(scope) && !matches.iter().any(|entry| entry.3 == frontier) {
            return None;
        }
        let watermark = committed_through.or_else(|| self.visible_through(scope))?;
        self.delivery.promote(scope, watermark);
        Some(watermark)
    }

    fn visible_through(&mut self, scope: &VisibilityScope) -> Option<u64> {
        let family = self.family.as_u64();
        match scope {
            VisibilityScope::Area(realm, area) => {
                self.stream_store.get_watermark(family, realm, area).ok()
            }
            VisibilityScope::Realm(realm) => {
                self.stream_store.get_realm_watermark(family, realm).ok()
            }
            VisibilityScope::Global => self
                .stream_store
                .get_global_watermark(family)
                .ok()?
                .checked_sub(1),
        }
    }

    fn recheck_parked(&mut self, scope: &VisibilityScope) {
        if self.delivery.has_parked(scope) {
            if let Some(watermark) = self.visible_through(scope) {
                self.delivery.promote(scope, watermark);
            }
        }
    }

    pub(in crate::domain::sink) fn recheck_commit_scopes(&mut self, realm: &str, area: &str) {
        self.recheck_parked(&VisibilityScope::Area(realm.to_owned(), area.to_owned()));
        self.recheck_parked(&VisibilityScope::Realm(realm.to_owned()));
        self.recheck_parked(&VisibilityScope::Global);
    }

    pub(in crate::domain::sink) fn recheck_parked_slice(&mut self) {
        for scope in self.delivery.parked_scopes(MAINTENANCE_SCOPES) {
            self.recheck_parked(&scope);
        }
    }

    pub(in crate::domain::sink) fn discard_drop_notifications(
        &mut self,
        scope: &crate::domain::StreamDropScope,
    ) {
        record_drops(self.delivery.discard_where(|event| {
            let key = &event.resource;
            scope.matches_parts(key.family, &key.realm, &key.area, &key.resource)
        }));
        self.recheck_parked(&VisibilityScope::Global);
    }
}
