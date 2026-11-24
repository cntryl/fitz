use super::budget::{Budget, Reservation};
use crate::domain::route_grammar::{classify_stream_route_shape, StreamRouteShape};
use fitz_core::domains::subscription_state::record_registration_delta;
use fitz_core::runtime::routing::{route_triplet, RouteAddress};
use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct StreamSubscriptionLimits {
    pub(crate) per_session: NonZeroUsize,
    pub(crate) bytes: NonZeroUsize,
}

impl Default for StreamSubscriptionLimits {
    fn default() -> Self {
        Self {
            per_session: NonZeroUsize::new(10_000).unwrap(),
            bytes: NonZeroUsize::new(128 * 1024 * 1024).unwrap(),
        }
    }
}

impl StreamSubscriptionLimits {
    ///
    /// # Errors
    /// Returns an error if a configured subscription limit is malformed or out of bounds.
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        Ok(Self {
            per_session: positive_env(
                "FITZ_STREAM_MAX_SUBSCRIPTIONS_PER_SESSION",
                defaults.per_session,
            )?,
            bytes: positive_env("FITZ_STREAM_SUBSCRIPTION_BUDGET_BYTES", defaults.bytes)?,
        })
    }
}

fn positive_env(name: &str, default: NonZeroUsize) -> Result<NonZeroUsize, String> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        value => value
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| format!("{name} must be a positive integer")),
    }
}

pub(super) enum RegistrationError {
    Limit,
    IdExhausted,
}

pub(super) struct Subscription {
    pub(super) original: Arc<str>,
    pub(super) canonical: Arc<str>,
    pub(super) session_id: u64,
    pub(super) subscriber: RouteAddress,
    _bytes: Reservation,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        record_registration_delta("stream", false);
    }
}

#[derive(Default)]
struct Bucket {
    ids: BTreeSet<u64>,
}

pub(super) struct SubscriptionRegistry {
    buckets: HashMap<Arc<str>, Bucket>,
    registrations: HashMap<u64, Subscription>,
    sessions: HashMap<u64, HashMap<Arc<str>, u64>>,
    wildcard_buckets: usize,
    next_id: Arc<AtomicU64>,
    bytes: Budget,
    per_session: usize,
}

impl SubscriptionRegistry {
    pub(super) fn new(next_id: Arc<AtomicU64>, bytes: Budget, per_session: usize) -> Self {
        Self {
            buckets: HashMap::new(),
            registrations: HashMap::new(),
            sessions: HashMap::new(),
            wildcard_buckets: 0,
            next_id,
            bytes,
            per_session,
        }
    }

    pub(super) fn subscribe(
        &mut self,
        session_id: u64,
        original: &str,
        shape: StreamRouteShape<'_>,
        subscriber: RouteAddress,
    ) -> Result<(u64, bool), RegistrationError> {
        if let Some(patterns) = self.sessions.get(&session_id) {
            if let Some(&id) = patterns.get(original) {
                return Ok((id, false));
            }
            if patterns.len() >= self.per_session {
                return Err(RegistrationError::Limit);
            }
        }
        let canonical = shape.canonical();
        let bytes = self
            .bytes
            .reserve(512 + original.len() + canonical.len())
            .ok_or(RegistrationError::Limit)?;
        let id = self
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| RegistrationError::IdExhausted)?;

        let canonical = if let Some((key, _)) = self.buckets.get_key_value(canonical.as_str()) {
            key.clone()
        } else {
            self.wildcard_buckets +=
                usize::from(!matches!(shape, StreamRouteShape::Resource { .. }));
            Arc::from(canonical)
        };
        let original: Arc<str> = Arc::from(original);
        self.buckets
            .entry(canonical.clone())
            .or_default()
            .ids
            .insert(id);
        self.sessions
            .entry(session_id)
            .or_default()
            .insert(original.clone(), id);
        self.registrations.insert(
            id,
            Subscription {
                original,
                canonical,
                session_id,
                subscriber,
                _bytes: bytes,
            },
        );
        record_registration_delta("stream", true);
        Ok((id, true))
    }

    pub(super) fn find(&self, session_id: u64, pattern: &str) -> Option<u64> {
        self.sessions.get(&session_id)?.get(pattern).copied()
    }

    pub(super) fn remove(&mut self, id: u64) -> Option<Subscription> {
        let entry = self.registrations.remove(&id)?;
        if let Some(patterns) = self.sessions.get_mut(&entry.session_id) {
            patterns.remove(&entry.original);
            compact(patterns);
            if patterns.is_empty() {
                self.sessions.remove(&entry.session_id);
            }
        }
        self.remove_from_bucket(id, &entry.canonical);
        self.compact();
        Some(entry)
    }

    pub(super) fn remove_session(&mut self, session: u64) -> Vec<(u64, Subscription)> {
        let Some(patterns) = self.sessions.remove(&session) else {
            return Vec::new();
        };
        let removed = patterns
            .into_values()
            .filter_map(|id| {
                let entry = self.registrations.remove(&id)?;
                self.remove_from_bucket(id, &entry.canonical);
                Some((id, entry))
            })
            .collect();
        self.compact();
        removed
    }

    fn remove_from_bucket(&mut self, id: u64, key: &str) {
        let bucket = self
            .buckets
            .get_mut(key)
            .expect("registration has a selector bucket");
        bucket.ids.remove(&id);
        if bucket.ids.is_empty() {
            self.buckets.remove(key);
            self.wildcard_buckets -= usize::from(key.contains('*'));
        }
    }

    fn compact(&mut self) {
        compact(&mut self.registrations);
        compact(&mut self.sessions);
        compact(&mut self.buckets);
    }

    pub(super) fn count(&self) -> usize {
        self.registrations.len()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &Subscription> {
        self.registrations.values()
    }

    pub(super) fn get(&self, id: u64) -> &Subscription {
        &self.registrations[&id]
    }

    pub(super) fn has_wildcards(&self) -> bool {
        self.wildcard_buckets > 0
    }

    pub(super) fn bucket(&self, canonical: &str) -> Option<(&Arc<str>, &BTreeSet<u64>)> {
        self.buckets
            .get_key_value(canonical)
            .map(|(key, bucket)| (key, &bucket.ids))
    }

    pub(super) fn ids(&self, key: &str) -> Option<&BTreeSet<u64>> {
        self.buckets.get(key).map(|bucket| &bucket.ids)
    }

    pub(super) fn matching_count(&self, route: &str) -> usize {
        let mut count = 0;
        if let Some(parts) = route_triplet(route) {
            self.for_each_match(&parts, |_, ids, _| count += ids.len());
        }
        count
    }

    pub(super) fn for_each_match(
        &self,
        parts: &fitz_core::runtime::routing::RouteTriplet<'_>,
        mut visit: impl FnMut(&Arc<str>, &BTreeSet<u64>, StreamRouteShape<'_>),
    ) {
        // The canonical spelling is derived from the same grammar as SUBSCRIBE and READ.
        use StreamRouteShape::{
            Area, Global, GlobalFilterArea, GlobalFilterAreaResource, GlobalFilterResource, Realm,
            RealmFilterResource, Resource,
        };
        let (realm, area, resource) = (parts.realm, parts.area, parts.resource);
        let shapes = [
            Resource {
                realm,
                area,
                resource,
            },
            Area { realm, area },
            RealmFilterResource { realm, resource },
            Realm { realm },
            GlobalFilterAreaResource { area, resource },
            GlobalFilterArea { area },
            GlobalFilterResource { resource },
            Global,
        ];
        let count = if self.has_wildcards() {
            shapes.len()
        } else {
            1
        };
        if self.buckets.is_empty() {
            return;
        }
        let mut key = String::with_capacity(realm.len() + area.len() + resource.len() + 2);
        for shape in &shapes[..count] {
            key.clear();
            shape.write_canonical(&mut key);
            if let Some((canonical, bucket)) = self.buckets.get_key_value(key.as_str()) {
                visit(canonical, &bucket.ids, *shape);
            }
        }
    }
}

pub(super) fn compact<K: std::hash::Hash + Eq, V>(map: &mut HashMap<K, V>) {
    if map.capacity() > 64 && map.capacity() / 4 > map.len() {
        map.shrink_to(map.len() * 2);
    }
}

pub(super) fn validate_selector(route: &str) -> Result<StreamRouteShape<'_>, String> {
    fitz_core::runtime::DomainKind::Stream
        .descriptor()
        .compile_registration_pattern(route)?;
    classify_stream_route_shape(route)
}
