//! Family-indexed projections. Family selection precedes cloning or scan admission.
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
pub type ResourceIdentity = (u64, String, String, String);

pub trait FamilyRow {
    fn family(&self) -> u64;
    fn inventory_identity(&self) -> Option<ResourceIdentity>;
    fn matches_resource(&self, realm: &str, area: &str, resource: &str) -> bool {
        self.inventory_identity()
            .is_some_and(|key| key.1 == realm && key.2 == area && key.3 == resource)
    }
}

pub struct FamilyRows<T> {
    // Both indices reference the same immutable projection row; payloads are never duplicated.
    families: BTreeMap<u64, Vec<Arc<T>>>,
    resources: BTreeMap<ResourceIdentity, Vec<Arc<T>>>,
    patterns: BTreeMap<u64, Vec<Arc<T>>>,
    #[cfg(test)]
    retention_index_visits: usize,
}

impl<T> Default for FamilyRows<T> {
    fn default() -> Self {
        Self {
            families: BTreeMap::new(),
            resources: BTreeMap::new(),
            patterns: BTreeMap::new(),
            #[cfg(test)]
            retention_index_visits: 0,
        }
    }
}

impl<T: FamilyRow> From<Vec<T>> for FamilyRows<T> {
    fn from(items: Vec<T>) -> Self {
        let mut rows = Self::default();
        rows.extend(items);
        rows
    }
}

impl<T: FamilyRow> FamilyRows<T> {
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.families.values().flatten().map(Arc::as_ref)
    }

    pub fn replace_matching(&mut self, item: T, matches: impl Fn(&T) -> bool) {
        self.retain(|existing| !matches(existing));
        self.push(item);
    }

    pub fn len(&self) -> usize {
        self.families.values().map(Vec::len).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.families.is_empty()
    }

    pub fn push(&mut self, item: T) {
        let item = Arc::new(item);
        if let Some(identity) = item.inventory_identity().filter(|key| {
            ![&key.1, &key.2, &key.3]
                .iter()
                .any(|part| part.contains('*'))
        }) {
            self.resources
                .entry(identity)
                .or_default()
                .push(Arc::clone(&item));
        } else {
            self.patterns
                .entry(item.family())
                .or_default()
                .push(Arc::clone(&item));
        }
        self.families.entry(item.family()).or_default().push(item);
    }

    pub fn extend(&mut self, items: impl IntoIterator<Item = T>) {
        for item in items {
            self.push(item);
        }
    }

    pub fn retain(&mut self, mut include: impl FnMut(&T) -> bool) {
        #[cfg(test)]
        {
            self.retention_index_visits = 0;
        }
        // These pointer identities are only compared and never dereferenced.
        // The secondary indices own the removed rows until cleanup completes.
        let mut removed = HashSet::new();
        self.families.retain(|_, items| {
            items.retain(|item| {
                let keep = include(item);
                if !keep {
                    removed.insert(Arc::as_ptr(item));
                }
                keep
            });
            !items.is_empty()
        });
        if removed.is_empty() {
            return;
        }
        for rows in self
            .resources
            .values_mut()
            .chain(self.patterns.values_mut())
        {
            rows.retain(|row| {
                #[cfg(test)]
                {
                    self.retention_index_visits += 1;
                }
                !removed.contains(&Arc::as_ptr(row))
            });
        }
        self.resources.retain(|_, rows| !rows.is_empty());
        self.patterns.retain(|_, rows| !rows.is_empty());
    }

    pub fn resource_page(
        &self,
        start: ResourceIdentity,
        includes: impl Fn(&ResourceIdentity) -> bool,
        after: Option<&ResourceIdentity>,
        limit: usize,
    ) -> Vec<ResourceIdentity> {
        let start = after.cloned().unwrap_or(start);
        self.resources
            .range(start..)
            .take_while(|(key, _)| includes(key))
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub fn scoped(&self, family: Option<u64>) -> impl Iterator<Item = &T> {
        let (start, end) = family.map_or((0, u64::MAX), |value| (value, value));
        self.families
            .range(start..=end)
            .flat_map(|(_, items)| items)
            .map(Arc::as_ref)
    }

    pub fn resource_snapshot(
        &self,
        identity: &ResourceIdentity,
        limit: usize,
        truncated: &mut bool,
    ) -> Vec<T>
    where
        T: Clone,
    {
        let mut exact = self.resources.get(identity).into_iter().flatten();
        let mut result = exact
            .by_ref()
            .take(limit)
            .map(|row| row.as_ref().clone())
            .collect::<Vec<_>>();
        *truncated |= exact.next().is_some();
        // Wildcard registrations cannot be indexed by one concrete resource. Bound their
        // inspection separately and report unknown completeness if that scan is exhausted.
        let mut patterns = self.patterns.get(&identity.0).into_iter().flatten();
        for row in patterns.by_ref().take(limit) {
            if row.matches_resource(&identity.1, &identity.2, &identity.3) {
                if result.len() == limit {
                    *truncated = true;
                    break;
                }
                result.push(row.as_ref().clone());
            }
        }
        *truncated |= patterns.next().is_some();
        result
    }
}

#[cfg(test)]
#[path = "projection/tests.rs"]
mod tests;
