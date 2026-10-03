//! Warm Queue actors and their bounded idle-sweep rotation.

use super::model::WarmQueueActor;
use crate::domains::queue::{QueueActor, QueueKey};
use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

#[derive(Default)]
pub(super) struct QueueActorRegistry {
    actors: HashMap<QueueKey, WarmQueueActor>,
    idle_sweep_keys: VecDeque<QueueKey>,
}

impl QueueActorRegistry {
    pub(super) fn with_actor<R>(
        &mut self,
        key: &QueueKey,
        store: &crate::domains::queue::actor::recovery_store::QueueStore,
        dedup_store: &Arc<crate::utils::idempotency::DedupStore>,
        write_policy: crate::domains::WritePolicy,
        operation: impl FnOnce(&mut QueueActor) -> R,
    ) -> Result<(R, bool), String> {
        let now = Instant::now();
        match self.actors.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().last_used = now;
                Ok((operation(&mut entry.get_mut().actor), false))
            }
            Entry::Vacant(entry) => {
                let actor = QueueActor::try_new_with_write_policy(
                    key.family,
                    key.clone(),
                    store.clone(),
                    None,
                    dedup_store.clone(),
                    write_policy,
                )?;
                let warm_actor = entry.insert(WarmQueueActor {
                    actor,
                    last_used: now,
                });
                self.idle_sweep_keys.push_back(key.clone());
                Ok((operation(&mut warm_actor.actor), true))
            }
        }
    }

    pub(super) fn take_idle_sweep_batch(&mut self, limit: usize) -> Vec<QueueKey> {
        let count = self.idle_sweep_keys.len().min(limit);
        self.idle_sweep_keys.drain(..count).collect()
    }

    pub(super) fn requeue_idle_key(&mut self, key: QueueKey) {
        self.idle_sweep_keys.push_back(key);
    }

    pub(super) fn get_mut(&mut self, key: &QueueKey) -> Option<&mut WarmQueueActor> {
        self.actors.get_mut(key)
    }

    pub(super) fn remove(&mut self, key: &QueueKey) -> Option<WarmQueueActor> {
        self.actors.remove(key)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&QueueKey, &WarmQueueActor)> {
        self.actors.iter()
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&QueueKey, &mut WarmQueueActor)> {
        self.actors.iter_mut()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &WarmQueueActor> {
        self.actors.values()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.actors.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }

    #[cfg(test)]
    pub(super) fn insert_for_tests(&mut self, key: QueueKey, actor: QueueActor) {
        self.actors.insert(
            key.clone(),
            WarmQueueActor {
                actor,
                last_used: Instant::now(),
            },
        );
        self.idle_sweep_keys.push_back(key);
    }

    #[cfg(test)]
    pub(super) fn age_all_for_tests(&mut self, last_used: Instant) {
        for actor in self.actors.values_mut() {
            actor.last_used = last_used;
        }
    }
}
