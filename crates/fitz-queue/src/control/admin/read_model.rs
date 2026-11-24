use super::model::{QueueDeadLetter, QueueInflight, QueueInfo};
use fitz_core::projection::{FamilyRow, FamilyRows};
use parking_lot::{RwLock, RwLockReadGuard};
use std::sync::Arc;

fn matches_realm(realm: Option<&str>, value: &str) -> bool {
    realm.is_none_or(|needle| value == needle)
}

fn collect_slice_matches<T: Clone + FamilyRow>(
    items: &FamilyRows<T>,
    include: impl Fn(&T) -> bool,
) -> Vec<T> {
    items.iter().filter(|item| include(item)).cloned().collect()
}

#[derive(Default)]
pub struct AdminReadModel {
    queues: RwLock<FamilyRows<QueueInfo>>,
    queue_inflight: RwLock<FamilyRows<QueueInflight>>,
    queue_dead_letters: RwLock<FamilyRows<QueueDeadLetter>>,
}

impl AdminReadModel {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_queues(&self, queues: Vec<QueueInfo>) {
        *self.queues.write() = queues.into();
    }

    pub fn queues(&self, realm: Option<&str>) -> Vec<QueueInfo> {
        let queues = self.queues.read();
        collect_slice_matches(&queues, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_queue_inflight(&self, inflight: Vec<QueueInflight>) {
        *self.queue_inflight.write() = inflight.into();
    }

    pub fn queue_inflight(&self, realm: Option<&str>) -> Vec<QueueInflight> {
        let inflight = self.queue_inflight.read();
        collect_slice_matches(&inflight, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_queue_dead_letters(&self, messages: Vec<QueueDeadLetter>) {
        *self.queue_dead_letters.write() = messages.into();
    }

    pub fn queue_dead_letters(&self, realm: Option<&str>) -> Vec<QueueDeadLetter> {
        let messages = self.queue_dead_letters.read();
        collect_slice_matches(&messages, |item| matches_realm(realm, &item.realm))
    }

    pub fn queue_dead_letter(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: u64,
    ) -> Option<QueueDeadLetter> {
        self.queue_dead_letters
            .read()
            .scoped(Some(family))
            .find(|item| {
                item.realm == realm
                    && item.area == area
                    && item.resource == resource
                    && item.message_id == message_id
            })
            .cloned()
    }

    #[doc(hidden)]
    pub fn queues_rows(&self) -> RwLockReadGuard<'_, FamilyRows<QueueInfo>> {
        self.queues.read()
    }

    #[doc(hidden)]
    pub fn queue_inflight_rows(&self) -> RwLockReadGuard<'_, FamilyRows<QueueInflight>> {
        self.queue_inflight.read()
    }

    #[doc(hidden)]
    pub fn queue_dead_letters_rows(&self) -> RwLockReadGuard<'_, FamilyRows<QueueDeadLetter>> {
        self.queue_dead_letters.read()
    }
}
