//! Persist one batch of reservation counters before granting live tokens.

use super::mutation_storage::QueueMutationError;
use super::{
    Bytes, Inflight, QueueActor, QueueCommit, QueueRecord, QueueState, ReadyEntry, ReservedMessage,
};
use crate::domains::queue::QueueResponse;

pub(super) struct PreparedReservation {
    pub(super) ready: ReadyEntry,
    pub(super) record: QueueRecord,
    pub(super) inflight: Inflight,
    pub(super) message: ReservedMessage,
}

impl QueueActor {
    pub(super) fn next_reservation_epoch(&self, id: super::MessageId) -> Option<u64> {
        self.records
            .get(&id)
            .map_or(Some(1), |record| record.inflight_epoch.checked_add(1))
    }

    pub(super) fn prepare_reservation(
        &self,
        ready: ReadyEntry,
        body: Bytes,
        inflight: Inflight,
        inflight_seconds: u64,
        now_ms: u64,
    ) -> PreparedReservation {
        let mut record = self
            .records
            .get(&ready.id)
            .expect("hydrated reservation record")
            .metadata_only_from();
        record.attempts = inflight.attempts;
        record.state = QueueState::Ready;
        record.visible_at_ms = 0;
        record.inflight_epoch = inflight.inflight_epoch;
        record.inflight_token = None;
        record.inflight_expires_at_ms = None;
        record.last_inflight_at_ms = Some(now_ms);
        PreparedReservation {
            ready,
            record,
            message: ReservedMessage {
                id: ready.id,
                body,
                token: inflight.token,
                inflight_seconds,
                attempts: inflight.attempts,
            },
            inflight,
        }
    }

    pub(super) fn commit_reservation_batch(
        &mut self,
        prepared: Vec<PreparedReservation>,
    ) -> Result<Vec<ReservedMessage>, QueueResponse> {
        if prepared.is_empty() {
            return Ok(Vec::new());
        }
        if self.storage_outcome_is_unknown() {
            return Err(Self::unknown_outcome_response());
        }
        if let Err(error) = self.persist_reservation_counters(&prepared) {
            self.restore_prepared_reservations(prepared.into_iter(), false);
            return Err(error.into_response());
        }

        let mut live = Vec::with_capacity(prepared.len());
        let mut pending = prepared.into_iter();
        while let Some(item) = pending.next() {
            let id = item.message.id;
            if self.record_has_expired(&item.record) {
                if let Err(error) = self.remove_expired_prepared_message(id) {
                    self.restore_prepared_reservations(
                        live.into_iter().chain(std::iter::once(item)).chain(pending),
                        true,
                    );
                    return Err(error.into_response());
                }
            } else {
                live.push(item);
            }
        }
        let mut messages = Vec::with_capacity(live.len());
        for item in live {
            let id = item.message.id;
            self.cache_record(id, item.record);
            self.update_cached_inflight_metadata(
                id,
                item.inflight.inflight_epoch,
                Some(item.inflight.token),
                Some(item.inflight.expires_at_epoch_ms),
                self.records
                    .get(&id)
                    .and_then(|record| record.last_inflight_at_ms),
            );
            self.schedule_inflight_expiration(
                id,
                item.inflight.inflight_epoch,
                item.inflight.expires_at,
                item.inflight.expires_at_epoch_ms,
            );
            self.inflight.insert(id, item.inflight);
            self.evict_cached_body(id);
            messages.push(item.message);
        }
        Ok(messages)
    }

    fn restore_prepared_reservations(
        &mut self,
        prepared: impl DoubleEndedIterator<Item = PreparedReservation>,
        counters_committed: bool,
    ) {
        for item in prepared.rev() {
            if counters_committed {
                self.cache_record(item.message.id, item.record);
            }
            self.ready.push_front(item.ready);
        }
        self.refresh_ready_count();
        self.recompute_oldest_ready_enqueued_at_ms();
    }

    fn persist_reservation_counters(
        &self,
        prepared: &[PreparedReservation],
    ) -> Result<(), QueueMutationError> {
        let mut txn = self.persistence.store.begin(
            self.queue_key.family.id(),
            super::recovery_store::QueueTransactionMode::ReadWrite,
        )?;
        for item in prepared {
            txn.put(
                self.cached_header_key(item.message.id),
                Self::encode_record_header(&item.record),
                self.record_ttl_seconds(&item.record),
            )?;
        }
        Self::commit_transaction(txn, QueueCommit::Reserve)?;
        Ok(())
    }
}
