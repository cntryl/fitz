//! Returning session-owned inflight reservations to ready work.
//!
//! Inflight ownership is ephemeral broker-local state and is not durably
//! recovered; releasing it makes the accepted message deliverable again.

use super::{MessageId, QueueActor, QueueState};

impl QueueActor {
    /// Drop any live inflight entries owned by a disconnected session and return the
    /// accepted messages to the ready queue.
    pub fn cleanup_session_inflight(&mut self, session_id: u64) -> usize {
        let released: Vec<_> = self
            .inflight
            .iter()
            .filter_map(|(id, inflight)| {
                (inflight.owner_session_id == Some(session_id)).then_some(*id)
            })
            .collect();

        for id in released.iter().copied() {
            self.return_inflight_to_ready(id);
        }

        released.len()
    }

    /// Return one reservation to ready work only when its session and token
    /// still identify the exact delivery that could not reach the client.
    pub fn release_undelivered_reservation(
        &mut self,
        session_id: u64,
        id: MessageId,
        token: u64,
    ) -> bool {
        if !self.inflight.get(&id).is_some_and(|inflight| {
            inflight.owner_session_id == Some(session_id) && inflight.token == token
        }) {
            return false;
        }
        self.return_inflight_to_ready(id);
        true
    }

    fn return_inflight_to_ready(&mut self, id: MessageId) {
        self.inflight.remove(&id);
        if let Some(record) = self.records.get_mut(&id) {
            record.state = QueueState::Ready;
            record.visible_at_ms = 0;
            record.inflight_token = None;
            record.inflight_expires_at_ms = None;
        }
        self.push_ready(id);
    }
}
