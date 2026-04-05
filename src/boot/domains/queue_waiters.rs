use crate::domains::queue::{QueueKey, QueueResponse, MAX_WAIT_QUEUE_DEPTH, MAX_WAIT_SECONDS};
use crate::protocol::frame::ChannelId;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::Envelope;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(super) struct PendingReceive {
    pub waiter_id: u64,
    pub session_id: u64,
    pub reply_source: crate::runtime::routing::RouteAddress,
    pub reply_destination: crate::runtime::routing::RouteAddress,
    pub channel_id: ChannelId,
    pub route_family: crate::runtime::routing::RouteFamily,
    pub msg_type: u16,
    pub lease_seconds: u64,
    pub batch_size: Option<usize>,
    pub requested_at: Instant,
    pub expires_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PendingReceiveRef {
    key: QueueKey,
    waiter_id: u64,
}

pub(super) struct QueueWaiterRegistry {
    pending_receives: Mutex<HashMap<QueueKey, VecDeque<PendingReceive>>>,
    session_waiters: Mutex<HashMap<u64, HashSet<PendingReceiveRef>>>,
    next_waiter_id: AtomicU64,
}

impl QueueWaiterRegistry {
    pub fn new() -> Self {
        Self {
            pending_receives: Mutex::new(HashMap::new()),
            session_waiters: Mutex::new(HashMap::new()),
            next_waiter_id: AtomicU64::new(1),
        }
    }

    pub fn enqueue(
        &self,
        key: &QueueKey,
        request_envelope: &Envelope,
        frame_ctx: &FrameContext,
        lease_seconds: u64,
        batch_size: Option<usize>,
        wait_seconds: u64,
    ) -> Result<(), QueueResponse> {
        if wait_seconds > MAX_WAIT_SECONDS {
            return Err(QueueResponse::BadRequest {
                reason: format!("queue wait_seconds exceeds max {}", MAX_WAIT_SECONDS),
            });
        }

        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let reply_destination = request_envelope.source().cloned().unwrap_or_else(|| {
            crate::runtime::routing::session_inbox_address(
                frame_ctx.route_family,
                frame_ctx.session_id,
            )
        });

        let mut pending_receives = self.pending_receives.lock();
        let queue = pending_receives.entry(key.clone()).or_default();
        if queue.len() >= MAX_WAIT_QUEUE_DEPTH {
            return Err(QueueResponse::BadRequest {
                reason: format!("queue wait depth exceeded ({})", MAX_WAIT_QUEUE_DEPTH),
            });
        }

        let requested_at = Instant::now();

        queue.push_back(PendingReceive {
            waiter_id,
            session_id: frame_ctx.session_id,
            reply_source: request_envelope.destination().clone(),
            reply_destination,
            channel_id: frame_ctx.channel_id,
            route_family: frame_ctx.route_family,
            msg_type: frame_ctx.msg_type.as_u16(),
            lease_seconds,
            batch_size,
            requested_at,
            expires_at: requested_at + Duration::from_secs(wait_seconds),
        });
        drop(pending_receives);

        self.track_session_waiter(frame_ctx.session_id, key, waiter_id);
        Ok(())
    }

    pub fn complete(&self, key: &QueueKey, waiter: &PendingReceive) {
        self.untrack_session_waiter(waiter.session_id, key, waiter.waiter_id);
    }

    pub fn remove_session_waiters(&self, session_id: u64) -> usize {
        let waiter_refs = self
            .session_waiters
            .lock()
            .remove(&session_id)
            .map(|waiters| waiters.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();

        if waiter_refs.is_empty() {
            return 0;
        }

        let mut removed = 0;
        let mut pending_receives = self.pending_receives.lock();
        for waiter_ref in waiter_refs {
            let mut should_remove_queue = false;
            if let Some(queue) = pending_receives.get_mut(&waiter_ref.key) {
                let before = queue.len();
                queue.retain(|waiter| waiter.waiter_id != waiter_ref.waiter_id);
                removed += before.saturating_sub(queue.len());
                should_remove_queue = queue.is_empty();
            }
            if should_remove_queue {
                pending_receives.remove(&waiter_ref.key);
            }
        }

        removed
    }

    pub fn expire_timed_out_for_key(&self, key: &QueueKey, now: Instant) -> Vec<PendingReceive> {
        let expired_waiters = {
            let mut pending_receives = self.pending_receives.lock();
            let mut expired = Vec::new();
            let mut should_remove_queue = false;

            if let Some(queue) = pending_receives.get_mut(key) {
                let mut index = 0;
                while index < queue.len() {
                    if queue[index].expires_at <= now {
                        expired.push(queue.remove(index).expect("waiter removal"));
                    } else {
                        index += 1;
                    }
                }
                should_remove_queue = queue.is_empty();
            }

            if should_remove_queue {
                pending_receives.remove(key);
            }

            expired
        };

        for waiter in &expired_waiters {
            self.untrack_session_waiter(waiter.session_id, key, waiter.waiter_id);
        }

        expired_waiters
    }

    pub fn pop_next_for_key(&self, key: &QueueKey) -> Option<PendingReceive> {
        let mut pending_receives = self.pending_receives.lock();
        let mut should_remove_queue = false;
        let waiter = if let Some(queue) = pending_receives.get_mut(key) {
            let waiter = queue.pop_front();
            should_remove_queue = queue.is_empty();
            waiter
        } else {
            None
        };

        if should_remove_queue {
            pending_receives.remove(key);
        }

        waiter
    }

    pub fn requeue_front(&self, key: &QueueKey, waiter: PendingReceive) {
        self.pending_receives
            .lock()
            .entry(key.clone())
            .or_default()
            .push_front(waiter);
    }

    pub fn keys(&self) -> Vec<QueueKey> {
        self.pending_receives.lock().keys().cloned().collect()
    }

    fn track_session_waiter(&self, session_id: u64, key: &QueueKey, waiter_id: u64) {
        self.session_waiters
            .lock()
            .entry(session_id)
            .or_default()
            .insert(PendingReceiveRef {
                key: key.clone(),
                waiter_id,
            });
    }

    fn untrack_session_waiter(&self, session_id: u64, key: &QueueKey, waiter_id: u64) {
        let mut session_waiters = self.session_waiters.lock();
        let should_remove_session = if let Some(waiters) = session_waiters.get_mut(&session_id) {
            waiters.remove(&PendingReceiveRef {
                key: key.clone(),
                waiter_id,
            });
            waiters.is_empty()
        } else {
            false
        };

        if should_remove_session {
            session_waiters.remove(&session_id);
        }
    }
}