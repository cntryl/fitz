use super::{OperationOutcome, QueueDomainCore};
use crate::runtime::routing::RouteFamily;
use std::sync::atomic::Ordering;

use crate::domains::queue::protocol::{
    MAX_QUEUE_RESPONSE_PAYLOAD_BYTES, RECEIVED_RESPONSE_HEADER_BYTES,
    RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES, ROUTED_MESSAGE_WIRE_OVERHEAD_BYTES,
};

fn receive_with_route_wire_budget(
    actor: &mut crate::domains::queue::QueueActor,
    session_id: u64,
    inflight_seconds: u64,
    batch_size: usize,
    response_bytes_remaining: &mut usize,
    route: &crate::runtime::routing::Route,
) -> crate::domains::queue::QueueResponse {
    let message_wire_overhead_bytes = RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES
        .saturating_add(ROUTED_MESSAGE_WIRE_OVERHEAD_BYTES)
        .saturating_add(route.as_str().len());
    actor.handle_receive_for_session_with_wire_budget(
        session_id,
        inflight_seconds,
        Some(batch_size),
        response_bytes_remaining,
        message_wire_overhead_bytes,
    )
}

fn empty_wildcard_receive_outcome() -> OperationOutcome {
    OperationOutcome {
        response: crate::domains::queue::QueueResponse::ReceivedRouted {
            messages: Vec::new(),
        },
        ready_notifications: Vec::new(),
        mark_admin_snapshot_dirty: false,
    }
}

impl QueueDomainCore {
    const MAX_WILDCARD_RESERVE_MATCHES: usize = 4096;

    fn wildcard_inventory_error(&self) -> Option<OperationOutcome> {
        self.inventory_error
            .lock()
            .clone()
            .map(|error| OperationOutcome {
                response: crate::domains::queue::QueueResponse::Error {
                    message: format!("Queue inventory unavailable: {error}"),
                },
                ready_notifications: Vec::new(),
                mark_admin_snapshot_dirty: false,
            })
    }

    fn wildcard_reserve_preflight(
        &self,
        family_id: RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
    ) -> Option<OperationOutcome> {
        if let Some(outcome) = self.wildcard_inventory_error() {
            return Some(outcome);
        }
        if self.matching_queue_key_count(family_id, pattern) > Self::MAX_WILDCARD_RESERVE_MATCHES {
            return Some(OperationOutcome {
                response: crate::domains::queue::QueueResponse::Error {
                    message: "wildcard reserve matched too many queues".to_string(),
                },
                ready_notifications: Vec::new(),
                mark_admin_snapshot_dirty: false,
            });
        }
        None
    }

    pub(super) fn handle_wildcard_receive(
        &self,
        family_id: RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
        session_id: u64,
        inflight_seconds: u64,
        batch_size: Option<usize>,
    ) -> OperationOutcome {
        if let Some(outcome) = self.wildcard_reserve_preflight(family_id, pattern) {
            return outcome;
        }

        let keys = self.matching_queue_keys(family_id, pattern);
        let limit = batch_size.unwrap_or(1);
        if keys.is_empty() || limit == 0 {
            return empty_wildcard_receive_outcome();
        }

        let start = usize::try_from(
            self.wildcard_reserve_sequence
                .fetch_add(1, Ordering::Relaxed),
        )
        .unwrap_or(0)
            % keys.len();
        let mut routed = Vec::with_capacity(limit);
        let mut notifications = Vec::new();
        let mut state_changed = false;
        let mut response_bytes_remaining =
            MAX_QUEUE_RESPONSE_PAYLOAD_BYTES - RECEIVED_RESPONSE_HEADER_BYTES;

        for offset in 0..keys.len() {
            if routed.len() == limit {
                break;
            }
            let key = &keys[(start + offset) % keys.len()];
            let route = Self::queue_ready_route(key);
            let (actor_handle, _) = match self.get_or_create_actor(key) {
                Ok(actor) => actor,
                Err(message) if routed.is_empty() => {
                    return OperationOutcome {
                        response: crate::domains::queue::QueueResponse::Error { message },
                        ready_notifications: notifications,
                        mark_admin_snapshot_dirty: state_changed,
                    };
                }
                Err(error) => {
                    tracing::warn!(
                        domain = "queue",
                        route = %route,
                        error = %error,
                        "Wildcard reserve returned a partial batch after queue recovery failed"
                    );
                    break;
                }
            };
            let response = {
                let mut actor = actor_handle.lock();
                state_changed |= actor.process_due_work();
                let remaining = limit - routed.len();
                let response = receive_with_route_wire_budget(
                    &mut actor,
                    session_id,
                    inflight_seconds,
                    remaining,
                    &mut response_bytes_remaining,
                    &route,
                );
                let counts = actor.live_counts();
                if counts.total() > 0 {
                    self.known_queue_keys.lock().insert(key.clone());
                }
                if let Some(notification) = self.record_ready_state(key, counts) {
                    notifications.push((key.clone(), notification));
                }
                response
            };
            match response {
                crate::domains::queue::QueueResponse::Received { messages } => {
                    routed.extend(messages.into_iter().map(|message| {
                        crate::domains::queue::RoutedReservedMessage {
                            route: route.clone(),
                            message,
                        }
                    }));
                }
                error if routed.is_empty() => {
                    return OperationOutcome {
                        response: error,
                        ready_notifications: notifications,
                        mark_admin_snapshot_dirty: state_changed,
                    };
                }
                error => {
                    tracing::warn!(
                        domain = "queue",
                        route = %route,
                        error = ?error,
                        "Wildcard reserve returned a partial batch after a queue error"
                    );
                    break;
                }
            }
        }

        OperationOutcome {
            mark_admin_snapshot_dirty: state_changed || !routed.is_empty(),
            response: crate::domains::queue::QueueResponse::ReceivedRouted { messages: routed },
            ready_notifications: notifications,
        }
    }
}
