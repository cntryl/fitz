use super::{
    Instant, OperationOutcome, PendingQueueReserve, QueueDomainCore, QueueOpKind, VecDeque,
};
use crate::runtime::routing::RouteFamily;

impl QueueDomainCore {
    fn pending_reserve_matches_route(
        message: &crate::domains::queue::protocol::QueueMessage,
        family: RouteFamily,
        concrete_route: &crate::runtime::routing::Route,
    ) -> bool {
        let crate::domains::queue::protocol::QueueMessage::Receive {
            family_id, route, ..
        } = message
        else {
            return false;
        };
        if *family_id != family {
            return false;
        }
        if !route.as_str().contains('*') {
            return route == concrete_route;
        }
        Self::wildcard_queue_selector(route).is_ok_and(|pattern| pattern.matches(concrete_route))
    }

    fn empty_reserve_response(
        message: &crate::domains::queue::protocol::QueueMessage,
    ) -> crate::domains::queue::QueueResponse {
        if matches!(
            message,
            crate::domains::queue::protocol::QueueMessage::Receive { route, .. }
                if route.as_str().contains('*')
        ) {
            crate::domains::queue::QueueResponse::ReceivedRouted {
                messages: Vec::new(),
            }
        } else {
            crate::domains::queue::QueueResponse::Received {
                messages: Vec::new(),
            }
        }
    }

    fn finish_pending_reserve(&self, pending: &PendingQueueReserve, outcome: OperationOutcome) {
        if outcome.mark_admin_snapshot_dirty {
            self.mark_admin_snapshot_dirty();
            if let Some(family_id) = Self::queue_message_family(&pending.message) {
                self.mark_fast_flush_dirty(family_id);
            }
        }
        for (key, notification) in outcome.ready_notifications {
            self.route_queue_ready_notification(&key, notification);
        }
        self.route_queue_response(&pending.envelope, pending.meta, &outcome.response);
        self.record_operation_metrics(
            pending.request_started,
            &outcome.response,
            QueueOpKind::Receive,
        );
    }

    pub(in crate::domains::queue::sink) fn wake_pending_reserves_for_route(
        &self,
        family: RouteFamily,
        concrete_route: &crate::runtime::routing::Route,
        now: Instant,
    ) {
        let mut pending = std::mem::take(&mut *self.pending_reserves.lock());
        let mut still_waiting = VecDeque::new();
        while let Some(reserve) = pending.pop_front() {
            if reserve.deadline <= now {
                let response = Self::empty_reserve_response(&reserve.message);
                self.route_queue_response(&reserve.envelope, reserve.meta, &response);
                self.record_operation_metrics(
                    reserve.request_started,
                    &response,
                    QueueOpKind::Receive,
                );
                continue;
            }
            if !Self::pending_reserve_matches_route(&reserve.message, family, concrete_route) {
                still_waiting.push_back(reserve);
                continue;
            }
            let Some(outcome) = self.dispatch_actor_operation(
                &reserve.envelope,
                reserve.meta,
                reserve.request_started,
                reserve.message.clone(),
            ) else {
                continue;
            };
            let empty = matches!(
                &outcome.response,
                crate::domains::queue::QueueResponse::Received { messages } if messages.is_empty()
            ) || matches!(
                &outcome.response,
                crate::domains::queue::QueueResponse::ReceivedRouted { messages } if messages.is_empty()
            );
            if empty {
                still_waiting.push_back(reserve);
            } else {
                self.finish_pending_reserve(&reserve, outcome);
            }
        }
        self.pending_reserves.lock().append(&mut still_waiting);
    }

    pub(in crate::domains::queue::sink) fn expire_pending_reserves_at(&self, now: Instant) {
        let mut pending = std::mem::take(&mut *self.pending_reserves.lock());
        let mut still_waiting = VecDeque::new();
        while let Some(reserve) = pending.pop_front() {
            if reserve.deadline > now {
                still_waiting.push_back(reserve);
                continue;
            }
            let response = Self::empty_reserve_response(&reserve.message);
            self.route_queue_response(&reserve.envelope, reserve.meta, &response);
            self.record_operation_metrics(reserve.request_started, &response, QueueOpKind::Receive);
        }
        self.pending_reserves.lock().append(&mut still_waiting);
    }
}
