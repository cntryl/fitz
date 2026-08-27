use super::model::NoticeDeliveryTarget;
use crate::runtime::routing::{Route, RouteFamily};
use crate::runtime::{DeliveryError, Envelope, RouteError, Router};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Maximum time the global Notice actor waits for one family worker.
pub(super) const NOTICE_DELIVERY_HANDOFF_TIMEOUT: Duration = Duration::from_millis(25);
/// Maximum retry window after a subscriber reports mailbox backpressure.
const NOTICE_MAILBOX_RETRY_TIMEOUT: Duration = Duration::from_millis(5);
const NOTICE_DELIVERY_WORKER_CAPACITY: usize = 64;

pub(super) struct NoticeDeliveryJob {
    target: NoticeDeliveryTarget,
    route: Route,
    payload: Bytes,
    completed: crossbeam_channel::Sender<()>,
}

impl NoticeDeliveryJob {
    pub(super) fn new(
        target: NoticeDeliveryTarget,
        route: Route,
        payload: Bytes,
        completed: crossbeam_channel::Sender<()>,
    ) -> Self {
        Self {
            target,
            route,
            payload,
            completed,
        }
    }
}

pub(super) fn spawn_notice_delivery_worker(
    router: Arc<Router>,
    family: RouteFamily,
) -> std::io::Result<crossbeam_channel::Sender<NoticeDeliveryJob>> {
    let (sender, receiver) =
        crossbeam_channel::bounded::<NoticeDeliveryJob>(NOTICE_DELIVERY_WORKER_CAPACITY);
    std::thread::Builder::new()
        .name(format!("fitz-notice-delivery-{}", family.as_u64()))
        .spawn(move || {
            while let Ok(job) = receiver.recv() {
                deliver_notice(&router, &job.target, &job.route, &job.payload);
                let _ = job.completed.send(());
            }
        })?;
    Ok(sender)
}

pub(super) fn notice_delivery_worker(
    workers: &Mutex<HashMap<RouteFamily, crossbeam_channel::Sender<NoticeDeliveryJob>>>,
    router: &Arc<Router>,
    family: RouteFamily,
) -> Option<crossbeam_channel::Sender<NoticeDeliveryJob>> {
    let mut workers = workers.lock();
    if let Some(worker) = workers.get(&family) {
        return Some(worker.clone());
    }

    match spawn_notice_delivery_worker(router.clone(), family) {
        Ok(worker) => {
            workers.insert(family, worker.clone());
            Some(worker)
        }
        Err(error) => {
            tracing::error!(
                domain = "notice",
                route_family = family.as_u64(),
                error = %error,
                "Notice delivery worker spawn failed"
            );
            None
        }
    }
}

fn deliver_notice(router: &Router, target: &NoticeDeliveryTarget, route: &Route, payload: &Bytes) {
    deliver_with_retry(router, &target.subscriber, || {
        build_notify_envelope(target, route, payload)
    });
}

pub(super) fn deliver_with_retry(
    router: &Router,
    subscriber: &crate::runtime::routing::RouteAddress,
    build_envelope: impl Fn() -> Envelope,
) {
    let deadline = Instant::now() + NOTICE_MAILBOX_RETRY_TIMEOUT;
    let subscriber_sink = router.resolve_sink(subscriber);

    loop {
        let envelope = build_envelope();
        let result = if let Some(sink) = subscriber_sink.as_ref() {
            router.route_to_resolved_sink(envelope, sink)
        } else {
            router.route(envelope)
        };

        match result {
            Ok(()) => return,
            Err(RouteError::DeliveryFailed(_, DeliveryError::MailboxFull { .. }))
                if Instant::now() < deadline =>
            {
                std::thread::yield_now();
            }
            Err(_) => {
                crate::observability::counter_inc(
                    crate::domains::notice::metrics::METRIC_DELIVERY_DROPS_TOTAL,
                );
                return;
            }
        }
    }
}

fn build_notify_envelope(
    target: &NoticeDeliveryTarget,
    route: &Route,
    payload: &Bytes,
) -> Envelope {
    #[cfg(test)]
    let notification = {
        use crate::dispatch::protocol::frame::ChannelId;
        use crate::dispatch::protocol::frame_context::FrameContext;
        use crate::dispatch::protocol::tlv::MessageType;

        let payload = crate::dispatch::protocol::notice_codec::encode_notify(
            target.subscription_id,
            route,
            payload.as_ref(),
        );
        FrameContext::new(
            target.session_id,
            ChannelId::Sub,
            MessageType::new(504),
            payload.into(),
            *target.subscriber.family(),
        )
    };

    #[cfg(not(test))]
    let notification = crate::domains::notice::NoticeClientNotification::new(
        target.session_id,
        *target.subscriber.family(),
        target.subscription_id,
        route.clone(),
        payload.clone(),
    );

    Envelope::new(target.subscriber.clone(), notification)
}
