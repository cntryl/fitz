use super::model::{NoticeDeliveryTarget, NoticeDeliveryTargets};
use crate::runtime::routing::{Route, RouteFamily};
use crate::runtime::{DeliveryError, Envelope, RouteError, Router};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Maximum retry window after a subscriber reports mailbox backpressure.
const NOTICE_MAILBOX_RETRY_TIMEOUT: Duration = Duration::from_millis(5);
const NOTICE_DELIVERY_WORKER_CAPACITY: usize = 64;

pub(super) struct NoticeDeliveryJob {
    targets: NoticeDeliveryTargets,
    route: Route,
    payload: Bytes,
}

impl NoticeDeliveryJob {
    pub(super) fn new(targets: NoticeDeliveryTargets, route: Route, payload: Bytes) -> Self {
        Self {
            targets,
            route,
            payload,
        }
    }
}

pub(super) fn spawn_notice_delivery_worker(
    router: Arc<Router>,
    family: RouteFamily,
    metrics: Option<crate::domains::notice::metrics::NoticeMetrics>,
) -> std::io::Result<crossbeam_channel::Sender<NoticeDeliveryJob>> {
    let (sender, receiver) =
        crossbeam_channel::bounded::<NoticeDeliveryJob>(NOTICE_DELIVERY_WORKER_CAPACITY);
    std::thread::Builder::new()
        .name(format!("fitz-notice-delivery-{}", family.as_u64()))
        .spawn(move || {
            while let Ok(job) = receiver.recv() {
                for target in &job.targets {
                    deliver_notice(&router, target, &job.route, &job.payload, metrics.as_ref());
                }
            }
        })?;
    Ok(sender)
}

pub(super) fn notice_delivery_worker(
    workers: &mut HashMap<RouteFamily, crossbeam_channel::Sender<NoticeDeliveryJob>>,
    router: &Arc<Router>,
    family: RouteFamily,
    metrics: Option<&crate::domains::notice::metrics::NoticeMetrics>,
) -> Option<crossbeam_channel::Sender<NoticeDeliveryJob>> {
    if let Some(worker) = workers.get(&family) {
        return Some(worker.clone());
    }

    match spawn_notice_delivery_worker(router.clone(), family, metrics.cloned()) {
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

/// Record a dropped delivery on the sink's own collector when one was injected,
/// and on the process-global collector otherwise.
///
/// A sink given its own collector must see every one of its drops there. Writing
/// only to the global collector made an injected collector silently incomplete,
/// and made any assertion on the global count depend on what every other live
/// Notice sink in the process happened to be doing.
pub(super) fn record_delivery_drop(
    metrics: Option<&crate::domains::notice::metrics::NoticeMetrics>,
) {
    match metrics {
        Some(metrics) => metrics.record_delivery_drop(),
        None => crate::observability::counter_inc(
            crate::domains::notice::metrics::METRIC_DELIVERY_DROPS_TOTAL,
        ),
    }
}

fn deliver_notice(
    router: &Router,
    target: &NoticeDeliveryTarget,
    route: &Route,
    payload: &Bytes,
    metrics: Option<&crate::domains::notice::metrics::NoticeMetrics>,
) {
    deliver_with_retry(router, &target.subscriber, metrics, || {
        build_notify_envelope(target, route, payload)
    });
}

pub(super) fn deliver_with_retry(
    router: &Router,
    subscriber: &crate::runtime::routing::RouteAddress,
    metrics: Option<&crate::domains::notice::metrics::NoticeMetrics>,
    build_envelope: impl Fn() -> Envelope,
) {
    deliver_with_retry_for(
        router,
        subscriber,
        NOTICE_MAILBOX_RETRY_TIMEOUT,
        metrics,
        build_envelope,
    );
}

#[cfg(test)]
pub(super) fn deliver_with_retry_for_test(
    router: &Router,
    subscriber: &crate::runtime::routing::RouteAddress,
    timeout: Duration,
    build_envelope: impl Fn() -> Envelope,
) {
    deliver_with_retry_for(router, subscriber, timeout, None, build_envelope);
}

fn deliver_with_retry_for(
    router: &Router,
    subscriber: &crate::runtime::routing::RouteAddress,
    timeout: Duration,
    metrics: Option<&crate::domains::notice::metrics::NoticeMetrics>,
    build_envelope: impl Fn() -> Envelope,
) {
    let deadline = Instant::now() + timeout;
    let mut attempts = 0_u32;

    loop {
        attempts += 1;
        let envelope = build_envelope();
        let subscriber_sink = router.resolve_sink(subscriber);
        let result = if let Some(sink) = subscriber_sink.as_ref() {
            router.route_to_resolved_sink(envelope, sink)
        } else {
            router.route(envelope)
        };

        match result {
            Ok(()) => return,
            // Always give a transient full mailbox one more attempt. The
            // deadline bounds how long we keep trying, but a subscriber that
            // was full for a microsecond should not lose its delivery just
            // because this thread happened to be descheduled past the deadline
            // between the two attempts.
            Err(RouteError::DeliveryFailed(_, DeliveryError::MailboxFull { .. }))
                if attempts == 1 || Instant::now() < deadline =>
            {
                std::thread::yield_now();
            }
            Err(_) => {
                record_delivery_drop(metrics);
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
