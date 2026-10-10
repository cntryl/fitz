use super::*;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct BlockingSink {
    entered: crossbeam_channel::Sender<RouteFamily>,
    release: crossbeam_channel::Receiver<()>,
}

impl MailboxSink for BlockingSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.entered.send(*envelope.destination().family()).unwrap();
        self.release.recv().unwrap();
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

fn overlapping_handoffs(second_family: u32, cancel_first: bool) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(2);
    let (release_tx, release_rx) = crossbeam_channel::bounded(2);
    let router = Arc::new(Router::new());
    router.register_domain_pattern(
        "queue",
        Arc::new(BlockingSink {
            entered: entered_tx,
            release: release_rx,
        }),
    );
    let ingress = Arc::new(super::super::RuntimeIngress::new(false).with_router(router.clone()));
    let dispatch = |family: u32| {
        let ingress = ingress.clone();
        let router = router.clone();
        rt.spawn(async move {
            let destination = RouteAddress::new(RouteFamily::new(family), Route::new("queue://in"));
            ingress
                .dispatcher
                .route_client_domain(
                    &router,
                    DispatchDomain::Queue,
                    Envelope::new(destination, ()),
                    Instant::now(),
                )
                .await
        })
    };
    let first = dispatch(1);
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    if cancel_first {
        first.abort();
    }
    let second = dispatch(second_family);
    let overlapped = entered_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    release_tx.send(()).unwrap();
    if !overlapped {
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    release_tx.send(()).unwrap();
    rt.block_on(async {
        if cancel_first {
            assert!(first.await.unwrap_err().is_cancelled());
        } else {
            first.await.unwrap().unwrap();
        }
        second.await.unwrap().unwrap();
    });
    overlapped
}

#[test]
fn should_serialize_queue_handoffs_within_a_route_family() {
    // Arrange
    let family = 1;

    // Act
    let overlapped = overlapping_handoffs(family, false);

    // Assert
    assert!(
        !overlapped,
        "one family must use one blocking Queue handoff at a time"
    );
}

#[test]
fn should_keep_queue_handoffs_independent_across_route_families() {
    // Arrange
    let family = 2;

    // Act
    let overlapped = overlapping_handoffs(family, false);

    // Assert
    assert!(
        overlapped,
        "one blocked Queue family must not hold another family"
    );
}

#[test]
fn should_retain_queue_family_handoff_after_transport_waiter_is_canceled() {
    // Arrange
    let family = 1;

    // Act
    let overlapped = overlapping_handoffs(family, true);

    // Assert
    assert!(
        !overlapped,
        "cancellation cannot recycle an active Queue handoff"
    );
}

#[test]
fn should_reject_expired_queue_handoff_before_enqueue() {
    // Arrange
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (_release_tx, release_rx) = crossbeam_channel::bounded(1);
    let router = Arc::new(Router::new());
    router.register_domain_pattern(
        "queue",
        Arc::new(BlockingSink {
            entered: entered_tx,
            release: release_rx,
        }),
    );
    let ingress = super::super::RuntimeIngress::new(false).with_router(router.clone());
    let family_lock = Arc::new(tokio::sync::Mutex::new(()));
    ingress
        .dispatcher
        .queue_family_dispatch
        .insert(1, family_lock.clone());
    let _active_handoff = rt.block_on(family_lock.lock_owned());
    let destination = RouteAddress::new(RouteFamily::new(1), Route::new("queue://in"));
    let started_at = Instant::now()
        .checked_sub(QUEUE_HANDOFF_WAIT_BUDGET)
        .unwrap();

    // Act
    let result = rt.block_on(ingress.dispatcher.route_client_domain(
        &router,
        DispatchDomain::Queue,
        Envelope::new(destination, ()),
        started_at,
    ));

    // Assert
    assert!(matches!(
        result,
        Err(crate::runtime::router::RouteError::DeliveryFailed(
            _,
            DeliveryError::MailboxFull { .. }
        ))
    ));
    assert!(
        entered_rx.try_recv().is_err(),
        "expired handoff reached the sink"
    );
}
