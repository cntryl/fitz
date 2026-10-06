use super::*;
use crate::dispatch::protocol::{ChannelId, MessageType};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::Mailbox;

fn block_family(sink: &QueueDomain) -> crossbeam_channel::Sender<()> {
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    let (done_tx, _) = crossbeam_channel::bounded(1);
    sink.family_runtime
        .try_enqueue(
            RouteFamily::new(1),
            crate::runtime::FamilyActorLane::Control,
            model::QueueDomainCommand::InspectForTests(
                Box::new(move |_| {
                    entered_tx.send(()).expect("report blocked family");
                    release_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("release blocked family");
                }),
                done_tx,
            ),
        )
        .expect("enqueue blocking command");
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("family is blocked");
    release_tx
}

#[test]
fn should_wait_for_admitted_queue_client_work_beyond_the_control_reply_budget() {
    // Arrange
    let family = RouteFamily::new(1);
    let inbox = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(inbox.clone(), mailbox.clone());
    let sink = Arc::new(new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::BestEffort,
    ));
    let release = block_family(&sink);
    let envelope = Envelope::from_route(
        inbox,
        RouteAddress::new(family, Route::new("queue://inbound")),
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send("queue://acme/jobs/delayed", b"work"),
            family,
        ),
    );
    let delivery_sink = sink.clone();
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);
    let delivery = std::thread::spawn(move || {
        result_tx
            .send(delivery_sink.deliver(envelope))
            .expect("report delivery result");
    });

    // Act
    let early = result_rx.recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT + Duration::from_millis(200));
    let returned_early = early.is_ok();
    release.send(()).expect("release family");
    let outcome = early.unwrap_or_else(|_| {
        result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("receive completed delivery")
    });
    delivery.join().expect("join delivery");

    // Assert
    assert!(
        !returned_early,
        "an admitted client command should keep waiting"
    );
    assert!(outcome.is_ok(), "delivery failed: {outcome:?}");
    let response = receive_queue_frame(&mailbox, "completed ENQUEUE response");
    assert_eq!(response.payload[0], 0, "ENQUEUE must succeed");
}

#[test]
fn should_keep_the_short_queue_control_reply_deadline() {
    // Arrange
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::BestEffort,
    );
    let release = block_family(&sink);
    let cleanup = Envelope::new(
        RouteAddress::new(RouteFamily::new(1), Route::new("queue://inbound")),
        crate::runtime::SessionCleanup { session_id: 7 },
    );

    // Act
    let result = sink.deliver_high_priority(cleanup);
    release.send(()).expect("release family");

    // Assert
    assert!(matches!(result, Err(DeliveryError::Timeout)));
}
