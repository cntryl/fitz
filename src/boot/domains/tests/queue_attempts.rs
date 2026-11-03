use super::*;
use crate::protocol::{ChannelId, MessageType};

fn deliver_queue_request(
    domains: &BrokerDomains,
    inbox: &RouteAddress,
    message_type: u16,
    payload: Vec<u8>,
) {
    domains
        .queue
        .deliver(Envelope::from_route(
            inbox.clone(),
            RouteAddress::new(RouteFamily::new(1), Route::new("queue://inbound")),
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(message_type),
                Bytes::from(payload),
                RouteFamily::new(1),
            ),
        ))
        .unwrap();
}

#[test]
fn should_apply_queue_attempt_limit_from_domain_setup_options() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    let mut options = domain_setup_options();
    options.route_families = vec![1];
    options.queue_max_attempts = std::num::NonZeroU32::new(1);
    let domains = setup(&router, &store, &read_model, &options).unwrap();
    let inbox = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/7"));
    let mailbox = Arc::new(crate::runtime::Mailbox::new(8));
    router.register(inbox.clone(), mailbox.clone());
    let route = b"queue://acme/jobs/limited";
    let mut enqueue = Vec::new();
    enqueue.put_u32(usize_to_u32_saturating(route.len()));
    enqueue.extend_from_slice(route);
    enqueue.put_u32(3);
    enqueue.extend_from_slice(b"job");
    deliver_queue_request(&domains, &inbox, 200, enqueue);
    receive_kv_response(&mailbox, "enqueue reply");
    let mut reserve = Vec::new();
    reserve.put_u32(usize_to_u32_saturating(route.len()));
    reserve.extend_from_slice(route);
    reserve.put_u64(0);
    reserve.extend_from_slice(&[0, 0]);
    deliver_queue_request(&domains, &inbox, 202, reserve);
    receive_kv_response(&mailbox, "reserve reply");

    // Act
    domains.queue.sweep_runtime_state();
    let counts = domains.queue.counts();

    // Assert
    assert_eq!(counts.dead_letters, 1);
    assert_eq!(counts.ready, 0);
    assert_eq!(counts.inflight, 0);
    domains.stop();
    router.clear();
}
