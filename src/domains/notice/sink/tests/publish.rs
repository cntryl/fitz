use super::*;

struct BlockingSink {
    entered: crossbeam_channel::Sender<()>,
    release: crossbeam_channel::Receiver<()>,
}

impl MailboxSink for BlockingSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        let _ = self.entered.send(());
        let _ = self.release.recv_timeout(Duration::from_secs(1));
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_accept_notice_publish_without_waiting_for_subscriber_delivery() {
    // Arrange
    let family = RouteFamily::new(1);
    let notice_route = "notice://acme/app/events";
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router.clone(), admin_read_model);
    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        7,
        notice_route,
        family,
    );
    let subscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(subscribe_response.status, 0);
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    router.register(
        subscriber_address,
        Arc::new(BlockingSink {
            entered: entered_tx,
            release: release_rx,
        }),
    );
    let request = crate::domains::notice::NoticeClientRequest::new(
        crate::runtime::ClientFrameMeta::new(11, crate::runtime::ClientChannel::Pub, 500, family),
        Ok(
            crate::domains::notice::protocol::NotificationMessage::Publish(
                crate::domains::notice::protocol::PublishMessage::new(
                    family,
                    Route::new(notice_route),
                    Bytes::from_static(b"hello"),
                ),
            ),
        ),
    );
    let (accepted_tx, accepted_rx) = crossbeam_channel::bounded(1);

    // Act
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let accepted = sink
                .deliver(Envelope::from_route(
                    publisher_address,
                    notice_address,
                    request,
                ))
                .is_ok();
            let _ = accepted_tx.send(accepted);
        });
        let early_accept = accepted_rx.recv_timeout(Duration::from_millis(50)).ok();
        let accepted_before_delivery_release = early_accept.is_some();
        let subscriber_delivery_started = entered_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        release_tx.send(()).expect("release blocking subscriber");
        let final_accept_result = early_accept.unwrap_or_else(|| {
            accepted_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("publish should eventually return")
        });

        // Assert
        assert!(subscriber_delivery_started);
        assert!(final_accept_result);
        assert!(accepted_before_delivery_release);
    });
}
