use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::protocol::wire2::header::{encode_message, HeaderFlags};
use fitz::runtime::router::MailboxSink;
use fitz::runtime::{DeliveryError, Envelope};
use fitz::testkit::{transport::build_connect_frame, TestServer};
use futures_util::{SinkExt, StreamExt};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

struct StalledDomain {
    entered: Arc<tokio::sync::Notify>,
    release: crossbeam_channel::Receiver<()>,
    first: AtomicBool,
}
impl MailboxSink for StalledDomain {
    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.first.swap(true, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.recv_timeout(Duration::from_secs(15)).unwrap();
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_keep_a_responsive_websocket_alive_while_domain_dispatch_is_blocked() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    server.runtime.router().register_domain_pattern(
        "stream",
        Arc::new(StalledDomain {
            entered: entered.clone(),
            release: release_rx,
            first: AtomicBool::new(false),
        }),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/", server.ws_addr))
        .await
        .unwrap();
    ws.send(Message::Binary(build_connect_frame("acme", "").into()))
        .await
        .unwrap();
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Binary(_) => break,
            Message::Ping(body) => ws.send(Message::Pong(body)).await.unwrap(),
            _ => {}
        }
    }
    let mut payload = PayloadEncoder::new();
    payload.put_string("stream://acme/logs/events/append");
    payload.put_u8(0);
    let request = encode_message(
        600,
        HeaderFlags::none(),
        std::num::NonZeroU64::new(77),
        &payload.finish(),
    );
    // Act
    ws.send(Message::Binary(request.into())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("domain entered");
    let deadline = tokio::time::sleep(Duration::from_millis(11_500));
    tokio::pin!(deadline);
    let mut pongs = 0;
    let failure = loop {
        tokio::select! {
            () = &mut deadline => break None,
            next = ws.next() => match next {
                Some(Ok(Message::Ping(body))) => {
                    pongs += 1;
                    if let Err(error) = ws.send(Message::Pong(body)).await { break Some(error.to_string()); }
                }
                Some(Ok(Message::Close(reason))) => break Some(format!("closed: {reason:?}")),
                Some(Err(error)) => break Some(error.to_string()),
                None => break Some("stream ended".to_string()),
                _ => {},
            }
        }
    };
    release_tx.send(()).unwrap();
    // Assert
    assert!(failure.is_none(), "responsive client sent {pongs} PONGs but slow domain caused premature transport failure: {failure:?}");
}
