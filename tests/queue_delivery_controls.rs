use fitz::domains::queue::retention::QueueRetention;
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::protocol::wire3::extensions::{
    encode_capability_ids, encode_payload, ExtensionDirection, ExtensionField,
};
use fitz::protocol::wire3::header::{decode_message, encode_message, HeaderFlags};
use fitz::testkit::transport::{connect_body, TestServer, TestWebSocketClient};
use fitz::testkit::TestClient;
use std::num::NonZeroU64;
use std::time::Duration;

const ROUTE: &str = "queue://acme/jobs/controls";

fn connect(capabilities: &[u32]) -> Vec<u8> {
    let core = connect_body("", "queue-controls");
    let capabilities = encode_capability_ids(capabilities).unwrap();
    let payload = encode_payload(
        &core,
        1,
        ExtensionDirection::ClientToBroker,
        &[ExtensionField {
            field_id: 1,
            value: &capabilities,
        }],
    )
    .unwrap();
    encode_message(
        1,
        HeaderFlags::extensions_present(),
        None,
        payload.value.as_ref(),
    )
}

fn enqueue(ttl_hours: Option<u32>) -> Vec<u8> {
    let mut core = PayloadEncoder::new();
    core.put_string(ROUTE);
    core.put_bytes(b"job");
    core.put_u8(0);
    let core = core.finish();
    if let Some(ttl_hours) = ttl_hours {
        let value = ttl_hours.to_be_bytes();
        let payload = encode_payload(
            &core,
            200,
            ExtensionDirection::ClientToBroker,
            &[ExtensionField {
                field_id: 1,
                value: &value,
            }],
        )
        .unwrap();
        encode_message(
            200,
            HeaderFlags::extensions_present(),
            NonZeroU64::new(1),
            payload.value.as_ref(),
        )
    } else {
        encode_message(200, HeaderFlags::none(), NonZeroU64::new(1), &core)
    }
}

fn reserve(correlation: u64) -> Vec<u8> {
    let mut core = PayloadEncoder::new();
    core.put_string(ROUTE);
    core.put_u64(30);
    core.put_u8(0);
    core.put_u8(0);
    encode_message(
        202,
        HeaderFlags::none(),
        NonZeroU64::new(correlation),
        &core.finish(),
    )
}

fn nack(id: u64, token: u64, visibility_seconds: u64) -> Vec<u8> {
    let mut core = PayloadEncoder::new();
    core.put_string(ROUTE);
    core.put_u64(id);
    core.put_u64(token);
    core.put_u64(visibility_seconds);
    encode_message(210, HeaderFlags::none(), NonZeroU64::new(3), &core.finish())
}

fn reservation(frame: &[u8]) -> (u64, u64, bytes::Bytes) {
    let (_, value) = decode_message(frame, u32::MAX).unwrap();
    let mut decoder = PayloadDecoder::new(value);
    assert_eq!(decoder.get_u8().unwrap(), 0);
    assert_eq!(decoder.get_u32().unwrap(), 1);
    (
        decoder.get_u64().unwrap(),
        decoder.get_u64().unwrap(),
        decoder.get_bytes().unwrap(),
    )
}

#[tokio::test]
async fn should_redeliver_over_tcp_after_a_negotiated_nack() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = TestClient::new(server.tcp_addr).await.unwrap();
    client.send_frame(&connect(&[1, 2])).await.unwrap();
    client.request(&enqueue(Some(2)), 2_000).await.unwrap();
    let (id, old_token, _) = reservation(&client.request(&reserve(2), 2_000).await.unwrap());
    let response = client
        .request(&nack(id, old_token, 0), 2_000)
        .await
        .unwrap();
    assert_eq!(decode_message(&response, u32::MAX).unwrap().1, [0]);

    // Act
    let response = client.request(&reserve(4), 2_000).await.unwrap();

    // Assert
    let (new_id, new_token, body) = reservation(&response);
    assert_eq!(new_id, id);
    assert_ne!(new_token, old_token);
    assert_eq!(body.as_ref(), b"job");
    drop(client);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn should_redeliver_over_websocket_after_a_negotiated_nack() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .unwrap();
    client.send_frame(&connect(&[2])).await.unwrap();
    client.request(&enqueue(None), 2_000).await.unwrap();
    let (id, old_token, _) = reservation(&client.request(&reserve(2), 2_000).await.unwrap());
    let response = client
        .request(&nack(id, old_token, 0), 2_000)
        .await
        .unwrap();
    assert_eq!(decode_message(&response, u32::MAX).unwrap().1, [0]);

    // Act
    let response = client.request(&reserve(4), 2_000).await.unwrap();

    // Assert
    let (new_id, new_token, body) = reservation(&response);
    assert_eq!(new_id, id);
    assert_ne!(new_token, old_token);
    assert_eq!(body.as_ref(), b"job");
    drop(client);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn should_reject_nack_when_its_capability_was_not_negotiated() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = TestClient::new(server.tcp_addr).await.unwrap();
    client.send_frame(&connect(&[])).await.unwrap();
    client.request(&enqueue(None), 2_000).await.unwrap();
    let (id, token, _) = reservation(&client.request(&reserve(2), 2_000).await.unwrap());

    // Act
    let response = client.request(&nack(id, token, 0), 2_000).await.unwrap();

    // Assert
    let (header, body) = decode_message(&response, u32::MAX).unwrap();
    assert_eq!(header.correlation, NonZeroU64::new(3));
    assert_eq!(body[0], 1);
    assert_eq!(u16::from_be_bytes([body[1], body[2]]), 10);
    drop(client);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn should_preserve_a_publish_ttl_override_through_transport_dispatch() {
    // Arrange
    let server = TestServer::start_with_queue_retention(QueueRetention {
        message_seconds: Some(1),
        dlq_seconds: None,
    })
    .await
    .unwrap();
    let mut client = TestClient::new(server.tcp_addr).await.unwrap();
    client.send_frame(&connect(&[1])).await.unwrap();
    client.request(&enqueue(Some(1)), 2_000).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_250)).await;

    // Act
    let response = client.request(&reserve(2), 2_000).await.unwrap();

    // Assert
    assert_eq!(reservation(&response).2.as_ref(), b"job");
    drop(client);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn should_apply_default_message_retention_when_publish_omits_ttl() {
    // Arrange
    let server = TestServer::start_with_queue_retention(QueueRetention {
        message_seconds: Some(1),
        dlq_seconds: None,
    })
    .await
    .unwrap();
    let mut client = TestClient::new(server.tcp_addr).await.unwrap();
    client.request(&enqueue(None), 2_000).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_250)).await;

    // Act
    let response = client.request(&reserve(2), 2_000).await.unwrap();

    // Assert
    let (_, body) = decode_message(&response, u32::MAX).unwrap();
    assert_eq!(body, [0, 0, 0, 0, 0]);
    drop(client);
    server.shutdown().await.unwrap();
}
