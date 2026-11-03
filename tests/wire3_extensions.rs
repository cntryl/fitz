use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::protocol::wire3::extensions::{encode_payload, ExtensionDirection, ExtensionField};
use fitz::protocol::wire3::header::{decode_message, encode_message, HeaderFlags};
use fitz::testkit::transport::{build_connect_frame, connect_body, TestServer};
use fitz::testkit::TestClient;
use std::num::NonZeroU64;

fn kv_begin_core() -> Vec<u8> {
    let mut payload = PayloadEncoder::new();
    payload.put_string("kv://acme/app/users");
    payload.put_u8(1);
    payload.finish()
}

#[tokio::test]
async fn should_dispatch_after_ignoring_bounded_unknown_extension() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = TestClient::new(server.tcp_addr).await.unwrap();
    let fields = [ExtensionField {
        field_id: 4096,
        value: b"future-field",
    }];
    let core = kv_begin_core();
    let payload = encode_payload(&core, 100, ExtensionDirection::ClientToBroker, &fields).unwrap();
    let frame = encode_message(
        100,
        HeaderFlags::extensions_present(),
        NonZeroU64::new(1),
        payload.value.as_ref(),
    );

    // Act
    let answer = client.request(&frame, 2_000).await.unwrap();

    // Assert
    let (header, value) = decode_message(&answer, u32::MAX).unwrap();
    assert_eq!(header.correlation, NonZeroU64::new(1));
    assert_eq!(value.first(), Some(&0));
}

#[tokio::test]
async fn should_close_before_dispatch_when_an_extension_value_is_truncated() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = TestClient::new(server.tcp_addr).await.unwrap();
    client
        .send_frame(&build_connect_frame("", ""))
        .await
        .unwrap();
    let fields = [ExtensionField {
        field_id: 4096,
        value: b"x",
    }];
    let core = kv_begin_core();
    let payload = encode_payload(&core, 100, ExtensionDirection::ClientToBroker, &fields).unwrap();
    let mut frame = encode_message(
        100,
        HeaderFlags::extensions_present(),
        NonZeroU64::new(2),
        payload.value.as_ref(),
    );
    let declared_value_length = 15 + 4 + core.len() + 2;
    frame[declared_value_length..declared_value_length + 4]
        .copy_from_slice(&u32::MAX.to_be_bytes());

    // Act
    client.send_frame(&frame).await.unwrap();
    let result = client.recv_frame(2_000).await;

    // Assert
    assert!(
        result.is_err(),
        "malformed extension must close the session"
    );
}

#[tokio::test]
async fn should_close_when_connect_omits_the_capability_set() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = TestClient::new(server.tcp_addr).await.unwrap();
    let core = connect_body("", "");
    let frame = encode_message(1, HeaderFlags::none(), None, &core);

    // Act
    client.send_frame(&frame).await.unwrap();
    let result = client.recv_frame(2_000).await;

    // Assert
    assert!(result.is_err(), "CONNECT without capability IDs must close");
}

#[tokio::test]
async fn should_validate_extensions_before_rejecting_an_unsupported_message() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = TestClient::new(server.tcp_addr).await.unwrap();
    let payload = [0, 0, 0, 0, 0, 8, 0, 0, 0, 4, b'x'];
    let frame = encode_message(
        999,
        HeaderFlags::extensions_present(),
        NonZeroU64::new(3),
        &payload,
    );

    // Act
    client.send_frame(&frame).await.unwrap();
    let result = client.recv_frame(2_000).await;

    // Assert
    assert!(
        result.is_err(),
        "malformed extensions close even on an unsupported message type"
    );
}
