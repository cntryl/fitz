//! Header-level contract: what the broker does with a frame it will not serve.

use fitz::protocol::error_codes::{decode_wire_error, protocol};
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::protocol::wire2::envelope::{Effect, ErrorClass};
use fitz::protocol::wire2::header::decode_message;
use fitz::testkit::transport::FrameBuilder;
use fitz::testkit::{TestClient, TestServer};
use std::num::NonZeroU64;

fn kv_begin(correlation: u64) -> Vec<u8> {
    let mut payload = PayloadEncoder::new();
    payload.put_string("kv://acme/app/users");
    payload.put_u8(1);
    let mut builder = FrameBuilder::new().with_correlation(correlation);
    builder.encode_field(100, &payload.finish());
    builder.build()
}

#[tokio::test]
async fn should_answer_a_reserved_header_flag_as_unsupported_on_the_open_session() {
    // Arrange
    let server = TestServer::start().await.expect("start broker");
    let mut client = TestClient::new(server.tcp_addr).await.expect("connect");
    let mut flagged = kv_begin(9);
    flagged[2] |= 0b1000_0000;

    // Act
    let answer = client.request(&flagged, 2_000).await.expect("answer");
    let follow_up = client
        .request(&kv_begin(10), 2_000)
        .await
        .expect("session still open");

    // Assert
    let (header, value) = decode_message(&answer, u32::MAX).expect("answer frame");
    assert_eq!(header.message_type, 100);
    assert_eq!(header.correlation, NonZeroU64::new(9));
    let error = decode_wire_error(value).expect("error envelope");
    assert_eq!(error.code, protocol::ERR_UNSUPPORTED);
    assert_eq!(error.class, ErrorClass::Unsupported);
    assert_eq!(error.effect, Effect::NotApplied);
    let (header, value) = decode_message(&follow_up, u32::MAX).expect("follow-up frame");
    assert_eq!(header.correlation, NonZeroU64::new(10));
    assert_eq!(value.first(), Some(&0), "a valid request still succeeds");
}
