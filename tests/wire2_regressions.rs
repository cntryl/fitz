use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::protocol::wire2::handshake::{Connect, Hello};
use fitz::protocol::wire2::header::{decode_message, encode_message, HeaderFlags};
use fitz::testkit::transport::{generate_test_jwt, TestServer};
use futures_util::{SinkExt, StreamExt};
use std::num::NonZeroU64;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn connect(version: u16, jwt: String) -> Vec<u8> {
    encode_message(
        1,
        HeaderFlags::none(),
        None,
        &Connect {
            min_version: version,
            max_version: version,
            client_caps: 0,
            jwt,
            service_name: String::new(),
            client_build: String::new(),
        }
        .encode(),
    )
}

async fn tcp_send(stream: &mut tokio::net::TcpStream, frame: &[u8]) {
    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .unwrap();
    stream.write_all(frame).await.unwrap();
}

async fn tcp_recv(stream: &mut tokio::net::TcpStream) -> Result<Vec<u8>, String> {
    tokio::time::timeout(Duration::from_secs(1), async {
        let len = stream.read_u32().await.map_err(|e| e.to_string())?;
        let mut frame = vec![0; len as usize];
        stream
            .read_exact(&mut frame)
            .await
            .map_err(|e| e.to_string())?;
        Ok(frame)
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tokio::test]
async fn should_deliver_connect_rejected_before_closing_tcp() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut stream = tokio::net::TcpStream::connect(server.tcp_addr)
        .await
        .unwrap();
    // Act
    tcp_send(&mut stream, &connect(99, String::new())).await;
    let answer = tcp_recv(&mut stream).await;
    // Assert
    assert!(answer.is_ok(), "CONNECT_REJECTED lost: {answer:?}");
    assert_eq!(
        decode_message(&answer.unwrap(), u32::MAX)
            .unwrap()
            .0
            .message_type,
        3
    );
}

#[tokio::test]
async fn should_deliver_connect_rejected_before_closing_websocket() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let (mut stream, _) = tokio_tungstenite::connect_async(format!("ws://{}", server.ws_addr))
        .await
        .unwrap();
    // Act
    stream
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            connect(99, String::new()).into(),
        ))
        .await
        .unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match stream.next().await {
                Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_))) => {}
                answer => break answer,
            }
        }
    })
    .await;
    // Assert
    let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(frame)))) = answer else {
        panic!("CONNECT_REJECTED lost: {answer:?}");
    };
    assert_eq!(decode_message(&frame, u32::MAX).unwrap().0.message_type, 3);
}

#[tokio::test]
async fn should_deliver_hello_when_authentication_changes_route_family() {
    // Arrange
    let server = TestServer::start_with_auth_route_families(vec![1, 2], [("acme", 2)])
        .await
        .unwrap();
    let mut stream = tokio::net::TcpStream::connect(server.tcp_addr)
        .await
        .unwrap();
    // Act
    tcp_send(&mut stream, &connect(2, generate_test_jwt("acme"))).await;
    let answer = tcp_recv(&mut stream).await;
    // Assert
    assert!(answer.is_ok(), "HELLO lost after family rebind: {answer:?}");
    assert_eq!(
        decode_message(&answer.unwrap(), u32::MAX)
            .unwrap()
            .0
            .message_type,
        2
    );
}

#[tokio::test]
async fn should_report_busy_when_queue_waiter_capacity_is_exhausted() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut stream = tokio::net::TcpStream::connect(server.tcp_addr)
        .await
        .unwrap();
    tcp_send(&mut stream, &connect(2, String::new())).await;
    let hello_frame = tcp_recv(&mut stream).await.unwrap();
    let hello = Hello::decode(decode_message(&hello_frame, u32::MAX).unwrap().1).unwrap();
    let mut payload = PayloadEncoder::new();
    payload.put_string("queue://acme/app/jobs");
    payload.put_u64(30);
    payload.put_u8(1);
    payload.put_u32(1);
    payload.put_u8(1);
    payload.put_u64(30);
    let payload = payload.finish();
    // Act
    for id in 1..=17 {
        tcp_send(
            &mut stream,
            &encode_message(202, HeaderFlags::none(), NonZeroU64::new(id), &payload),
        )
        .await;
    }
    let answer = tcp_recv(&mut stream).await;
    // Assert
    assert!(
        answer.is_ok(),
        "no BUSY after 17 parked requests (advertised max {}): {answer:?}",
        hello.max_in_flight
    );
    let answer = answer.unwrap();
    let error = fitz::protocol::error_codes::decode_wire_error(
        decode_message(&answer, u32::MAX).unwrap().1,
    )
    .unwrap();
    assert_eq!(
        error.class,
        fitz::protocol::wire2::envelope::ErrorClass::Busy,
        "unexpected answer: {error:?}"
    );
    assert_eq!(error.code, 4005);
    assert_eq!(
        error.effect,
        fitz::protocol::wire2::envelope::Effect::NotApplied
    );
    assert_eq!(
        decode_message(&answer, u32::MAX).unwrap().0.correlation,
        NonZeroU64::new(17)
    );
}

#[tokio::test]
async fn should_report_indeterminate_effect_after_dispatched_worker_disconnects() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut worker = server.connect().await.unwrap();
    let mut caller = server.connect().await.unwrap();
    let mut registration = PayloadEncoder::new();
    registration.put_string("rpc://acme/app/charge");
    registration.put_u32(1);
    registration.put_u8(0);
    let registered = worker
        .request(
            &encode_message(
                300,
                HeaderFlags::none(),
                NonZeroU64::new(1),
                &registration.finish(),
            ),
            2000,
        )
        .await
        .unwrap();
    assert_eq!(decode_message(&registered, u32::MAX).unwrap().1[0], 0);
    let mut request = PayloadEncoder::new();
    request.put_raw(uuid::Uuid::new_v4().as_bytes());
    request.put_string("rpc://acme/app/charge");
    request.put_bytes(b"apply side effect");
    request.put_u8(0);
    // Act
    caller
        .send_frame(&encode_message(
            302,
            HeaderFlags::none(),
            NonZeroU64::new(2),
            &request.finish(),
        ))
        .await
        .unwrap();
    let delivered = worker.recv_frame(2000).await.unwrap();
    assert_eq!(
        decode_message(&delivered, u32::MAX).unwrap().0.message_type,
        302
    );
    worker.close().await.unwrap();
    let response = caller.recv_frame(2000).await.unwrap();
    let (_, payload) = decode_message(&response, u32::MAX).unwrap();
    let error = fitz::protocol::error_codes::decode_wire_error(&payload[29..]).unwrap();
    // Assert
    assert_eq!(error.code, 6050);
    assert_eq!(
        error.effect,
        fitz::protocol::wire2::envelope::Effect::Indeterminate,
        "worker received the request, so its effects are unknown"
    );
}

#[tokio::test]
async fn should_accept_a_queue_value_below_the_advertised_message_limit() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = server.connect().await.unwrap();
    let mut enqueue = PayloadEncoder::new();
    enqueue.put_string("queue://acme/app/jobs");
    enqueue.put_bytes(&vec![42; 70000]);
    enqueue.put_u8(0);
    // Act
    let answer = client
        .request(
            &encode_message(
                200,
                HeaderFlags::none(),
                NonZeroU64::new(1),
                &enqueue.finish(),
            ),
            2000,
        )
        .await
        .unwrap();
    // Assert
    let (_, value) = decode_message(&answer, u32::MAX).unwrap();
    assert_eq!(
        value[0],
        0,
        "70KB under 1MiB should be accepted: {:?}",
        fitz::protocol::error_codes::decode_wire_error(value)
    );
}

async fn enqueue_large_value(client: &mut fitz::testkit::transport::TestClient) {
    let mut enqueue = PayloadEncoder::new();
    enqueue.put_string("queue://acme/app/jobs");
    enqueue.put_bytes(&vec![42; 70_000]);
    enqueue.put_u8(0);
    let answer = client
        .request(
            &encode_message(
                200,
                HeaderFlags::none(),
                NonZeroU64::new(1),
                &enqueue.finish(),
            ),
            2000,
        )
        .await
        .unwrap();
    assert_eq!(decode_message(&answer, u32::MAX).unwrap().1[0], 0);
}

async fn reserve_large_value(
    client: &mut fitz::testkit::transport::TestClient,
    route: &str,
) -> Vec<u8> {
    let mut reserve = PayloadEncoder::new();
    reserve.put_string(route);
    reserve.put_u64(30);
    reserve.put_u8(1);
    reserve.put_u32(1);
    reserve.put_u8(0);
    client
        .request(
            &encode_message(
                202,
                HeaderFlags::none(),
                NonZeroU64::new(2),
                &reserve.finish(),
            ),
            2000,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn should_reserve_a_queue_value_larger_than_the_old_u16_limit() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = server.connect().await.unwrap();
    enqueue_large_value(&mut client).await;
    // Act
    let answer = reserve_large_value(&mut client, "queue://acme/app/jobs").await;
    // Assert
    let mut value = PayloadDecoder::new(decode_message(&answer, u32::MAX).unwrap().1);
    assert_eq!(value.get_u8().unwrap(), 0);
    assert_eq!(value.get_u32().unwrap(), 1);
    value.get_u64().unwrap();
    value.get_u64().unwrap();
    assert_eq!(value.get_bytes().unwrap(), vec![42; 70_000].as_slice());
    assert!(value.is_complete());
}

#[tokio::test]
async fn should_wildcard_reserve_a_queue_value_larger_than_the_old_u16_limit() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = server.connect().await.unwrap();
    enqueue_large_value(&mut client).await;
    // Act
    let answer = reserve_large_value(&mut client, "queue://acme/app/*").await;
    // Assert
    let mut value = PayloadDecoder::new(decode_message(&answer, u32::MAX).unwrap().1);
    assert_eq!(value.get_u8().unwrap(), 0);
    assert_eq!(value.get_u32().unwrap(), 1);
    assert_eq!(value.get_string().unwrap(), "queue://acme/app/jobs");
    value.get_u64().unwrap();
    value.get_u64().unwrap();
    assert_eq!(value.get_bytes().unwrap(), vec![42; 70_000].as_slice());
    assert!(value.is_complete());
}

#[tokio::test]
async fn should_reject_a_65th_deferred_lease_acquire_when_max_in_flight_is_64() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut holder = server.connect().await.unwrap();
    let mut waiter = server.connect().await.unwrap();
    let acquire = |owner: &str, id: u64| {
        let mut payload = PayloadEncoder::new();
        payload.put_string("lease://acme/app/lock");
        payload.put_string(owner);
        payload.put_u64(30);
        payload.put_u32(30);
        encode_message(
            400,
            HeaderFlags::none(),
            NonZeroU64::new(id),
            &payload.finish(),
        )
    };
    holder.request(&acquire("holder", 1), 2000).await.unwrap();
    // Act
    let mut last = Vec::new();
    for id in 1..=65 {
        last = waiter
            .request(&acquire(&format!("waiter-{id}"), id), 2000)
            .await
            .unwrap();
    }
    // Assert
    let (header, value) = decode_message(&last, u32::MAX).unwrap();
    let error = fitz::protocol::error_codes::decode_wire_error(value).unwrap();
    assert_eq!(header.correlation, NonZeroU64::new(65));
    assert_eq!(error.code, 5);
    assert_eq!(
        error.class,
        fitz::protocol::wire2::envelope::ErrorClass::Busy
    );
    assert_eq!(
        error.effect,
        fitz::protocol::wire2::envelope::Effect::NotApplied
    );
}
