mod fixtures;

use bytes::Bytes;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::protocol::wire2::header::decode_message;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::domain_internals::stream::{StreamActor, StreamStore};
use fixtures::transport::*;
use std::sync::Arc;

const EVENT_LIMIT: usize = 400 * 1024;

fn actor() -> StreamActor {
    let store = Arc::new(StreamStore::new(fitz::testkit::create_test_db()));
    let mut actor = StreamActor::new(
        RouteFamily::new(1),
        "test".into(),
        "events".into(),
        "sized".into(),
        store,
    )
    .unwrap();
    actor.begin_append_session(1, 1, None).unwrap();
    actor
}

#[test]
fn should_accept_a_400_kib_combined_event_payload() {
    // Arrange
    let mut actor = actor();
    let body = Bytes::from(vec![42; EVENT_LIMIT - 16]);
    let metadata = Some(Bytes::from(vec![7; 16]));
    // Act
    let appended =
        actor.append_to_session_with_discriminator_for_owner(1, 1, 0, body, metadata, None);
    // Assert
    assert_eq!(appended, Ok(0));
}

#[test]
fn should_reject_one_byte_over_400_kib_of_combined_event_payload() {
    // Arrange
    let mut actor = actor();
    let body = Bytes::from(vec![42; EVENT_LIMIT]);
    let metadata = Some(Bytes::from_static(b"x"));
    // Act
    let appended =
        actor.append_to_session_with_discriminator_for_owner(1, 1, 0, body, metadata, None);
    // Assert
    assert_eq!(appended, Err("event too large".into()));
}

async fn append_large_event<C: StreamConnector>(client: &mut C, route: &str, offset: u64) {
    let begin = client
        .send_and_receive(&build_stream_begin(route), 2_000)
        .await
        .unwrap();
    let (_, _, payload) = parse_stream_response(&begin);
    let session = parse_stream_session_id(&payload).unwrap();
    let append = build_stream_append_with_metadata(
        session,
        offset,
        &vec![42; EVENT_LIMIT - 16],
        Some(&[7; 16]),
    );
    let reply = client.send_and_receive(&append, 2_000).await.unwrap();
    assert_eq!(
        decode_message(&reply, u32::MAX).unwrap().1[0],
        0,
        "APPEND at the limit"
    );
    let reply = client
        .send_and_receive(&build_stream_commit(session), 2_000)
        .await
        .unwrap();
    assert_eq!(decode_message(&reply, u32::MAX).unwrap().1[0], 0, "COMMIT");
}

async fn assert_large_event_replays<C: StreamConnector>() {
    // Arrange
    let server = TestServer::start_with_max_message_bytes(512 * 1024)
        .await
        .unwrap();
    let mut client = C::connect(&server).await.unwrap();
    let realm = "r".repeat(4_096 - "stream://".len() - "/events/sized".len());
    let route = format!("stream://{realm}/events/sized");
    for offset in 0..3 {
        append_large_event(&mut client, &route, offset).await;
    }
    // Act
    let mut replies = Vec::new();
    for offset in 0..3 {
        replies.push(
            client
                .send_and_receive(&build_stream_read(&route, offset), 2_000)
                .await
                .unwrap(),
        );
    }
    // Assert
    for (offset, reply) in replies.iter().enumerate() {
        assert!(reply.len() <= 512 * 1024);
        let (_, payload) = decode_message(reply, u32::MAX).unwrap();
        let mut response = PayloadDecoder::new(payload);
        assert_eq!(
            response.get_u8().unwrap(),
            0,
            "{:?}",
            fitz::protocol::error_codes::decode_wire_error(payload)
        );
        assert_eq!(response.get_u8().unwrap(), 0);
        let data_bytes = response.get_bytes().unwrap();
        assert!(response.is_complete());
        let mut data = PayloadDecoder::new(&data_bytes);
        assert_eq!(data.get_u32().unwrap(), 1, "one event per 512 KiB page");
        assert_eq!(data.get_string().unwrap(), route);
        assert_eq!(data.get_u8().unwrap(), 0);
        assert_eq!(data.get_u64().unwrap(), u64::try_from(offset).unwrap());
        data.get_optional_u64().unwrap();
        data.get_optional_u64().unwrap();
        assert_eq!(
            data.get_bytes().unwrap(),
            vec![42; EVENT_LIMIT - 16].as_slice()
        );
        assert_eq!(data.get_u8().unwrap(), 1);
        assert_eq!(data.get_bytes().unwrap().as_ref(), &[7; 16]);
        data.get_u64().unwrap();
        assert_eq!(data.get_u64().unwrap(), u64::try_from(offset).unwrap());
        data.get_optional_u64().unwrap();
        data.get_optional_u64().unwrap();
        assert_eq!(data.get_u8().unwrap(), u8::from(offset < 2));
        assert!(data.is_complete());
    }
}

#[tokio::test]
async fn should_page_400_kib_events_on_the_longest_route_over_tcp() {
    assert_large_event_replays::<TcpStreamConnector>().await;
}

#[tokio::test]
async fn should_replay_a_400_kib_event_on_the_longest_route_over_websocket() {
    assert_large_event_replays::<WsStreamConnector>().await;
}

async fn assert_oversized_event_is_not_staged<C: StreamConnector>() {
    // Arrange
    let server = TestServer::start_with_max_message_bytes(512 * 1024)
        .await
        .unwrap();
    let mut client = C::connect(&server).await.unwrap();
    let begin = client
        .send_and_receive(&build_stream_begin("stream://test/events/sized"), 2_000)
        .await
        .unwrap();
    let (_, _, payload) = parse_stream_response(&begin);
    let session = parse_stream_session_id(&payload).unwrap();
    let append = build_stream_append_with_metadata(session, 0, &vec![42; EVENT_LIMIT], Some(b"x"));
    // Act
    let rejected = client.send_and_receive(&append, 2_000).await.unwrap();
    let next = client
        .send_and_receive(&build_stream_append(session, 0, b"valid"), 2_000)
        .await
        .unwrap();
    // Assert
    let error = fitz::protocol::error_codes::decode_wire_error(
        decode_message(&rejected, u32::MAX).unwrap().1,
    )
    .unwrap();
    assert_eq!(error.code, 2008);
    assert_eq!(
        error.effect,
        fitz::protocol::wire2::envelope::Effect::NotApplied
    );
    assert_eq!(decode_message(&next, u32::MAX).unwrap().1[0], 0);
}

#[tokio::test]
async fn should_refuse_an_oversized_event_without_staging_it_over_tcp() {
    assert_oversized_event_is_not_staged::<TcpStreamConnector>().await;
}

#[tokio::test]
async fn should_refuse_an_oversized_event_without_staging_it_over_websocket() {
    assert_oversized_event_is_not_staged::<WsStreamConnector>().await;
}

#[tokio::test]
async fn should_replay_a_400_kib_event_after_disk_recovery() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let boot = fitz::boot::runtime::BootConfig::with_local_storage(
        directory
            .path()
            .join("stream")
            .to_string_lossy()
            .into_owned(),
    );
    let engine = fitz::testkit::init_storage(&boot).await.unwrap();
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut writer = StreamActor::new(
        RouteFamily::new(1),
        "test".into(),
        "events".into(),
        "sized".into(),
        store.clone(),
    )
    .unwrap();
    writer.begin_append_session(1, 1, None).unwrap();
    writer
        .append_to_session_with_discriminator_for_owner(
            1,
            1,
            0,
            Bytes::from(vec![42; EVENT_LIMIT - 16]),
            Some(Bytes::from(vec![7; 16])),
            None,
        )
        .unwrap();
    writer
        .commit_session_for_owner(1, 1, fitz::domains::stream::protocol::StreamWriteMode::Sync)
        .unwrap();
    drop(writer);
    drop(store);
    let mut engine =
        Arc::try_unwrap(engine).unwrap_or_else(|_| panic!("release Stream engine references"));
    engine.shutdown(std::time::Duration::from_secs(2)).unwrap();
    drop(engine);
    // Act
    let engine = fitz::testkit::init_storage(&boot).await.unwrap();
    let store = Arc::new(StreamStore::new(engine));
    let reader = StreamActor::new(
        RouteFamily::new(1),
        "test".into(),
        "events".into(),
        "sized".into(),
        store,
    )
    .unwrap();
    let replay = reader.read(0, 1, None).unwrap();
    // Assert
    let [fitz::domains::stream::StreamReadItem::Event(record)] = replay.items.as_slice() else {
        panic!("expected one recovered event")
    };
    assert_eq!(record.body.as_ref(), vec![42; EVENT_LIMIT - 16].as_slice());
    assert_eq!(record.metadata.as_ref().unwrap().as_ref(), &[7; 16]);
}
