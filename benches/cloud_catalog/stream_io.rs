use super::super::protocol::request;
use cntryl_stress::ProgressHandle;
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_read_with_limit,
};
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub(super) const STREAMS: u32 = 16;
pub(super) const EVENTS: u32 = 1024;
pub(super) const BODY_BYTES: u32 = 64 * 1024;
const BATCH: u32 = 16;

fn route(stream: u32) -> String {
    format!("stream://test/gib/history-{stream}")
}

fn body(stream: u32, offset: u32) -> Vec<u8> {
    let mut state = (u64::from(stream) << 32) | (u64::from(offset) + 1);
    let mut bytes = vec![0; usize::try_from(BODY_BYTES).unwrap()];
    for byte in &mut bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state.to_le_bytes()[0];
    }
    bytes[..4].copy_from_slice(&stream.to_be_bytes());
    bytes[4..8].copy_from_slice(&offset.to_be_bytes());
    bytes
}

fn write_data(payload: &[u8]) -> bytes::Bytes {
    let mut decoder = PayloadDecoder::new(payload);
    assert_eq!(decoder.get_u8().unwrap(), 0);
    let data = decoder.get_bytes().unwrap();
    assert!(decoder.is_complete());
    data
}

pub(super) async fn append_batch(address: SocketAddr, stream: u32, offset: u32) {
    let mut client = TestClient::new(address).await.unwrap();
    let route = route(stream);
    let begun = request(&mut client, &build_stream_begin(&route), 600).await;
    let mut decoder = PayloadDecoder::new(&begun);
    assert_eq!(decoder.get_u8().unwrap(), 0);
    let session = decoder.get_u64().unwrap();
    assert!(decoder.get_bytes().unwrap().is_empty());
    assert!(decoder.is_complete());
    for index in offset..offset + BATCH {
        let appended = request(
            &mut client,
            &build_stream_append(session, u64::from(index), &body(stream, index)),
            601,
        )
        .await;
        let data = write_data(&appended);
        let mut decoder = PayloadDecoder::new(&data);
        assert_eq!(decoder.get_u64().unwrap(), u64::from(index));
        assert!(decoder.is_complete());
    }
    let committed = request(&mut client, &build_stream_commit(session, 1), 602).await;
    assert!(write_data(&committed).is_empty());
    client.close().await.unwrap();
}

pub(super) async fn seed(
    address: SocketAddr,
    endpoint: &str,
    progress: &ProgressHandle,
    accepted: Arc<AtomicU64>,
) {
    let mut workers = tokio::task::JoinSet::new();
    for lane in 0..4 {
        let endpoint = endpoint.to_owned();
        let progress = progress.clone();
        let accepted = Arc::clone(&accepted);
        workers.spawn(async move {
            for stream in (lane..STREAMS).step_by(4) {
                let start = if stream == 0 { BATCH } else { 0 };
                for offset in (start..EVENTS).step_by(usize::try_from(BATCH).unwrap()) {
                    append_batch(address, stream, offset).await;
                    accepted.fetch_add(u64::from(BATCH), Ordering::AcqRel);
                    progress.advance();
                    healthy(&endpoint).await;
                }
            }
        });
    }
    while let Some(result) = workers.join_next().await {
        result.unwrap();
    }
}

pub(super) async fn healthy(endpoint: &str) {
    let health = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap()
        .get(format!("{endpoint}/healthz"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(health["status"], "ready");
}

fn verify_page(payload: &[u8], stream: u32, offset: u32) -> u32 {
    let mut outer = PayloadDecoder::new(payload);
    assert_eq!(outer.get_u8().unwrap(), 0);
    assert!(outer.get_optional_u64().unwrap().is_none());
    let data = outer.get_bytes().unwrap();
    assert!(outer.is_complete());
    let mut decoder = PayloadDecoder::new(&data);
    let count = decoder.get_u32().unwrap();
    assert!(count > 0 && count <= BATCH && offset + count <= EVENTS);
    for index in offset..offset + count {
        assert_eq!(decoder.get_string_ref().unwrap(), route(stream));
        assert_eq!(decoder.get_u8().unwrap(), 0);
        assert_eq!(decoder.get_u64().unwrap(), u64::from(index));
        decoder.get_optional_u64().unwrap();
        decoder.get_optional_u64().unwrap();
        assert_eq!(decoder.get_bytes().unwrap().as_ref(), body(stream, index));
        assert!(decoder.get_optional_bytes().unwrap().is_none());
        decoder.get_u64().unwrap();
    }
    assert_eq!(decoder.get_u64().unwrap(), u64::from(offset + count - 1));
    decoder.get_optional_u64().unwrap();
    decoder.get_optional_u64().unwrap();
    let more = decoder.get_u8().unwrap();
    assert_eq!(more != 0, offset + count < EVENTS);
    assert!(decoder.is_complete());
    count
}

pub(super) async fn verify(address: SocketAddr, progress: &ProgressHandle) -> u64 {
    let mut client = TestClient::new(address).await.unwrap();
    let mut verified = 0;
    for stream in 0..STREAMS {
        let mut offset = 0;
        while offset < EVENTS {
            let page = request(
                &mut client,
                &build_stream_read_with_limit(&route(stream), u64::from(offset), u64::from(BATCH)),
                604,
            )
            .await;
            let count = verify_page(&page, stream, offset);
            offset += count;
            verified += u64::from(count);
            progress.advance();
        }
    }
    client.close().await.unwrap();
    verified
}
