use super::fixture::{self, KEYS, ROUTE};
use cntryl_stress::ProgressHandle;
use fitz::benchkit::{
    build_kv_begin, build_kv_commit, build_kv_put, build_kv_rollback, extract_single_tlv_field,
    parse_kv_tx_id,
};
use fitz::testkit::{FrameBuilder, TestClient};
use std::net::SocketAddr;

pub(super) async fn request(client: &mut TestClient, frame: &[u8], expected: u16) -> Vec<u8> {
    client.send_frame(frame).await.unwrap();
    let reply = client.recv_frame(60_000).await.unwrap();
    let (kind, payload) = extract_single_tlv_field(&reply);
    assert_eq!(kind, expected);
    assert_eq!(payload.first(), Some(&0), "operation failed: {payload:?}");
    payload.to_vec()
}

pub(super) async fn verify(address: SocketAddr, segments: u32, progress: &ProgressHandle) -> u64 {
    let mut client = TestClient::new(address).await.unwrap();
    let begun = request(&mut client, &build_kv_begin(ROUTE, 0), 100).await;
    let tx = parse_kv_tx_id(&begun).unwrap();
    for slot in 0..KEYS {
        assert_eq!(
            read(&mut client, tx, &slot.to_be_bytes()).await,
            fixture::expected(segments, slot)
        );
        progress.advance();
    }
    request(&mut client, &build_kv_rollback(tx, ROUTE), 102).await;
    u64::from(KEYS)
}

async fn read(client: &mut TestClient, tx: u64, key: &[u8]) -> Vec<u8> {
    let mut payload = tx.to_be_bytes().to_vec();
    payload.extend_from_slice(&u32::try_from(ROUTE.len()).unwrap().to_be_bytes());
    payload.extend_from_slice(ROUTE.as_bytes());
    payload.extend_from_slice(&u32::try_from(key.len()).unwrap().to_be_bytes());
    payload.extend_from_slice(key);
    let mut frame = FrameBuilder::new();
    frame.encode_field(103, &payload);
    let reply = request(client, &frame.build(), 103).await;
    assert_eq!(reply[1], 1, "historical value must exist");
    let length = u32::from_be_bytes(reply[2..6].try_into().unwrap());
    assert_eq!(usize::try_from(length).unwrap(), reply.len() - 6);
    reply[6..].to_vec()
}

pub(super) async fn verify_marker(address: SocketAddr) {
    let mut client = TestClient::new(address).await.unwrap();
    let begun = request(&mut client, &build_kv_begin(ROUTE, 0), 100).await;
    let tx = parse_kv_tx_id(&begun).unwrap();
    assert_eq!(read(&mut client, tx, b"after-recovery").await, b"sync");
    request(&mut client, &build_kv_rollback(tx, ROUTE), 102).await;
}

pub(super) async fn write_marker(address: SocketAddr) {
    let mut client = TestClient::new(address).await.unwrap();
    let begun = request(&mut client, &build_kv_begin(ROUTE, 1), 100).await;
    let tx = parse_kv_tx_id(&begun).unwrap();
    request(
        &mut client,
        &build_kv_put(tx, ROUTE, b"after-recovery", b"sync"),
        104,
    )
    .await;
    // A real cloud-strict commit exercises catalog publication after startup.
    request(&mut client, &build_kv_commit(tx, ROUTE, 1), 101).await;
}
