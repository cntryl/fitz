mod fixtures;

use fitz::boot::runtime::StorageMode;
use fixtures::transport::*;

const KV: &str = "kv://test/cloud/state";
const STREAM: &str = "stream://test/cloud/events";

async fn request(client: &mut TestClient, frame: &[u8], expected: u16) -> Vec<u8> {
    let response = client.request(frame, 30_000).await.unwrap();
    let mut parser = TlvFrameParser::new(&response);
    let (kind, payload) = parser.next_field().unwrap();
    assert_eq!(kind, expected);
    assert_eq!(payload.first(), Some(&0), "operation failed: {payload:?}");
    payload
}

#[tokio::test]
#[ignore = "requires Sqrzl and FITZ_STORAGE_PROVIDER; run by integration workflows"]
async fn should_recover_committed_kv_and_stream_from_cloud_with_a_fresh_cache() {
    // Arrange
    let StorageMode::CloudBacked(mut config) = StorageMode::from_env() else {
        panic!("cloud integration requires valid cloud storage configuration");
    };
    let cache = tempfile::tempdir().unwrap();
    config.local_cache_path = cache.path().to_string_lossy().into_owned();
    config.prefix = Some(format!("integration/{}/", uuid::Uuid::new_v4()));
    let server = TestServer::start_with_storage(StorageMode::CloudBacked(config.clone()))
        .await
        .unwrap();
    let mut client = TestClient::new(server.tcp_addr).await.unwrap();

    // Act
    let begun = request(&mut client, &build_kv_begin(KV, 1), 100).await;
    let tx = parse_kv_tx_id(&begun).unwrap();
    request(&mut client, &build_kv_put(tx, KV, b"key", b"value"), 104).await;
    request(&mut client, &build_kv_commit(tx, KV, 1), 101).await;
    let begun = request(&mut client, &build_stream_begin(STREAM), 600).await;
    let session = parse_stream_session_id(&begun).unwrap();
    request(&mut client, &build_stream_append(session, 0, b"event"), 601).await;
    let mut payload = session.to_be_bytes().to_vec();
    payload.push(1); // Sync persistence waits for cloud storage.
    let mut commit = TlvFrameBuilder::new();
    commit.encode_field(602, &payload);
    request(&mut client, &commit.build(), 602).await;
    drop(client);
    server.shutdown().await.unwrap();
    let fresh_cache = tempfile::tempdir().unwrap();
    config.local_cache_path = fresh_cache.path().to_string_lossy().into_owned();
    let reopened = TestServer::start_with_storage(StorageMode::CloudBacked(config))
        .await
        .unwrap();
    let mut client = TestClient::new(reopened.tcp_addr).await.unwrap();
    let begun = request(&mut client, &build_kv_begin(KV, 0), 100).await;
    let tx = parse_kv_tx_id(&begun).unwrap();
    let value = request(&mut client, &build_kv_get(tx, KV, b"key"), 103).await;
    request(&mut client, &build_kv_rollback(tx, KV), 102).await;
    let events = request(&mut client, &build_stream_read(STREAM, 0), 604).await;

    // Assert
    assert_eq!(extract_kv_value(&value).unwrap(), b"value");
    assert!(events.windows(5).any(|bytes| bytes == b"event"));
    drop(client);
    reopened.shutdown().await.unwrap();
}
