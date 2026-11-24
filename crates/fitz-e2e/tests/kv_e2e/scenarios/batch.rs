use crate::fixtures::transport::{
    build_kv_begin, build_kv_commit, build_kv_put, parse_kv_response, KvConnector, TestServer,
};

pub(crate) async fn should_handle_large_batch_writes_in_transaction<C>(server: &TestServer)
where
    C: KvConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "kv://test/app/batch";

    let begin_frame = build_kv_begin(route, 1);
    client.request(&begin_frame, 2000).await.expect("BEGIN");

    // Act - Write 50 key-value pairs
    for i in 0..50 {
        let key = format!("key_{i:03}").into_bytes();
        let value = format!("value_{i:03}").into_bytes();
        let put_frame = build_kv_put(1, route, &key, &value);
        let response = client
            .request(&put_frame, 2000)
            .await
            .unwrap_or_else(|_| panic!("PUT {i} failed"));

        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 0, "PUT {i} should succeed");
    }

    let commit_frame = build_kv_commit(1, route, 0);
    let commit_response = client
        .request(&commit_frame, 2000)
        .await
        .expect("COMMIT failed");

    // Assert
    let (_msg_type, status, _data) = parse_kv_response(&commit_response);
    assert_eq!(status, 0, "COMMIT should succeed");
}
