use super::*;

const KEY: &[u8] = b"same-key";
const REQUEST_TIMEOUT_MS: u64 = 2000;

async fn begin_kv(client: &mut impl KvConnector, route: &str, mode: u8) -> u64 {
    let reply = client
        .request(&build_kv_begin(route, mode), REQUEST_TIMEOUT_MS)
        .await
        .expect("begin KV transaction");
    let (message_type, status, body) = parse_kv_response(&reply);
    assert_eq!((message_type, status), (100, 0), "BEGIN in {route}");
    parse_kv_tx_id(&body).expect("decode transaction ID")
}

async fn seed_realm(
    kv: &mut impl KvConnector,
    queue: &mut impl QueueConnector,
    kv_route: &str,
    queue_route: &str,
    body: &[u8],
) {
    let tx = begin_kv(kv, kv_route, 1).await;
    let put_reply = kv
        .request(&build_kv_put(tx, kv_route, KEY, body), REQUEST_TIMEOUT_MS)
        .await
        .expect("put KV value");
    let (message_type, status, _) = parse_kv_response(&put_reply);
    assert_eq!((message_type, status), (104, 0), "PUT in {kv_route}");
    let commit_reply = kv
        .request(&build_kv_commit(tx, kv_route, 1), REQUEST_TIMEOUT_MS)
        .await
        .expect("commit KV transaction");
    let (message_type, status, _) = parse_kv_response(&commit_reply);
    assert_eq!((message_type, status), (101, 0), "COMMIT in {kv_route}");

    let enqueue_reply = queue
        .request(&build_queue_enqueue(queue_route, body), REQUEST_TIMEOUT_MS)
        .await
        .expect("enqueue realm's job");
    let (message_type, status, _) = parse_queue_response(&enqueue_reply);
    assert_eq!((message_type, status), (200, 0), "send to {queue_route}");
}

async fn check_realm_isolation<K, Q>(server: &TestServer)
where
    K: KvConnector,
    Q: QueueConnector,
{
    // Arrange
    let cases = [
        (
            "kv://east/app/jobs",
            "queue://east/app/jobs",
            b"east".as_slice(),
        ),
        (
            "kv://west/app/jobs",
            "queue://west/app/jobs",
            b"west".as_slice(),
        ),
    ];
    let mut kv = K::connect(server).await.expect("connect KV client");
    let mut queue = Q::connect(server).await.expect("connect Queue client");
    for (kv_route, queue_route, body) in cases {
        seed_realm(&mut kv, &mut queue, kv_route, queue_route, body).await;
    }

    // Act
    let mut reads = Vec::new();
    for (kv_route, queue_route, _) in cases {
        let read_tx = begin_kv(&mut kv, kv_route, 0).await;
        let kv_frame = build_kv_get(read_tx, kv_route, KEY);
        let queue_frame = build_queue_reserve(queue_route, Some(2));
        let replies = tokio::try_join!(
            kv.request(&kv_frame, REQUEST_TIMEOUT_MS),
            queue.request(&queue_frame, REQUEST_TIMEOUT_MS),
        )
        .expect("read realm through both domains");
        reads.push(replies);
    }

    // Assert
    for ((kv_route, queue_route, expected), (kv_reply, queue_reply)) in cases.into_iter().zip(reads)
    {
        let (message_type, status, body) = parse_kv_response(&kv_reply);
        assert_eq!((message_type, status), (103, 0), "GET in {kv_route}");
        assert_eq!(extract_kv_value(&body).expect("decode KV value"), expected);
        let (message_type, status, body) = parse_queue_response(&queue_reply);
        assert_eq!(
            (message_type, status),
            (202, 0),
            "reserve from {queue_route}"
        );
        let jobs = extract_queue_messages(&body).expect("decode reserved realm's jobs");
        assert_eq!(jobs, [expected.to_vec()]);
    }
}

#[tokio::test]
async fn should_keep_realm_data_isolated_given_active_kv_and_queue_tcp() {
    let server = TestServer::start().await.expect("start");
    check_realm_isolation::<TcpConnector, TcpQueueConnector>(&server).await;
}

#[tokio::test]
async fn should_keep_realm_data_isolated_given_active_kv_and_queue_ws() {
    let server = TestServer::start().await.expect("start");
    check_realm_isolation::<WsConnector, WsQueueConnector>(&server).await;
}
