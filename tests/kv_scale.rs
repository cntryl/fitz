use bytes::Bytes;
use fitz::domains::kv::{KvActor, KvMessage, KvResponse, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

#[test]
fn should_handle_high_throughput_batch_puts() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);

    // Act - perform many small transactions (fast, buffered)
    for i in 0..200 {
        // Arrange per-iteration
        let begin = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "scale".to_string(),
            area: "kv".to_string(),
            resource: "batch".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match begin { KvResponse::BeginOk { tx_id } => tx_id, _ => panic!("Begin failed") };

        // Act
        let key = Bytes::from(format!("k{:04}", i));
        let _ = actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "batch".to_string(),
            key: key.clone(),
            value: Bytes::from_static(b"v"),
        });

        // Assert commit succeeds
        let c = actor.handle(KvMessage::Commit { tx_id });
        assert!(matches!(c, KvResponse::CommitOk));
    }

    // Sanity-check: begin read-only txn and verify one key exists
    let b = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "scale".to_string(),
        area: "kv".to_string(),
        resource: "batch".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx = match b { KvResponse::BeginOk { tx_id } => tx_id, _ => panic!("Begin failed") };

    let get = actor.handle(KvMessage::Get {
        tx_id: tx,
        route_family: RouteFamily::new(1),
        resource: "batch".to_string(),
        key: Bytes::from(format!("k{:04}", 42)),
    });

    match get {
        KvResponse::GetResult { found: true, .. } => {},
        _ => panic!("Expected a stored key from batch puts"),
    }

    actor.handle(KvMessage::Rollback { tx_id: tx });
}
