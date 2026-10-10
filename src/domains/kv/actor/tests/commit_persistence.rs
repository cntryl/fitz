//! Real local WAL boundaries selected when completing a KV transaction.
use crate::domains::kv::{KvActor, KvMessage, KvResourceScope, KvResponse, TxMode};
use crate::domains::CommitPersistence;
use crate::runtime::routing::RouteFamily;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

fn commit_with_policy(policy: CommitPersistence) -> (u64, u64) {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(directory.path())
                .background_compaction(false)
                .lease_ttl(Duration::from_secs(1))
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    let family = engine.create_column_family("tenant_default").unwrap();
    let scope = KvResourceScope::new(RouteFamily::new(family.id()), "test", "commit", "policy");
    let mut actor = KvActor::new(engine.clone());
    let KvResponse::BeginOk { tx_id } = actor.handle(KvMessage::Begin {
        scope: scope.clone(),
        mode: TxMode::ReadWrite,
    }) else {
        panic!("BEGIN failed")
    };
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
        }),
        KvResponse::PutOk
    ));
    let before = engine.metrics().get_runtime_metrics().unwrap();
    assert!(matches!(
        actor.handle(KvMessage::Commit {
            tx_id,
            scope,
            persistence: policy,
        }),
        KvResponse::CommitOk
    ));
    let after = engine.metrics().get_runtime_metrics().unwrap();
    drop(actor);
    crate::testkit::midge::shutdown_test_engine(engine);
    (
        after.wal_append_count - before.wal_append_count,
        after.wal_fsync_count - before.wal_fsync_count,
    )
}

#[test]
fn should_use_buffered_wal_when_kv_commit_selects_buffered() {
    // Arrange
    let policy = CommitPersistence::Buffered;

    // Act
    let (appends, syncs) = commit_with_policy(policy);

    // Assert
    assert!(appends > 0);
    assert_eq!(syncs, 0);
}

#[test]
fn should_wait_for_synced_wal_when_kv_commit_selects_sync() {
    // Arrange
    let policy = CommitPersistence::Sync;

    // Act
    let (appends, syncs) = commit_with_policy(policy);

    // Assert
    assert!(appends > 0);
    assert!(syncs > 0);
}
