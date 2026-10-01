use super::*;
use crate::snapshot::SnapshotKvEntry;
use crate::snapshot::{SnapshotDomain, SnapshotSelector};

fn commit_value(
    actor: &mut KvActor,
    scope: KvResourceScope,
    key: &'static [u8],
    value: &'static [u8],
) {
    let tx_id = begin_with_scope(actor, scope.clone());
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        }),
        KvResponse::PutOk
    ));
    assert!(matches!(
        actor.handle(KvMessage::Commit { tx_id, scope }),
        KvResponse::CommitOk
    ));
}

#[test]
fn should_capture_only_committed_rows_from_one_kv_resource() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "acme", "jobs", "orders");
    let committed_tx = begin_with_scope(&mut actor, scope.clone());
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id: committed_tx,
            scope: scope.clone(),
            key: Bytes::from_static(b"committed"),
            value: Bytes::from_static(b"value"),
        }),
        KvResponse::PutOk
    ));
    assert!(matches!(
        actor.handle(KvMessage::Commit {
            tx_id: committed_tx,
            scope: scope.clone(),
        }),
        KvResponse::CommitOk
    ));
    commit_value(
        &mut actor,
        KvResourceScope::new(RouteFamily::new(1), "acme", "jobs", "shipments"),
        b"sibling",
        b"excluded",
    );
    commit_value(
        &mut actor,
        KvResourceScope::new(RouteFamily::new(2), "acme", "jobs", "orders"),
        b"other-family",
        b"excluded",
    );
    let uncommitted_tx = begin_with_scope(&mut actor, scope.clone());
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id: uncommitted_tx,
            scope: scope.clone(),
            key: Bytes::from_static(b"pending"),
            value: Bytes::from_static(b"uncommitted"),
        }),
        KvResponse::PutOk
    ));

    // Act
    let selector = SnapshotSelector::new(
        SnapshotDomain::Kv,
        scope.route_family,
        "kv://acme/jobs/orders",
    )
    .expect("valid resource selector");
    let snapshot = KvActor::snapshot_committed_resources(&actor.store, &selector);

    // Assert
    assert_eq!(
        snapshot.expect("committed resource snapshot"),
        [crate::snapshot::SnapshotKvResource {
            route: "kv://acme/jobs/orders".to_string(),
            entries: vec![SnapshotKvEntry {
                key: b"committed".to_vec(),
                value: b"value".to_vec(),
            }],
        }]
    );
}
