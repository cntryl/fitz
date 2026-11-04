//! A timed-out DROP may apply after a metadata writer takes its snapshot.

use super::*;
use crate::domains::kv::inventory::{encode_estimate, KvInventoryEstimate};
use crate::domains::kv::{KvActor, KvDomain, KvDropScope};
use crate::runtime::routing::RouteFamily;
use crate::runtime::Router;

fn seed_scope(store: &KvStore, complete: Option<bool>) -> (Vec<u8>, Vec<u8>) {
    let data_key = KvActor::encode_scoped_key(
        &KvActor::realm_resource_prefix("acme", "app", "rows"),
        b"key",
    );
    let inventory_key = KvActor::inventory_metadata_key("acme", "app", "rows");
    let mut tx = store.begin(1, TxMode::ReadWrite).unwrap();
    tx.put(data_key.clone(), b"value".to_vec()).unwrap();
    if let Some(complete) = complete {
        tx.put(
            inventory_key.clone(),
            encode_estimate(KvInventoryEstimate {
                estimated_record_count: 1,
                estimated_storage_bytes: 8,
                estimate_complete: complete,
            }),
        )
        .unwrap();
    }
    tx.commit(WritePolicy::Sync).unwrap();
    (data_key, inventory_key)
}

fn drop_scope() -> KvDropScope {
    KvDropScope::parse(RouteFamily::new(1), "kv://acme/app/rows").unwrap()
}

fn return_before_drop_applies(store: &KvStore) {
    let _hook = write_admission::inject_admission(Box::new(|_, _| {
        Err(cntryl_midge::MidgeError::OutcomeUnknown(
            "DROP queued, application delayed".into(),
        ))
    }));
    let result = store.drop_scope(&drop_scope(), WritePolicy::Sync);
    assert!(matches!(result, Err(KvError::BackendError(_))));
}

fn apply_delayed_drop(engine: &cntryl_midge::Engine) -> cntryl_midge::MidgeResult<()> {
    let mut tx = engine.begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)?;
    for prefix in KvActor::drop_prefixes(&drop_scope()) {
        tx.delete_range(prefix.clone(), KvActor::prefix_range_end(&prefix))?;
    }
    tx.commit(cntryl_midge::WriteOptions::sync())
}

fn assert_stale_refresh_is_rejected(initial_metadata: Option<bool>) {
    let (engine, store, _directory) = local_store();
    let (data_key, inventory_key) = seed_scope(&store, initial_metadata);
    let sink = KvDomain::new(
        store.clone(),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    return_before_drop_applies(&store);
    let delayed_engine = engine.clone();
    let _hook = write_admission::inject_admission(Box::new(move |_, _| {
        apply_delayed_drop(&delayed_engine)?;
        Ok(true)
    }));

    let refresh = sink.admin_inventory_resource(RouteFamily::new(1), "acme", "app", "rows");
    let retry = sink.admin_inventory_resource(RouteFamily::new(1), "acme", "app", "rows");
    let tx = store.begin(1, TxMode::ReadOnly).unwrap();
    let data = tx.get(&data_key).unwrap();
    let inventory = tx.get(&inventory_key).unwrap();
    drop(tx);
    sink.stop();
    drop(sink);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);

    assert!(
        refresh.is_err(),
        "a refresh based on pre-DROP rows must not publish its estimate"
    );
    assert!(data.is_none());
    assert!(
        inventory.is_none(),
        "the delayed DROP must not be followed by stale inventory"
    );
    assert!(
        retry.unwrap().is_none(),
        "a fresh inventory read must observe the completed DROP"
    );
}

#[test]
fn should_reject_a_stale_refresh_when_an_indeterminate_drop_applies_after_its_scan() {
    // Arrange
    let initial_metadata = Some(false);
    // Act
    assert_stale_refresh_is_rejected(initial_metadata);
    // Assert
    // The helper checks conflict, absent values/metadata, and a successful fresh retry.
}

#[test]
fn should_reject_a_stale_refresh_when_a_delayed_drop_covers_initially_absent_metadata() {
    // Arrange
    let initial_metadata = None;
    // Act
    assert_stale_refresh_is_rejected(initial_metadata);
    // Assert
    // A covering range deletion must invalidate the original absence assertion too.
}

#[test]
fn should_reject_a_stale_repair_when_an_indeterminate_drop_applies_before_its_commit() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let (data_key, inventory_key) = seed_scope(&store, Some(true));
    return_before_drop_applies(&store);
    let delayed_engine = engine.clone();
    let _hook = write_admission::inject_admission(Box::new(move |_, _| {
        apply_delayed_drop(&delayed_engine)?;
        Ok(true)
    }));
    let scope =
        crate::domains::kv::KvResourceScope::new(RouteFamily::new(1), "acme", "app", "rows");

    // Act
    let repair = KvActor::mark_inventory_incomplete(&store, 1, &scope, WritePolicy::Sync);
    let tx = store.begin(1, TxMode::ReadOnly).unwrap();
    let data = tx.get(&data_key).unwrap();
    let inventory = tx.get(&inventory_key).unwrap();
    drop(tx);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);

    // Assert
    assert!(
        matches!(repair, Err(KvError::Conflict(_))),
        "a repair must not recreate metadata removed by a delayed DROP"
    );
    assert!(data.is_none());
    assert!(inventory.is_none());
}

#[test]
fn should_allow_a_refresh_to_commit_before_an_indeterminate_drop_applies() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let (data_key, inventory_key) = seed_scope(&store, Some(false));
    let sink = KvDomain::new(
        store.clone(),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    return_before_drop_applies(&store);

    // Act
    let refresh = sink.admin_inventory_resource(RouteFamily::new(1), "acme", "app", "rows");
    apply_delayed_drop(&engine).unwrap();
    let after_drop = sink.admin_inventory_resource(RouteFamily::new(1), "acme", "app", "rows");
    let tx = store.begin(1, TxMode::ReadOnly).unwrap();
    let data = tx.get(&data_key).unwrap();
    let inventory = tx.get(&inventory_key).unwrap();
    drop(tx);
    sink.stop();
    drop(sink);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);

    // Assert
    let estimate = refresh
        .unwrap()
        .expect("a pending DROP must not indefinitely fence inventory");
    assert_eq!(estimate.estimated_record_count, 1);
    assert!(estimate.estimate_complete);
    assert!(after_drop.unwrap().is_none());
    assert!(data.is_none());
    assert!(inventory.is_none());
}

#[test]
fn should_recreate_inventory_with_a_data_commit_after_an_indeterminate_drop_applies() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let (old_key, inventory_key) = seed_scope(&store, Some(false));
    return_before_drop_applies(&store);
    let scope =
        crate::domains::kv::KvResourceScope::new(RouteFamily::new(1), "acme", "app", "rows");
    let mut actor = KvActor::new(store.clone());
    let crate::domains::kv::KvResponse::BeginOk { tx_id } =
        actor.handle(crate::domains::kv::KvMessage::Begin {
            scope: scope.clone(),
            mode: TxMode::ReadWrite,
        })
    else {
        panic!("write transaction must begin")
    };
    let inserted = actor.handle(crate::domains::kv::KvMessage::Put {
        tx_id,
        scope: scope.clone(),
        key: Bytes::from_static(b"new"),
        value: Bytes::from_static(b"replacement"),
    });
    assert!(matches!(inserted, crate::domains::kv::KvResponse::PutOk));
    let delayed_engine = engine.clone();
    let _hook = write_admission::inject_admission(Box::new(move |_, _| {
        apply_delayed_drop(&delayed_engine)?;
        Ok(true)
    }));

    // Act
    let committed = actor.handle(crate::domains::kv::KvMessage::Commit {
        tx_id,
        scope,
        persistence: crate::domains::CommitPersistence::Sync,
    });
    let tx = store.begin(1, TxMode::ReadOnly).unwrap();
    let new_key = KvActor::encode_scoped_key(
        &KvActor::realm_resource_prefix("acme", "app", "rows"),
        b"new",
    );
    let old = tx.get(&old_key).unwrap();
    let replacement = tx.get(&new_key).unwrap();
    let inventory = tx.get(&inventory_key).unwrap();
    drop(tx);
    drop(actor);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);

    // Assert
    assert!(matches!(
        committed,
        crate::domains::kv::KvResponse::CommitOk
    ));
    assert!(old.is_none());
    assert_eq!(replacement, Some(Bytes::from_static(b"replacement")));
    let estimate = crate::domains::kv::inventory::decode_estimate(
        inventory
            .as_deref()
            .expect("committed replacement data must have inventory metadata"),
    )
    .unwrap();
    assert!(!estimate.estimate_complete);
}
