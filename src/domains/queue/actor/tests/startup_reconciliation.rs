use super::*;

fn read_queue_validation_row(store: &cntryl_midge::Engine, suffix: &[u8]) -> Option<Bytes> {
    read_queue_validation_row_for_realm(store, "test", suffix)
}

fn read_queue_validation_row_for_realm(
    store: &cntryl_midge::Engine,
    realm: &str,
    suffix: &[u8],
) -> Option<Bytes> {
    let txn = store
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin queue validation read tx");
    txn.get(&storage_key::prefixed_key(
        realm,
        DomainKeyspace::Queue,
        suffix,
    ))
    .expect("read queue validation row")
}

fn put_queue_validation_row_for_realm(
    store: &cntryl_midge::Engine,
    realm: &str,
    suffix: &[u8],
    value: Vec<u8>,
) {
    let mut txn = store
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin queue validation write tx");
    txn.put(
        storage_key::prefixed_key(realm, DomainKeyspace::Queue, suffix),
        value,
        None,
    )
    .expect("write queue validation row");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit queue validation row");
}

#[test]
fn should_reconcile_missing_queue_body_for_fast_policy_during_preflight() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let header_suffix = authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_HEADER, Some(1));
    let index_meta_suffix =
        authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_INDEX_META, None);
    put_queue_validation_row(
        store.as_ref(),
        &header_suffix,
        QueueActor::encode_record_header(&QueueRecord::ready(
            Bytes::from_static(b"payload"),
            1,
            1,
            1_700_000_000_000,
        )),
    );
    put_queue_validation_row(store.as_ref(), &index_meta_suffix, vec![1]);

    // Act
    QueueActor::prepare_persisted_state_for_existing_families(
        store.as_ref(),
        cntryl_midge::WriteOptions::best_effort(),
        cntryl_midge::WriteOptions::sync(),
    )
    .expect("fast queue preflight should reconcile a missing body");

    // Assert
    assert!(read_queue_validation_row(store.as_ref(), &header_suffix).is_none());
    assert!(read_queue_validation_row(store.as_ref(), &index_meta_suffix).is_none());
    QueueActor::validate_persisted_state_for_existing_families(store.as_ref())
        .expect("reconciled queue state should pass strict validation");
}

#[test]
fn should_reconcile_orphan_queue_body_for_fast_policy_during_preflight() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let body_suffix = authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_BODY, Some(1));
    let ready_index_suffix =
        authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_READY_INDEX, Some(1));
    put_queue_validation_row(store.as_ref(), &body_suffix, b"payload".to_vec());
    put_queue_validation_row(store.as_ref(), &ready_index_suffix, vec![1]);

    // Act
    QueueActor::prepare_persisted_state_for_existing_families(
        store.as_ref(),
        cntryl_midge::WriteOptions::best_effort(),
        cntryl_midge::WriteOptions::sync(),
    )
    .expect("fast queue preflight should reconcile an orphan body");

    // Assert
    assert!(read_queue_validation_row(store.as_ref(), &body_suffix).is_none());
    assert!(read_queue_validation_row(store.as_ref(), &ready_index_suffix).is_none());
    QueueActor::validate_persisted_state_for_existing_families(store.as_ref())
        .expect("reconciled queue state should pass strict validation");
}

#[test]
fn should_reconcile_orphan_queue_body_with_background_cloud_recovery() {
    // Arrange
    let tempdir = tempfile::TempDir::new().expect("create cloud simulation directory");
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::cloud_simulated(
                tempdir.path(),
                "fitz-queue-recovery",
                "background",
            )
            .build()
            .expect("build cloud-simulated options"),
        )
        .expect("open cloud-simulated engine"),
    );
    store
        .create_column_family("tenant_default")
        .expect("create route-family column family");
    let body_suffix = authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_BODY, Some(1));
    let ready_index_suffix =
        authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_READY_INDEX, Some(1));
    for (suffix, value) in [
        (&body_suffix, b"payload".to_vec()),
        (&ready_index_suffix, vec![1]),
    ] {
        let mut txn = store
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin cloud queue seed transaction");
        txn.put(
            storage_key::prefixed_key("test", DomainKeyspace::Queue, suffix),
            value,
            None,
        )
        .expect("write cloud queue seed row");
        txn.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit cloud queue seed row");
    }

    // Act
    let result = QueueActor::prepare_persisted_state_for_existing_families(
        store.as_ref(),
        cntryl_midge::WriteOptions::best_effort(),
        cntryl_midge::WriteOptions::cloud_async(),
    );

    // Assert
    assert!(
        result.is_ok(),
        "cloud queue reconciliation failed: {result:?}"
    );
    assert!(read_queue_validation_row(store.as_ref(), &body_suffix).is_none());
    assert!(read_queue_validation_row(store.as_ref(), &ready_index_suffix).is_none());
}

#[test]
fn should_reject_partial_queue_rows_for_buffered_and_strict_policies() {
    // Arrange
    let policies = [
        cntryl_midge::WriteOptions::buffered(),
        cntryl_midge::WriteOptions::sync(),
        cntryl_midge::WriteOptions::cloud_strict(),
    ];

    // Act
    let errors = policies.map(|policy| {
        let store = create_test_engine_with_cfs(vec![1]);
        let header_suffix = authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_HEADER, Some(1));
        put_queue_validation_row(
            store.as_ref(),
            &header_suffix,
            QueueActor::encode_record_header(&QueueRecord::ready(
                Bytes::from_static(b"payload"),
                1,
                1,
                1_700_000_000_000,
            )),
        );
        QueueActor::prepare_persisted_state_for_existing_families(
            store.as_ref(),
            policy,
            cntryl_midge::WriteOptions::sync(),
        )
        .expect_err("durable queue policies should reject a missing body")
    });

    // Assert
    assert!(errors
        .into_iter()
        .all(|error| error.contains("missing body for split header")));
}

#[test]
fn should_require_durable_write_policy_for_fast_queue_reconciliation() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let header_suffix = authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_HEADER, Some(1));
    put_queue_validation_row(
        store.as_ref(),
        &header_suffix,
        QueueActor::encode_record_header(&QueueRecord::ready(
            Bytes::from_static(b"payload"),
            1,
            1,
            1_700_000_000_000,
        )),
    );

    // Act
    let result = QueueActor::prepare_persisted_state_for_existing_families(
        store.as_ref(),
        cntryl_midge::WriteOptions::best_effort(),
        cntryl_midge::WriteOptions::buffered(),
    );

    // Assert
    assert!(result
        .expect_err("fast queue reconciliation must use a durable write")
        .contains("durable recovery write policy required"));
    assert!(read_queue_validation_row(store.as_ref(), &header_suffix).is_some());
}

#[test]
fn should_leave_complete_split_and_embedded_queue_records_untouched() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let split_header_suffix =
        authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_HEADER, Some(1));
    let split_body_suffix = authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_BODY, Some(1));
    let embedded_header_suffix =
        authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_HEADER, Some(2));
    let split_header = QueueActor::encode_record_header(&QueueRecord::ready(
        Bytes::from_static(b"split"),
        1,
        1,
        1_700_000_000_000,
    ));
    let split_body = b"split".to_vec();
    let embedded_header = QueueActor::encode_legacy_record(&QueueRecord::loaded(
        Bytes::from_static(b"embedded"),
        0,
        1_700_000_000_000,
    ));
    put_queue_validation_row(store.as_ref(), &split_header_suffix, split_header.clone());
    put_queue_validation_row(store.as_ref(), &split_body_suffix, split_body.clone());
    put_queue_validation_row(
        store.as_ref(),
        &embedded_header_suffix,
        embedded_header.clone(),
    );

    // Act
    QueueActor::prepare_persisted_state_for_existing_families(
        store.as_ref(),
        cntryl_midge::WriteOptions::best_effort(),
        cntryl_midge::WriteOptions::sync(),
    )
    .expect("complete queue records should pass fast preflight");

    // Assert
    assert_eq!(
        read_queue_validation_row(store.as_ref(), &split_header_suffix),
        Some(Bytes::from(split_header))
    );
    assert_eq!(
        read_queue_validation_row(store.as_ref(), &split_body_suffix),
        Some(Bytes::from(split_body))
    );
    assert_eq!(
        read_queue_validation_row(store.as_ref(), &embedded_header_suffix),
        Some(Bytes::from(embedded_header))
    );
}

#[test]
fn should_not_match_split_queue_rows_across_realms_during_preflight() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let header_suffix = authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_HEADER, Some(1));
    let body_suffix = authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_BODY, Some(1));
    put_queue_validation_row_for_realm(
        store.as_ref(),
        "alpha",
        &header_suffix,
        QueueActor::encode_record_header(&QueueRecord::ready(
            Bytes::from_static(b"alpha"),
            1,
            1,
            1_700_000_000_000,
        )),
    );
    put_queue_validation_row_for_realm(store.as_ref(), "beta", &body_suffix, b"beta".to_vec());

    // Act
    let result = QueueActor::validate_persisted_state_for_existing_families(store.as_ref());

    // Assert
    assert!(result
        .expect_err("rows in different realms must remain incomplete")
        .contains("missing body for split header"));
}

#[test]
fn should_persist_fast_queue_reconciliation_across_restart() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create queue recovery temp dir");
    let mut store = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::local(temp_dir.path())
            .build()
            .expect("build queue recovery store options"),
    )
    .expect("open queue recovery store");
    let family = store
        .create_column_family("cf_1")
        .expect("create queue recovery column family");
    assert_eq!(family.id(), 1);
    let header_suffix = authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_HEADER, Some(1));
    put_queue_validation_row_for_realm(
        &store,
        "test",
        &header_suffix,
        QueueActor::encode_record_header(&QueueRecord::ready(
            Bytes::from_static(b"payload"),
            1,
            1,
            1_700_000_000_000,
        )),
    );
    QueueActor::prepare_persisted_state_for_existing_families(
        &store,
        cntryl_midge::WriteOptions::best_effort(),
        cntryl_midge::WriteOptions::sync(),
    )
    .expect("reconcile fast queue state before restart");
    store
        .shutdown(std::time::Duration::from_secs(2))
        .expect("shutdown queue recovery store");

    // Act
    let reopened = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::local(temp_dir.path())
            .build()
            .expect("build reopened queue recovery store options"),
    )
    .expect("reopen queue recovery store");
    let result = QueueActor::validate_persisted_state_for_existing_families(&reopened);

    // Assert
    result.expect("durable reconciliation should survive restart");
    assert!(read_queue_validation_row_for_realm(&reopened, "test", &header_suffix).is_none());
}
