use super::*;

#[test]
fn should_reject_layout_activation_with_orphaned_deletion_state() {
    // Arrange
    let deletion_rows = [
        (
            super::super::drop_scope::cutoff_key("acme", None, None),
            1_u64.to_le_bytes().to_vec(),
        ),
        (
            super::super::drop_reclamation::progress_key(),
            [0_u64.to_le_bytes(), 1_u64.to_le_bytes()].concat(),
        ),
        (
            crate::utils::storage_key::domain_marker_encoder(
                "",
                crate::utils::storage_key::DomainKeyspace::Stream,
                KeyPrefix::DropRevision as u8,
                0,
            )
            .into_vec(),
            1_u64.to_le_bytes().to_vec(),
        ),
    ];
    for (key, value) in deletion_rows {
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let mut transaction = store
            .db
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        transaction.put(key, value, None).unwrap();
        transaction
            .commit(cntryl_midge::WriteOptions::sync())
            .unwrap();

        // Act
        let activation = store.inspect_and_activate_layout_for_family(1);

        // Assert
        let error = activation.expect_err("orphaned deletion rows are not a fresh family");
        assert!(
            error.starts_with("ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED"),
            "{error}"
        );
        assert!(store
            .db
            .read_tx(1)
            .get(&encode_stream_layout_marker_key())
            .unwrap()
            .is_none());
    }
}

#[test]
fn should_retry_a_test_session_without_a_global_gap_after_a_cutoff_read_error() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store.ensure_layout_activation_for_family(1).unwrap();
    let session = store
        .begin_session(1, "acme", "logs", "events", None)
        .unwrap();
    store
        .append_to_session(1, session, single_event(b"retry").remove(0))
        .unwrap();
    StreamStore::fail_next_drop_cutoff_read_for_tests();

    // Act
    let failed = store.commit_session(1, session, 0, 0, 0, StreamWriteMode::Sync);
    let retried = store.commit_session(1, session, 0, 0, 0, StreamWriteMode::Sync);

    // Assert
    assert!(failed.is_err(), "{failed:?}");
    assert_eq!(
        retried
            .expect("cutoff read failure must preserve a retryable session")
            .first_global_offset,
        0
    );
}
