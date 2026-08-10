use super::*;

#[test]
fn should_support_local_storage_mode() {
    // Arrange
    let config = BootConfig::with_local_storage("/data/fitz");

    // Act
    match &config.storage_mode {
        crate::boot::runtime::StorageMode::LocalDisk { db_path } => {
            // Assert
            assert_eq!(db_path, "/data/fitz");
        }
        _ => panic!("Expected local disk storage mode"),
    }
}

#[tokio::test]
async fn should_reopen_storage_given_clean_shutdown() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-local");
    let config = BootConfig::with_local_storage(db_path.to_string_lossy().to_string());

    // Act
    let store = init(&config).await.expect("open first store");
    let cf = store
        .get_column_family("tenant_default")
        .expect("tenant_default cf");
    write_marker(store.as_ref(), cf.id(), b"marker", b"value");
    shutdown_store(store);

    let reopened = init(&config).await.expect("reopen store");
    let reopened_cf = reopened
        .get_column_family("tenant_default")
        .expect("tenant_default cf after reopen");

    // Assert
    assert_eq!(
        read_marker(reopened.as_ref(), reopened_cf.id(), b"marker"),
        Some(b"value".to_vec())
    );
}

#[tokio::test]
async fn should_retry_local_disk_open_given_active_writer_lease_when_holder_releases() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-local-retry");
    let config = BootConfig::with_local_storage(db_path.to_string_lossy().to_string());
    let store = init(&config).await.expect("open first store");
    let release_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        shutdown_store(store);
    });

    // Act
    let reopened = tokio::time::timeout(Duration::from_secs(5), init(&config))
        .await
        .expect("local disk retry should not hang")
        .expect("reopen store after retry");

    // Assert
    release_task.await.expect("release first store");
    shutdown_store(reopened);
}

#[tokio::test]
async fn should_cancel_local_disk_lease_retry_given_shutdown_request() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-local-retry-cancel");
    let config = BootConfig::with_local_storage(db_path.to_string_lossy().to_string());
    let holder = init(&config).await.expect("open lease holder");
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(None);
    let mut contender = Box::pin(init_with_shutdown(&config, shutdown_rx));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut contender)
            .await
            .is_err(),
        "contender must remain pending while the writer lease is held"
    );

    // Act
    shutdown_tx
        .send(Some(crate::boot::shutdown::ShutdownSignal::Sigterm))
        .expect("request shutdown");
    let outcome = tokio::time::timeout(Duration::from_secs(2), contender)
        .await
        .expect("lease retry should observe shutdown")
        .expect("lease retry cancellation");

    // Assert
    assert!(matches!(outcome, StorageInitOutcome::ShutdownRequested));
    assert!(
        holder.get_runtime_metrics().is_ok(),
        "holder remains active"
    );
    shutdown_store(holder);
    let reopened = tokio::time::timeout(Duration::from_secs(2), init(&config))
        .await
        .expect("cancelled contender must not retain the writer lease")
        .expect("reopen storage after cancelling contender");
    shutdown_store(reopened);
}

#[tokio::test]
async fn should_wait_for_transient_engine_reference_before_storage_shutdown() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-local-reference-drain");
    let config = BootConfig::with_local_storage(db_path.to_string_lossy().to_string());
    let store = init(&config).await.expect("open store");
    let transient_reference = Arc::clone(&store);
    let release_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(75)).await;
        drop(transient_reference);
    });

    // Act
    crate::boot::storage::shutdown_store(store)
        .await
        .expect("shutdown should wait for transient reference");
    release_task.await.expect("release transient reference");
    let reopened = init(&config).await.expect("reopen released store");

    // Assert
    assert!(reopened.is_primary_lease_healthy());
    crate::boot::storage::shutdown_store(reopened)
        .await
        .expect("shutdown reopened store");
}

#[test]
fn should_bound_storage_retry_delay_given_initial_and_saturated_attempts() {
    // Arrange
    let minimum_initial_delay = backoff::BASE_BACKOFF;
    let maximum_initial_delay =
        backoff::BASE_BACKOFF + Duration::from_millis(backoff::MAX_JITTER_MS);
    let minimum_saturated_delay = backoff::MAX_BACKOFF;
    let maximum_saturated_delay =
        backoff::MAX_BACKOFF + Duration::from_millis(backoff::MAX_JITTER_MS);

    // Act
    let initial_delay = backoff::retry_delay(0);
    let saturated_delay = backoff::retry_delay(u32::MAX);

    // Assert
    assert!((minimum_initial_delay..=maximum_initial_delay).contains(&initial_delay));
    assert!((minimum_saturated_delay..=maximum_saturated_delay).contains(&saturated_delay));
}
