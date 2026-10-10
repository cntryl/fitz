use super::*;

fn stalled_engine() -> (cntryl_midge::Engine, TempDir) {
    let directory = TempDir::new().unwrap();
    let engine = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::local(directory.path())
            .background_compaction(false)
            .build()
            .unwrap(),
    )
    .unwrap();
    let family = engine.create_column_family("tenant_default").unwrap();
    for index in 0_u32..64 {
        let mut tx = engine
            .begin_tx(family.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(index.to_be_bytes().to_vec(), b"value".to_vec(), None)
            .unwrap();
        match tx.commit(WriteOptions::sync()) {
            Ok(()) => engine.flush_cf(&family).unwrap(),
            Err(cntryl_midge::MidgeError::WriteStall(_)) => return (engine, directory),
            Err(error) => panic!("unexpected write failure: {error}"),
        }
    }
    panic!("expected a real Midge L0 admission rejection");
}

#[test]
fn should_fail_startup_admission_when_replayed_l0_pressure_cannot_clear() {
    // Arrange
    let (mut engine, _directory) = stalled_engine();

    // Act
    let result = ensure_startup_write_admission(&engine, Duration::from_millis(10));
    engine.shutdown(Duration::from_secs(2)).unwrap();

    // Assert
    assert!(
        result.is_err(),
        "startup must not expose write-rejected domain initialization"
    );
}

#[test]
fn should_admit_startup_when_all_existing_families_can_accept_writes() {
    // Arrange
    let engine = crate::testkit::create_test_engine_with_cfs(vec![1, 2]);

    // Act
    let result = ensure_startup_write_admission(&engine, Duration::from_secs(1));

    // Assert
    assert!(result.is_ok());
}
