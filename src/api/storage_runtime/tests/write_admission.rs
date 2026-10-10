use super::*;

fn stalled_engine() -> cntryl_midge::Engine {
    // Memory mode cannot flush away this pressure. Disabling background
    // compaction alone does not prevent Midge's critical L0 relief.
    let engine = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::in_memory()
            .with_memtable_size_limit(64 * 1024)
            .build()
            .unwrap(),
    )
    .unwrap();
    let family = engine.create_column_family("tenant_default").unwrap();
    let pressure = engine.create_column_family("pressure").unwrap();
    write_marker(&engine, pressure.id(), b"pressure", &vec![7; 256 * 1024]);
    // Refresh the route family's admission hint after filling the other CF.
    write_marker(&engine, family.id(), b"seed", b"value");
    assert!(!engine
        .wait_for_write_stall_clear(family.id(), Duration::from_millis(1))
        .unwrap());
    engine
}

#[test]
fn should_fail_startup_admission_when_storage_pressure_cannot_clear() {
    // Arrange
    let mut engine = stalled_engine();

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
