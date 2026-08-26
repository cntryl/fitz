use super::*;

#[test]
fn should_refresh_projection_when_marked_dirty() {
    // Arrange
    let read_model = AdminReadModel::new();
    let projection = KvAdminProjection::new(read_model.clone());
    projection.mark_dirty();

    // Act
    projection.refresh_if_dirty(|| {
        vec![KvTransaction::snapshot(
            1,
            41,
            7,
            "acme",
            "app",
            "users",
            "2026-07-01T00:00:00Z",
        )]
    });

    // Assert
    assert_eq!(read_model.kv_transactions(None).len(), 1);
}

#[test]
fn should_record_projection_latency_by_operation_kind() {
    // Arrange
    let read_model = AdminReadModel::new();
    let projection = KvAdminProjection::new(read_model);
    let key = KvResourceLockKey::new(1, "acme", "app", "users");

    // Act
    projection.record_write_latency(&key, 5.0);
    projection.record_read_latency(&key, 3.0);
    let (reads, writes) = projection.latency_snapshots(&key);

    // Assert
    assert!((reads.avg_ms - 3.0).abs() < f64::EPSILON);
    assert!((writes.avg_ms - 5.0).abs() < f64::EPSILON);
}
