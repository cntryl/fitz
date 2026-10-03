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

#[test]
fn should_bound_latency_projection_resources_and_evict_oldest_entry() {
    // Arrange
    let projection = KvAdminProjection::new(AdminReadModel::new());
    let keys = (0..=super::super::admin_projection::KV_LATENCY_RESOURCE_LIMIT)
        .map(|index| KvResourceLockKey::new(1, "realm", "area", &format!("resource-{index}")))
        .collect::<Vec<_>>();
    for key in &keys[..super::super::admin_projection::KV_LATENCY_RESOURCE_LIMIT] {
        projection.record_read_latency(key, 7.5);
    }

    // Act
    projection.record_read_latency(keys.last().expect("new resource key"), 3.0);
    let evicted = projection.latency_snapshots(&keys[0]);
    let retained = projection.latency_snapshots(keys.last().expect("new resource key"));

    // Assert
    assert_eq!(
        projection.latency_resource_count(),
        super::super::admin_projection::KV_LATENCY_RESOURCE_LIMIT
    );
    assert!(evicted.0.avg_ms.abs() < f64::EPSILON);
    assert!(evicted.0.p95_ms.abs() < f64::EPSILON);
    assert!((retained.0.avg_ms - 3.0).abs() < f64::EPSILON);
}
