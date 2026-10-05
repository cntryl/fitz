use super::*;

fn setup_domains() -> Arc<BrokerDomains> {
    let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
    setup(
        &Arc::new(Router::new()),
        &store,
        &crate::control::admin::read_model::AdminReadModel::new(),
        &domain_setup_options(),
    )
    .expect("setup domains")
}

#[test]
fn should_keep_domain_usable_after_one_family_fails() {
    // Arrange
    let domains = setup_domains();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);

    // Act
    domains.panic_rpc_family_actor_for_failpoint(RouteFamily::new(1));
    while domains
        .rpc
        .family_health_snapshot()
        .failed_families
        .is_empty()
        && std::time::Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    let permanently_failed = domains.has_permanently_failed_domain();
    let snapshot = domains.rpc.family_health_snapshot();

    // Assert
    assert_eq!(snapshot.failed_families, vec![RouteFamily::new(1)]);
    assert_eq!(snapshot.healthy_families.len(), 6);
    assert!(!permanently_failed);
}

#[test]
fn should_report_domain_unusable_after_all_its_families_fail() {
    // Arrange
    let domains = setup_domains();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);

    // Act
    domains.panic_rpc_actor_for_failpoint();
    wait_for_named_domain_failure(&domains, "rpc");
    while !domains.has_permanently_failed_domain() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let permanently_failed = domains.has_permanently_failed_domain();
    let snapshot = domains.rpc.family_health_snapshot();

    // Assert
    assert_eq!(snapshot.failed_families.len(), 7);
    assert!(snapshot.healthy_families.is_empty());
    assert!(permanently_failed);
}

#[test]
fn should_report_domains_unusable_after_stop() {
    // Arrange
    let domains = setup_domains();

    // Act
    domains.stop();
    let permanently_failed = domains.has_permanently_failed_domain();
    let snapshots = domains.health_snapshots();

    // Assert
    assert!(permanently_failed);
    assert!(snapshots
        .iter()
        .all(|snapshot| !snapshot.has_usable_family()));
}
