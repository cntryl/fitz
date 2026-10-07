use super::*;
use crate::protocol::FrameContext;
use crate::runtime::routing::{Route, RouteAddress};
use crate::runtime::Envelope;
use bytes::{BufMut, Bytes};

mod health;

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn domain_setup_options() -> DomainSetupOptions {
    DomainSetupOptions {
        route_families: vec![1, 2, 3, 4, 5, 6, 7],
        schedule_write_policy: crate::domains::WritePolicy::BestEffort,
        queue_write_policy: crate::domains::WritePolicy::BestEffort,
        queue_recovery_write_policy: crate::domains::WritePolicy::Sync,
        queue_fast_flush_interval: Some(std::time::Duration::from_millis(100)),
        queue_fast_local_wal: true,
        request_sync_write_policy: crate::domains::WritePolicy::Sync,
        request_buffered_write_policy: crate::domains::WritePolicy::Buffered,
        rpc_request_timeout: None,
        stream_storage_layout: crate::domains::stream::StreamStorageLayout::default(),
        kv_idle_transaction_ttl: std::time::Duration::from_mins(5),
        schedule_preload_timeout: crate::domains::schedule::DEFAULT_SCHEDULE_PRELOAD_TIMEOUT,
    }
}

fn cloud_domain_setup_options(
    durable_write_policy: crate::domains::WritePolicy,
) -> DomainSetupOptions {
    DomainSetupOptions {
        route_families: vec![1],
        schedule_write_policy: durable_write_policy,
        queue_write_policy: durable_write_policy,
        queue_recovery_write_policy: durable_write_policy,
        queue_fast_flush_interval: None,
        queue_fast_local_wal: false,
        request_sync_write_policy: durable_write_policy,
        request_buffered_write_policy: crate::domains::WritePolicy::CloudAsync,
        rpc_request_timeout: None,
        stream_storage_layout: crate::domains::stream::StreamStorageLayout::default(),
        kv_idle_transaction_ttl: std::time::Duration::from_mins(5),
        schedule_preload_timeout: crate::domains::schedule::DEFAULT_SCHEDULE_PRELOAD_TIMEOUT,
    }
}

fn assert_cloud_domain_bootstrap(prefix: &str, durable_write_policy: crate::domains::WritePolicy) {
    let tempdir = tempfile::TempDir::new().expect("create cloud simulation directory");
    let options =
        cntryl_midge::OpenOptions::cloud_simulated(tempdir.path(), "fitz-domain-bootstrap", prefix)
            .build()
            .expect("build cloud-simulated options");
    let shutdown_budget = options.runtime_response_timeout();
    let store = Arc::new(cntryl_midge::Engine::open(options).expect("open cloud-simulated engine"));
    crate::api::storage_runtime::ensure_route_family(&store, RouteFamily::new(1))
        .expect("provision route family");
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    let domains = setup(
        &router,
        &store,
        &admin_read_model,
        &cloud_domain_setup_options(durable_write_policy),
    )
    .unwrap_or_else(|error| panic!("cloud domain bootstrap failed: {error}"));

    domains.stop();
    router.clear();
    drop(domains);
    drop(router);
    crate::testkit::midge::shutdown_test_engine_with_timeout(store, shutdown_budget);
}

fn encode_kv_begin(route: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(usize_to_u32_saturating(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u8(1);
    payload.put_u8(0);
    Bytes::from(payload)
}

fn receive_kv_response(mailbox: &crate::runtime::Mailbox, label: &str) -> FrameContext {
    mailbox
        .receiver()
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("{label}"))
        .into_payload::<FrameContext>()
        .unwrap_or_else(|| panic!("{label} frame"))
}

fn wait_for_domain_failures(domains: &BrokerDomains) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if domains
            .health_snapshots()
            .iter()
            .all(|snapshot| !snapshot.has_usable_family())
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let snapshots = domains.health_snapshots();
    panic!("domain actors did not fail closed: {snapshots:?}");
}

fn wait_for_named_domain_failure(domains: &BrokerDomains, domain: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        if domains
            .health_snapshots()
            .iter()
            .any(|snapshot| snapshot.domain == domain && !snapshot.has_usable_family())
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let snapshots = domains.health_snapshots();
    panic!("{domain} actor did not fail closed: {snapshots:?}");
}

#[test]
fn should_setup_all_seven_domains() {
    // Arrange
    let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let domains =
        setup(&router, &store, &admin_read_model, &domain_setup_options()).expect("setup domains");

    // Assert
    assert_eq!(domains.health_snapshots().len(), DomainKind::ALL.len());
}

#[test]
fn should_bootstrap_domains_with_background_cloud_write_policy() {
    // Arrange

    // Act
    assert_cloud_domain_bootstrap("background", crate::domains::WritePolicy::CloudAsync);

    // Assert
}

#[test]
fn should_bootstrap_domains_with_strict_cloud_write_policy() {
    // Arrange

    // Act
    assert_cloud_domain_bootstrap("strict", crate::domains::WritePolicy::CloudStrict);

    // Assert
}

#[test]
fn should_register_all_manifest_domains_for_session_cleanup() {
    // Arrange
    let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let _domains =
        setup(&router, &store, &admin_read_model, &domain_setup_options()).expect("setup domains");

    // Assert
    for domain in DomainKind::ALL {
        let result = router.route(Envelope::new(
            RouteAddress::new(RouteFamily::new(1), domain.cleanup_route()),
            crate::runtime::SessionCleanup { session_id: 42 },
        ));
        assert!(
            result.is_ok(),
            "expected {} cleanup route to be registered",
            domain.as_str()
        );
    }
}

#[test]
fn should_fail_closed_all_domain_actors_after_test_panic_commands() {
    // Arrange
    let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let domains =
        setup(&router, &store, &admin_read_model, &domain_setup_options()).expect("setup domains");

    // Act
    domains.panic_all_domain_actors_for_failpoint();
    wait_for_domain_failures(&domains);
    let snapshots = domains.health_snapshots();

    // Assert
    assert_eq!(snapshots.len(), DomainKind::ALL.len());
    assert!(snapshots
        .iter()
        .all(|snapshot| snapshot.healthy_families.is_empty()));
    // Family-sharded domains are provisioned with 7 route families here
    // (`domain_setup_options`) and must be
    // panicked on *every* family to reach full exhaustion -- see
    // the panic failpoints on `RpcDomain`/`StreamDomain` --
    // so their panic_count legitimately lands at 7, not 1.
    for snapshot in &snapshots {
        let expected_panic_count = match snapshot.domain {
            "kv" | "queue" | "notice" | "rpc" | "lease" | "schedule" | "stream" => 7,
            _ => unreachable!("unknown domain in health inventory"),
        };
        assert_eq!(
            snapshot.panic_count, expected_panic_count,
            "unexpected panic_count for domain {}",
            snapshot.domain
        );
    }
    assert!(snapshots
        .iter()
        .all(|snapshot| snapshot.failed_families.len() == 7));
    let admins = domains.admin_ports();
    assert_eq!(admins.kv_active_transaction_count(), 0);
    assert_eq!(admins.queue_ready_message_count(), 0);
    assert_eq!(admins.stream_count(), 0);
    assert_eq!(admins.rpc_worker_count(), 0);
    assert_eq!(admins.lease_count(), 0);
    assert_eq!(admins.schedule_count(), 0);
}

#[test]
fn should_reject_new_work_after_domain_actor_panic() {
    // Arrange
    let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let domains =
        setup(&router, &store, &admin_read_model, &domain_setup_options()).expect("setup domains");
    let family = RouteFamily::new(1);
    let warmup_route = "kv://acme/app/restart-regression-a";
    let recovery_route = "kv://acme/app/restart-regression-b";
    let warmup_kv_address = RouteAddress::new(family, Route::new(warmup_route));
    let recovery_kv_address = RouteAddress::new(family, Route::new(recovery_route));
    let warmup_session = 101;
    let recovery_session = 202;
    let warmup_inbox = RouteAddress::new(family, Route::new("inbox://session/a"));
    let recovery_inbox = RouteAddress::new(family, Route::new("inbox://session/b"));
    let warmup_mailbox = Arc::new(crate::runtime::Mailbox::new(16));
    let recovery_mailbox = Arc::new(crate::runtime::Mailbox::new(16));
    router.register(warmup_inbox.clone(), warmup_mailbox.clone());
    router.register(recovery_inbox.clone(), recovery_mailbox.clone());
    router
        .route(Envelope::from_route(
            warmup_inbox,
            warmup_kv_address,
            FrameContext::new(
                warmup_session,
                crate::protocol::frame::ChannelId::Pub,
                crate::protocol::tlv::MessageType::new(crate::protocol::kv::msg_type::BEGIN),
                encode_kv_begin(warmup_route),
                family,
            ),
        ))
        .expect("route session A begin");
    let warmup_response = receive_kv_response(&warmup_mailbox, "session A begin response");
    assert_eq!(warmup_response.payload.first(), Some(&0));

    // Act
    domains.kv.panic_actor_for_failpoint();
    wait_for_named_domain_failure(&domains, "kv");
    let result = router.route(Envelope::from_route(
        recovery_inbox,
        recovery_kv_address,
        FrameContext::new(
            recovery_session,
            crate::protocol::frame::ChannelId::Pub,
            crate::protocol::tlv::MessageType::new(crate::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(recovery_route),
            family,
        ),
    ));

    // Assert
    assert!(domains
        .health_snapshots()
        .iter()
        .any(|snapshot| snapshot.domain == "kv" && !snapshot.has_usable_family()));
    assert!(matches!(
        result,
        Err(crate::runtime::router::RouteError::DeliveryFailed(
            _,
            crate::runtime::router::DeliveryError::ActorStopped
        ))
    ));
}

#[test]
fn should_enable_periodic_wal_sync_for_local_fast_queue() {
    // Arrange
    let mut config = crate::boot::runtime::BootConfig::new().with_storage_mode(
        crate::boot::runtime::StorageMode::LocalDisk {
            db_path: "unused-local-test".into(),
        },
    );
    config.queue_write_policy = crate::boot::runtime::QueueWritePolicy::Fast;
    // Act
    let enabled = queue_fast_local_wal(&config);
    // Assert
    assert!(enabled);
}

#[test]
fn should_keep_cloud_fast_queue_on_sst_flushing() {
    // Arrange
    let cloud = crate::boot::runtime::CloudStorageConfig {
        provider_name: "aws-s3".into(),
        provider_config: cntryl_midge::AwsS3Config::new("unused-bucket", "us-east-1").into(),
        prefix: None,
        local_cache_path: "unused-cache".into(),
    };
    let mut config = crate::boot::runtime::BootConfig::new().with_storage_mode(
        crate::boot::runtime::StorageMode::CloudBacked(Box::new(cloud)),
    );
    config.queue_write_policy = crate::boot::runtime::QueueWritePolicy::Fast;
    // Act
    let enabled = queue_fast_local_wal(&config);
    // Assert
    assert!(!enabled);
}

#[test]
fn should_keep_memory_fast_queue_on_existing_flush_path() {
    // Arrange
    let mut config = crate::boot::runtime::BootConfig::new()
        .with_storage_mode(crate::boot::runtime::StorageMode::Memory);
    config.queue_write_policy = crate::boot::runtime::QueueWritePolicy::Fast;
    // Act
    let enabled = queue_fast_local_wal(&config);
    // Assert
    assert!(!enabled);
}

#[test]
fn should_keep_strict_local_queue_off_fast_wal_path() {
    // Arrange
    let mut config = crate::boot::runtime::BootConfig::new().with_storage_mode(
        crate::boot::runtime::StorageMode::LocalDisk {
            db_path: "unused-local-test".into(),
        },
    );
    config.queue_write_policy = crate::boot::runtime::QueueWritePolicy::Strict;
    // Act
    let enabled = queue_fast_local_wal(&config);
    // Assert
    assert!(!enabled);
}
