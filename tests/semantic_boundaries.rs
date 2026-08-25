use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const DOMAINS: &[&str] = &[
    "kv", "lease", "notice", "queue", "rpc", "schedule", "stream",
];
const SYNC_CORE_DIRS: &[&str] = &["session", "runtime", "protocol", "domains"];
const SYNC_CORE_ASYNC_FORBIDDEN: &[&str] = &[
    "async fn",
    "async move",
    "async {",
    ".await",
    "tokio::",
    "async_trait",
    "futures::",
    "futures_util",
];
const SYNC_CORE_TRANSPORT_FORBIDDEN: &[&str] = &[
    "hyper::",
    "hyper_util::",
    "http_body_util::",
    "axum::",
    "warp::",
    "reqwest::",
    "tungstenite::",
    "tokio_tungstenite",
    "hyper_tungstenite",
    "crate::api::admin::",
    "crate::api::handlers::",
    "crate::api::http::",
    "crate::api::tcp::",
    "crate::api::transport::",
    "crate::api::ws::",
];
const SYNC_CORE_API_FORBIDDEN: &[&str] = &["crate::api::"];
const ADMIN_BOUNDARY_FORBIDDEN: &[&str] = &[
    "crate::boot::domains::",
    "crate::domains::kv::sink",
    "crate::domains::queue::sink",
    "crate::domains::notice::sink",
    "crate::domains::stream::sink",
    "crate::domains::rpc::sink",
    "crate::domains::lease::sink",
    "crate::domains::schedule::sink",
    "crate::domains::kv::actor",
    "crate::domains::queue::actor",
    "crate::domains::notice::actor",
    "crate::domains::stream::actor",
    "crate::domains::rpc::actor",
    "crate::domains::lease::actor",
    "crate::domains::schedule::actor",
];
const ADMIN_BOUNDARY_ALLOWED: &[&str] = &[
    "crate::domains::kv::sink::AdminKvRowsRequest",
    "crate::domains::stream::sink::AdminStreamReadRequest",
];
const PRODUCTION_RUST_LINE_LIMIT: usize = 1_000;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct OwnedSourceFile {
    owner: &'static str,
    path: PathBuf,
}

#[test]
fn should_keep_sync_core_synchronous() {
    // Arrange
    let repo_root = repo_root();
    let files = sync_core_source_files(&repo_root);

    // Act
    let report = report_for_patterns(&repo_root, &files, SYNC_CORE_ASYNC_FORBIDDEN);

    // Assert
    assert!(
        report.is_empty(),
        "found async constructs outside src/api:\n{report}"
    );
}

#[test]
fn should_keep_transport_plus_admin_frameworks_out_of_sync_core() {
    // Arrange
    let repo_root = repo_root();
    let files = sync_core_source_files(&repo_root);

    // Act
    let report = report_for_patterns(&repo_root, &files, SYNC_CORE_TRANSPORT_FORBIDDEN);

    // Assert
    assert!(
        report.is_empty(),
        "found transport or admin framework dependencies in sync core:\n{report}"
    );
}

#[test]
fn should_keep_sync_core_independent_from_api_modules() {
    // Arrange
    let repo_root = repo_root();
    let files = sync_core_source_files(&repo_root);

    // Act
    let report = report_for_patterns(&repo_root, &files, SYNC_CORE_API_FORBIDDEN);

    // Assert
    assert!(
        report.is_empty(),
        "sync core must not depend on src/api modules:\n{report}"
    );
}

#[test]
fn should_keep_protocol_plus_domains_on_dispatch_boundary() {
    // Arrange
    let repo_root = repo_root();
    let protocol_files = source_files_under(&repo_root.join("src").join("protocol"));
    let domain_files = source_files_under(&repo_root.join("src").join("domains"));

    // Act
    let protocol_report = report_for_patterns(&repo_root, &protocol_files, &["crate::domains::"]);
    let domain_report = report_for_patterns(&repo_root, &domain_files, &["crate::protocol::"]);
    let report = [
        (!protocol_report.is_empty()).then(|| {
            format!("protocol imports domain DTOs outside dispatch::wire:\n{protocol_report}")
        }),
        (!domain_report.is_empty()).then(|| {
            format!(
                "domains import protocol contracts outside dispatch::protocol:\n{domain_report}"
            )
        }),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n");

    // Assert
    assert!(
        report.is_empty(),
        "protocol/domain dependencies must cross through src/dispatch:\n{report}"
    );
}

#[test]
fn should_disallow_direct_cross_domain_references() {
    // Arrange
    let repo_root = repo_root();
    let files = domain_owned_source_files(&repo_root);

    // Act
    let report = format_violation_report(&collect_foreign_domain_reference_violations(
        &repo_root, &files,
    ));

    // Assert
    assert!(
        report.is_empty(),
        "found direct cross-domain references:\n{report}"
    );
}

#[test]
fn should_disallow_foreign_route_scheme_literals() {
    // Arrange
    let repo_root = repo_root();
    let files = domain_owned_source_files(&repo_root);

    // Act
    let report =
        format_violation_report(&collect_foreign_route_scheme_violations(&repo_root, &files));

    // Assert
    assert!(
        report.is_empty(),
        "found foreign route scheme literals:\n{report}"
    );
}

#[test]
fn should_keep_admin_api_on_runtime_facades() {
    // Arrange
    let repo_root = repo_root();
    let files = admin_api_source_files(&repo_root);

    // Act
    let report = report_for_patterns_with_allowed(
        &repo_root,
        &files,
        ADMIN_BOUNDARY_FORBIDDEN,
        ADMIN_BOUNDARY_ALLOWED,
    );

    // Assert
    assert!(
        report.is_empty(),
        "admin API bypasses runtime/admin facades:\n{report}"
    );
}

#[test]
fn should_keep_rpc_route_actor_removed_from_default_surface() {
    // Arrange
    let repo_root = repo_root();
    let rpc_mod = repo_root
        .join("src")
        .join("domains")
        .join("rpc")
        .join("mod.rs");
    let content = read_source_file(&rpc_mod);
    let forbidden_exports = [
        "\npub mod actor;",
        "\npub mod session;",
        "\npub(crate) mod actor;",
        "\npub(crate) mod session;",
        "\npub use actor::RpcRouteActor;",
        "\npub use session::SessionActor;",
        "legacy_actor_tests",
    ];

    // Act
    let mut violations = forbidden_exports
        .iter()
        .filter(|forbidden| content.contains(**forbidden))
        .map(|forbidden| format!("src/domains/rpc/mod.rs exposes `{}`", forbidden.trim()))
        .collect::<Vec<_>>();
    for file_name in ["actor.rs", "session.rs", "legacy_actor_tests.rs"] {
        let path = repo_root
            .join("src")
            .join("domains")
            .join("rpc")
            .join(file_name);
        if path.exists() {
            violations.push(format!(
                "{} still exists",
                relative_display_path(&repo_root, &path)
            ));
        }
    }
    let report = format_violation_report(&violations);

    // Assert
    assert!(
        report.is_empty(),
        "RPC route actor and legacy session helper must stay pruned from the default surface:\n{report}"
    );
}

#[test]
fn should_keep_shadow_notice_surface_removed() {
    // Arrange
    let repo_root = repo_root();
    let notice_dir = repo_root.join("src").join("domains").join("notice");
    let notice_mod = read_source_file(&notice_dir.join("mod.rs"));
    let forbidden_exports = [
        "\npub mod actor;",
        "\npub mod events;",
        "\npub mod session;",
        "\npub use actor::NoticeRouteActor;",
        "\npub use session::SessionActor;",
    ];

    // Act
    let mut violations = forbidden_exports
        .iter()
        .filter(|forbidden| notice_mod.contains(**forbidden))
        .map(|forbidden| format!("src/domains/notice/mod.rs exposes `{}`", forbidden.trim()))
        .collect::<Vec<_>>();
    for relative in [
        "src/domains/notice/actor.rs",
        "src/domains/notice/events.rs",
        "src/domains/notice/session.rs",
        "tests/notice_basics.rs",
        "tests/notice_advanced.rs",
    ] {
        if repo_root.join(relative).exists() {
            violations.push(format!("{relative} retains the shadow Notice surface"));
        }
    }
    let report = format_violation_report(&violations);

    // Assert
    assert!(
        report.is_empty(),
        "shadow Notice actors and events must stay absent:\n{report}"
    );
}

#[test]
fn should_keep_notice_family_state_key_type_safe() {
    // Arrange
    let repo_root = repo_root();
    let sink = read_source_file(
        &repo_root
            .join("src")
            .join("domains")
            .join("notice")
            .join("sink.rs"),
    );

    // Act
    let has_typed_key = sink.contains(
        "HashMap<crate::runtime::routing::RouteFamily, RoutedSubscriptionSet<NoticeSubscription>>",
    );
    let retains_round_trip = sink.contains("RouteFamily::try_from(*family_id)");

    // Assert
    assert!(
        has_typed_key,
        "Notice family state must use RouteFamily keys"
    );
    assert!(
        !retains_round_trip,
        "Notice cleanup must not reconstruct RouteFamily from an integer key"
    );
}

#[test]
fn should_keep_notice_backpressure_plus_duplicate_paths_bounded() {
    // Arrange
    let repo_root = repo_root();
    let notice_sink_dir = repo_root
        .join("src")
        .join("domains")
        .join("notice")
        .join("sink");
    let domain_sink = read_source_file(&notice_sink_dir.join("domain_sink_impl.rs"));
    let delivery_worker = read_source_file(&notice_sink_dir.join("delivery_worker.rs"));

    // Act
    let duplicate_check = domain_sink.find("self.try_reuse_existing(sub_msg)");
    let pattern_compile = domain_sink.find("Self::compile_pattern(sub_msg)");
    let has_deadline_retry = delivery_worker.contains("NOTICE_MAILBOX_RETRY_TIMEOUT")
        && delivery_worker.contains("Instant::now() < deadline");
    let has_fixed_retry_loop = delivery_worker.contains("MAX_RETRIES");

    // Assert
    assert!(
        duplicate_check
            .zip(pattern_compile)
            .is_some_and(|(check, compile)| check < compile),
        "Notice duplicate lookup must precede pattern compilation"
    );
    assert!(
        has_deadline_retry,
        "Notice backpressure retry must use a deadline"
    );
    assert!(
        !has_fixed_retry_loop,
        "Notice retry must not restore a fixed spin count"
    );
}

#[test]
fn should_compile_lease_bench_commands_only_for_tests_or_benchkit() {
    // Arrange
    let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let model = read_source_file(&repo_root.join("src/domains/lease/sink/model.rs"));
    let lifecycle = read_source_file(
        &repo_root.join("src/domains/lease/sink/lifecycle_and_admin/lifecycle.rs"),
    );

    // Act
    let gate = "#[cfg(any(test, feature = \"benchkit\"))]";

    // Assert
    assert!(model.matches(gate).count() >= 2);
    assert!(lifecycle.matches(gate).count() >= 2);
}

#[test]
fn should_keep_shadow_lease_actor_removed_from_default_surface() {
    // Arrange
    let repo_root = repo_root();
    let lease_dir = repo_root.join("src").join("domains").join("lease");
    let lease_mod = read_source_file(&lease_dir.join("mod.rs"));
    let removed_files = [
        "actor.rs",
        "guard.rs",
        "session.rs",
        "events.rs",
        "projection.rs",
    ];

    // Act
    let exposed_shadow_modules = ["actor", "guard", "session", "events", "projection"]
        .into_iter()
        .filter(|module| lease_mod.contains(&format!("pub mod {module};")))
        .collect::<Vec<_>>();
    let retained_shadow_files = removed_files
        .into_iter()
        .filter(|file| lease_dir.join(file).exists())
        .collect::<Vec<_>>();

    // Assert
    assert!(
        exposed_shadow_modules.is_empty() && retained_shadow_files.is_empty(),
        "shadow Lease surface remains: modules={exposed_shadow_modules:?}, files={retained_shadow_files:?}"
    );
}

#[test]
fn should_keep_panicking_stream_storage_decoders_test_only() {
    // Arrange
    let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let storage = repo_root.join("src/domains/stream/storage");
    let compact = read_source_file(&storage.join("compact_page_values.rs"));
    let hierarchy = read_source_file(&storage.join("resource_area_realm_values.rs"));

    // Act
    let test_gate = "#[cfg(test)]";

    // Assert
    assert!(compact.matches(test_gate).count() >= 3);
    assert!(hierarchy.matches(test_gate).count() >= 3);
}

#[test]
fn should_keep_lease_benchmark_mutation_actor_serialized() {
    // Arrange
    let repo_root = repo_root();
    let lifecycle = read_source_file(
        &repo_root.join("src/domains/lease/sink/lifecycle_and_admin/lifecycle.rs"),
    );

    // Act
    let forbidden = [
        "acquire_direct_for_bench",
        "release_direct_for_bench",
        ".runtime().handle_acquire",
        ".runtime().handle_release",
    ];
    let violations = forbidden
        .into_iter()
        .filter(|pattern| lifecycle.contains(pattern))
        .collect::<Vec<_>>();

    // Assert
    assert!(
        violations.is_empty(),
        "Lease benchmark helpers bypass actor serialization: {violations:?}"
    );
}

#[test]
fn should_keep_scheduler_plus_duplicate_transport_surfaces_private() {
    // Arrange
    let repo_root = repo_root();
    let runtime_module = read_source_file(&repo_root.join("src/runtime/mod.rs"));
    let api_module = read_source_file(&repo_root.join("src/api/mod.rs"));

    // Act
    let violations = [
        runtime_module
            .contains("pub use scheduler::Scheduler")
            .then_some("runtime::Scheduler is publicly re-exported"),
        api_module
            .contains("pub mod ws;")
            .then_some("duplicate api::ws transport module is exported"),
        api_module
            .contains("pub mod transport;")
            .then_some("duplicate api::transport module is exported"),
        repo_root
            .join("src/api/ws.rs")
            .exists()
            .then_some("duplicate src/api/ws.rs transport file remains"),
        repo_root
            .join("src/api/transport.rs")
            .exists()
            .then_some("duplicate src/api/transport.rs file remains"),
    ]
    .into_iter()
    .flatten()
    .map(str::to_string)
    .collect::<Vec<_>>();
    let report = format_violation_report(&violations);

    // Assert
    assert!(
        report.is_empty(),
        "legacy scheduler and duplicate transport surfaces must stay absent:\n{report}"
    );
}

#[test]
fn should_keep_production_rust_files_below_line_budget() {
    // Arrange
    let repo_root = repo_root();
    let files = production_rust_source_files(&repo_root);

    // Act
    let violations = files
        .iter()
        .filter_map(|path| {
            let line_count = read_source_file(path).lines().count();
            (line_count > PRODUCTION_RUST_LINE_LIMIT).then(|| {
                format!(
                    "{} has {line_count} lines",
                    relative_display_path(&repo_root, path)
                )
            })
        })
        .collect::<Vec<_>>();
    let report = format_violation_report(&violations);

    // Assert
    assert!(
        report.is_empty(),
        "production Rust files must stay at or below {PRODUCTION_RUST_LINE_LIMIT} lines:\n{report}"
    );
}

#[test]
fn should_keep_domain_actor_mailbox_capacity_centralized() {
    // Arrange
    let repo_root = repo_root();
    let files = domain_production_source_files(&repo_root);

    // Act
    let report = format_violation_report(&collect_domain_mailbox_capacity_violations(
        &repo_root, &files,
    ));

    // Assert
    assert!(
        report.is_empty(),
        "domain managed actor mailbox capacity must use DOMAIN_ACTOR_MAILBOX_CAPACITY:\n{report}"
    );
}

#[test]
fn should_document_all_rpc_error_codes_in_client_spec() {
    // Arrange
    let repo_root = repo_root();
    let error_codes = repo_root
        .join("src")
        .join("protocol")
        .join("error_codes.rs");
    let client_spec = repo_root
        .join("docs")
        .join("clients")
        .join("spec")
        .join("queue-rpc-kv.md");
    let constants = rpc_error_constants(&error_codes);
    let documented_rows = rpc_error_rows_from_markdown(&client_spec);

    // Act
    let missing_rows = constants
        .iter()
        .filter(|row| !documented_rows.contains(*row))
        .map(|(code, name)| format!("{code} {name} missing from docs/clients/spec/queue-rpc-kv.md"))
        .collect::<Vec<_>>();
    let report = format_violation_report(&missing_rows);

    // Assert
    assert!(
        report.is_empty(),
        "RPC client spec must list every RPC error code/name from src/protocol/error_codes.rs:\n{report}"
    );
}

#[test]
fn should_keep_rpc_design_seams_explicit() {
    // Arrange
    let repo_root = repo_root();
    let rpc = repo_root.join("src/domains/rpc/sink");
    let constants = read_source_file(&rpc.join("state_model/constants.rs"));
    let mailbox = read_source_file(&rpc.join("mailbox_sink_impl.rs"));
    let requests = read_source_file(&rpc.join("state_model/requests.rs"));
    let route_state = read_source_file(&rpc.join("state_model/route_state.rs"));
    let state = read_source_file(&rpc.join("state_model/state.rs"));
    let worker = read_source_file(&rpc.join("state_model/worker.rs"));
    let registration_table = read_source_file(&rpc.join("state_model/registration_table.rs"));
    let ready_queue = read_source_file(&rpc.join("state_model/ready_queue.rs"));
    let response_forwarder = read_source_file(&rpc.join("response_forwarder.rs"));

    // Act
    let violations = [
        (
            !constants.contains("RPC_MSG_TYPE_REQUEST"),
            "request message constant",
        ),
        (
            !constants.contains("RPC_MSG_TYPE_RESPONSE"),
            "response message constant",
        ),
        (
            !mailbox.contains("deliver_with_priority"),
            "shared delivery guard",
        ),
        (
            !requests.contains("dispatch_info: RpcPendingDispatchInfo"),
            "owned pending dispatch view",
        ),
        (
            !route_state.contains("struct RegistrationRotor"),
            "registration rotor",
        ),
        (
            state.contains("clippy::too_many_lines"),
            "small dispatch coordinator",
        ),
        (
            state.contains("fn dispatch_or_queue_request(\n"),
            "test-only dispatch wrapper",
        ),
        (
            !registration_table.contains("struct RegistrationTable"),
            "registration table",
        ),
        (
            !ready_queue.contains("struct RouteReadyQueue"),
            "route-ready queue",
        ),
        (
            !state.contains("trait RpcRequestState") || !state.contains("trait RpcResponseState"),
            "request and response state facades",
        ),
        (
            !response_forwarder.contains("struct RpcResponseForwarder"),
            "response forwarder",
        ),
        (
            !worker.contains("struct RegistrationCredit"),
            "registration credit accounting",
        ),
        (
            [
                "/// Selects the next available registration",
                "/// Claims one registration credit",
                "/// Reserves one unit of global pending capacity",
                "/// Coordinates duplicate, capacity, fairness, and tracking policy",
            ]
            .iter()
            .any(|contract| !state.contains(contract)),
            "RPC state policy documentation",
        ),
    ]
    .into_iter()
    .filter_map(|(missing, label)| missing.then_some(label))
    .collect::<Vec<_>>();

    // Assert
    assert!(violations.is_empty(), "missing RPC seams: {violations:?}");
}

#[test]
fn should_use_registration_vocabulary_throughout_rpc_state_model_source() {
    // Arrange
    let state = read_source_file(&repo_root().join("src/domains/rpc/sink/state_model/state.rs"));
    let registration_table = read_source_file(
        &repo_root().join("src/domains/rpc/sink/state_model/registration_table.rs"),
    );

    // Act
    // Scan comments and literals too: these internal model files should use one vocabulary.
    let mixed_terms = [
        ("state.rs", state),
        ("registration_table.rs", registration_table),
    ]
    .into_iter()
    .flat_map(|(file, source)| {
        source
            .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
            .filter(|identifier| identifier.contains("worker"))
            .map(move |identifier| format!("{file}:{identifier}"))
            .collect::<Vec<_>>()
    })
    .collect::<BTreeSet<_>>();

    // Assert
    assert!(
        mixed_terms.is_empty(),
        "RPC state model source must use registration vocabulary: {mixed_terms:?}"
    );
}

#[test]
fn should_keep_stream_design_seams_explicit() {
    // Arrange
    let stream = repo_root().join("src/domains/stream");
    let keys = read_source_file(&stream.join("storage/keys_and_models.rs"));
    let model = read_source_file(&stream.join("sink/model.rs"));
    let sink = read_source_file(&stream.join("sink/domain_sink_impl.rs"));
    let core = read_source_file(
        &stream.join("sink/domain_sink_impl/domain_core_impl/watermark_coordination.rs"),
    );
    let codecs = read_source_file(&stream.join("storage/compact_page_values.rs"));
    let sequence = read_source_file(&stream.join("store/sequence_and_filters.rs"));
    let actor = read_source_file(&stream.join("actor.rs"));
    let store = read_source_file(&stream.join("store/mod.rs"));
    let store_sources = source_files_under(&stream.join("store"))
        .iter()
        .map(|path| read_source_file(path))
        .collect::<Vec<_>>()
        .join("\n");

    // Act
    let violations = [
        (
            !keys.contains("impl TryFrom<u8> for KeyPrefix"),
            "key-prefix decoding",
        ),
        (
            !model.contains("struct SubscriptionRegistry"),
            "subscription registry",
        ),
        (
            !model.contains("struct AdminSnapshotState"),
            "admin snapshot state",
        ),
        (
            !model.contains("struct WatermarkCoordinators"),
            "watermark coordinators",
        ),
        (
            !codecs.contains("trait PageRecordCodec"),
            "page-record codec",
        ),
        (
            actor.contains("impl ActiveAppendSession {}"),
            "empty append-session impl",
        ),
        (
            !store.contains("enum StreamStoreError"),
            "stream store error",
        ),
        (
            [
                "commit_records_promotion_frontier(",
                "commit_session_promotion_frontier(",
                "read_resource_promotion_frontier(",
                "read_area_promotion_frontier(",
                "read_realm_promotion_frontier(",
            ]
            .iter()
            .any(|wrapper| store_sources.contains(wrapper)),
            "single-layout wrapper twins",
        ),
        (
            !core.contains("fn dispatch_watermark_commit<K>"),
            "shared watermark dispatch",
        ),
        (
            !sink.contains("fn dispatch_family_command<T>"),
            "shared family command dispatch",
        ),
        (
            !sequence.contains("fn load_existing_watermark_for_guard"),
            "shared watermark guard read",
        ),
        (
            !sequence.contains("for key in keys"),
            "discriminator row loop",
        ),
        (
            !keys.contains("LEGACY D3 PROTOTYPE PREFIXES"),
            "legacy prototype prefix boundary",
        ),
    ]
    .into_iter()
    .filter_map(|(missing, label)| missing.then_some(label))
    .collect::<Vec<_>>();

    // Assert
    assert!(
        violations.is_empty(),
        "missing Stream seams: {violations:?}"
    );
}

#[test]
fn should_keep_queue_design_seams_explicit() {
    // Arrange
    let queue = repo_root().join("src/domains/queue");
    let actor = read_source_file(&queue.join("actor/mod.rs"));
    let ack = read_source_file(&queue.join("actor/reserve_and_ack.rs"));
    let storage = read_source_file(&queue.join("actor/storage.rs"));
    let sink = read_source_file(&queue.join("sink/domain_sink_impl.rs"));

    // Act
    let violations = [
        (
            !queue.join("actor/dlq.rs").exists(),
            "DLQ transition module",
        ),
        (
            !queue.join("actor/dead_letter_admin.rs").exists(),
            "dead-letter admin module",
        ),
        (
            !queue.join("actor/startup_reconciliation.rs").exists(),
            "startup reconciliation module",
        ),
        (
            !actor.contains("fn wire_code") || !actor.contains("fn as_str"),
            "DLQ reason mappings",
        ),
        (
            !actor.contains("trait QueueDataPlane") || !actor.contains("trait QueueAdminPlane"),
            "queue interface traits",
        ),
        (
            !ack.contains("fn validate_ack_authorization"),
            "ack authorization seam",
        ),
        (
            !ack.contains("stage_delayed") || !ack.contains("fast path"),
            "ack staging and fast-path documentation",
        ),
        (
            !storage.contains("fn commit_transaction"),
            "shared transaction commit",
        ),
        (
            !sink.contains("struct QueueCounts"),
            "queue counts accessor",
        ),
        (
            !actor.contains("QUEUE_IDLE_HORIZON")
                || !actor.contains("QUEUE_STORAGE_RETRY_BACKOFF")
                || !actor.contains("QUEUE_ACTOR_REPLY_TIMEOUT"),
            "queue timing constants",
        ),
    ]
    .into_iter()
    .filter_map(|(missing, label)| missing.then_some(label))
    .collect::<Vec<_>>();

    // Assert
    assert!(violations.is_empty(), "missing Queue seams: {violations:?}");
}

#[test]
fn should_keep_schedule_design_seams_explicit() {
    // Arrange
    let schedule = repo_root().join("src/domains/schedule");
    let actor = read_source_file(&schedule.join("actor/claim_and_ack.rs"));
    let actor_mod = read_source_file(&schedule.join("actor/mod.rs"));
    let sink = read_source_file(&schedule.join("sink/domain_sink_impl.rs"));
    let model = read_source_file(&schedule.join("sink/model.rs"));
    let store = read_source_file(&schedule.join("store/model.rs"));

    // Act
    let violations = [
        (
            !schedule.join("sink/delivery_strategy.rs").exists(),
            "delivery strategy",
        ),
        (
            !sink.contains("fn claim_due")
                || !sink.contains("fn deliver_claims")
                || !sink.contains("fn acknowledge_delivered"),
            "due scan stages",
        ),
        (
            !actor.contains("fn pop_due_from_heap")
                || !actor.contains("fn recompute_next_fires")
                || !actor.contains("fn persist_claims")
                || !actor.contains("fn apply_claims_to_state"),
            "claim stages",
        ),
        (
            !model.contains("enum PendingFireState"),
            "pending-fire state",
        ),
        (
            !actor_mod.contains("#[cfg(test)]") || !actor_mod.contains("test_actor_harness"),
            "test-only actor harness",
        ),
        (
            !sink.contains("trait ScheduleObservability"),
            "observability interface",
        ),
        (
            !store.contains("trait SchedulePersistence"),
            "persistence interface",
        ),
        (schedule.join("events.rs").exists(), "dead schedule events"),
        (
            !actor_mod.contains("SCAN_DEDUP_WINDOW") || !model.contains("EXECUTIONS_WINDOW_MS"),
            "schedule timing constants",
        ),
        (
            !model.contains("sink wrapper") || !model.contains("runtime body"),
            "sink runtime naming docs",
        ),
    ]
    .into_iter()
    .filter_map(|(missing, label)| missing.then_some(label))
    .collect::<Vec<_>>();

    // Assert
    assert!(
        violations.is_empty(),
        "missing Schedule seams: {violations:?}"
    );
}

#[test]
fn should_complete_reopened_kv_plus_lease_design_criteria() {
    // Arrange
    let root = repo_root().join("src/domains");
    let kv_domain = read_source_file(&root.join("kv/sink/domain_sink_impl.rs"));
    let kv_mailbox = read_source_file(&root.join("kv/sink/mailbox_sink_impl.rs"));
    let lease_expiry = read_source_file(&root.join("lease/sink/domain_sink_impl/expiry.rs"));
    let lease_mailbox = read_source_file(&root.join("lease/sink/mailbox_sink_impl.rs"));

    // Act
    let violations = [
        (
            !kv_domain.contains("use crate::domains::kv::KvActor;")
                || kv_domain.contains("crate::domains::kv::KvActor::"),
            "KV domain actor import cleanup",
        ),
        (
            !kv_mailbox.contains("use crate::domains::kv::{KvActor, KvError, KvResponse};")
                || ["KvActor", "KvError", "KvResponse"]
                    .iter()
                    .any(|name| kv_mailbox.contains(&format!("crate::domains::kv::{name}"))),
            "KV mailbox imports cleanup",
        ),
        (
            !lease_expiry.contains(
                "/// Removes every queued waiter owned by the session before empty queues are dropped.",
            ),
            "Lease session-waiter ordering docs",
        ),
        (
            !lease_mailbox.contains("fn scope_operation_owner")
                || lease_mailbox.matches("session_scoped_owner_id(").count() != 1,
            "Lease owner-scoping step",
        ),
    ]
    .into_iter()
    .filter_map(|(missing, label)| missing.then_some(label))
    .collect::<Vec<_>>();

    // Assert
    assert!(
        violations.is_empty(),
        "reopened design criteria remain incomplete: {violations:?}"
    );
}

#[test]
fn should_document_unified_wildcard_registration_plus_exact_lease_semantics() {
    // Arrange
    let root = repo_root().join("docs");
    let wire = read_source_file(&root.join("clients/spec/wire-routing.md"));
    let boundaries = read_source_file(&root.join("development/domain-boundaries-spec.md"));
    let laws = read_source_file(&root.join("development/architectural-laws.md"));
    let schedule = read_source_file(&root.join("clients/spec/lease-schedule.md"));
    let operations = read_source_file(&root.join("clients/spec/operations.md"));

    // Act
    let combined = [
        wire.as_str(),
        boundaries.as_str(),
        laws.as_str(),
        schedule.as_str(),
        operations.as_str(),
    ]
    .join("\n");

    // Assert
    assert!(wire
        .contains("KV, Queue, Notice, Stream, RPC, and Schedule each permit at most 128 wildcard"));
    assert!(
        wire.contains("Notifications carry the matching `subscription_id` and the exact concrete")
    );
    assert!(wire.contains("Ready concrete routes rotate fairly"));
    assert!(boundaries.contains("exact and wildcard registrations are equal candidates"));
    assert!(boundaries.contains("Lease does not participate in this wildcard contract"));
    assert!(laws.contains("whole-segment `*` and `**`"));
    assert!(schedule.contains("Overlapping\npatterns remain distinct"));
    assert!(schedule.contains("Watches are exact-route subscriptions"));
    assert!(schedule.contains("5010 = ERR_INVALID_SUBSCRIPTION_ROUTE"));
    assert!(operations.contains("KV, Queue, Notice, Stream, RPC, and Schedule registrations"));
    assert!(operations.contains("Duplicate `(session, original registration"));
    assert!(operations.contains("Matching never\ncrosses `RouteFamily`"));
    assert!(operations.contains("the exact concrete route"));
    assert!(operations.contains("Lease is intentionally different"));
    assert!(!combined.contains("Wildcard worker registration is not part of the contract"));
    assert!(!combined.contains("Workers register exact listening routes"));
    assert!(!combined.contains("Wildcard schedule subscribe is invalid"));
    assert!(!combined.contains("Lease subscriptions accept wildcard"));
    assert!(!combined.contains("Lease watches support `*`"));
}

#[test]
fn should_keep_boot_runtime_design_seams_explicit() {
    // Arrange
    let root = repo_root();
    let boot = read_source_file(&root.join("src/boot/mod.rs"));
    let storage = read_source_file(&root.join("src/boot/storage.rs"));
    let config = read_source_file(&root.join("src/boot/runtime/config.rs"));
    let cloud = read_source_file(&root.join("src/boot/runtime/config/cloud_provider.rs"));
    let env = read_source_file(&root.join("src/boot/runtime/config/env.rs"));
    let domains = read_source_file(&root.join("src/boot/domains.rs"));
    let pool =
        read_source_file(&root.join("src/runtime/family_".to_string() + "a" + "ctor_pool.rs"));
    let managed =
        read_source_file(&root.join("src/runtime/managed_".to_string() + "a" + "ctor.rs"));
    let shutdown = read_source_file(&root.join("src/boot/shutdown.rs"));

    // Act
    let required = [
        (boot.contains("enum BootStage"), "named boot stages"),
        (boot.contains("fn start_listeners"), "listener stage"),
        (boot.contains("fn open_storage_stage"), "storage stage"),
        (boot.contains("fn register_domains_stage"), "domain stage"),
        (!boot.contains("clippy::too_many_lines"), "boot line lint"),
        (
            boot.matches(&["\n    ShutdownContext ", &char::from(123).to_string()].concat())
                .count()
                == 1,
            "shutdown context construction",
        ),
        (
            root.join("src/boot/storage/backoff.rs").is_file(),
            "storage backoff module",
        ),
        (
            root.join("src/boot/storage/contention.rs").is_file(),
            "storage contention module",
        ),
        (
            read_source_file(&root.join("src/boot/storage/contention.rs"))
                .contains("enum ContentionKind"),
            "typed storage contention seam",
        ),
        (
            config.contains("struct TransportConfig"),
            "transport sub-config",
        ),
        (
            config.contains("struct StorageConfig"),
            "storage sub-config",
        ),
        (config.contains("struct DrainConfig"), "drain sub-config"),
        (
            storage.contains("fn open_with_retry"),
            "shared storage retry loop",
        ),
        (
            cloud.contains("fn s3_compatible_provider"),
            "shared S3-compatible provider constructor",
        ),
        (
            cloud.contains("PROVIDER_DESCRIPTORS"),
            "provider descriptor table",
        ),
        (
            config.contains("fn cloud_durable_write_options"),
            "cloud write options mapping",
        ),
        (
            env.contains("fn positive_u64_from_env"),
            "positive integer environment parser",
        ),
        (
            managed.contains(&("Unsupervised ".to_string() + "a" + "ctors do not fire timers")),
            "unsupervised timer contract",
        ),
        (
            domains.contains("DomainKind::ALL.len()"),
            "domain handle consistency regression",
        ),
        (
            pool.contains(&("struct Family".to_string() + "A" + "ctorPoolHealthSnapshot")),
            "family pool health type",
        ),
        (
            shutdown.contains("PRIORITY_FATAL"),
            "named shutdown priority",
        ),
        (
            !boot.contains("fn warn_defaulted_fast_queue_policy"),
            "queue warning ownership",
        ),
        (
            config.contains("fn warn_defaulted_fast_queue_policy"),
            "queue warning policy",
        ),
    ];
    let missing = required
        .into_iter()
        .filter_map(|(present, label)| (!present).then_some(label))
        .collect::<Vec<_>>();

    // Assert
    assert!(missing.is_empty(), "missing boot/runtime seams");
}

#[test]
fn should_document_route_bearing_schedule_notify_wire_format() {
    // Arrange
    let root = repo_root().join("docs");
    let schedule = read_source_file(&root.join("clients/spec/lease-schedule.md"));
    let migration = read_source_file(&root.join("operations/migration-guide.md"));

    // Act
    let has_route_bearing_schema = schedule.contains("[u32 BE]  exact_route_len")
        && schedule.contains("[bytes]   exact_route")
        && schedule.contains("[subscription_id][exact_route][payload]");

    // Assert
    assert!(has_route_bearing_schema);
    assert!(migration
        .contains("`[subscription_id][payload]` to `[subscription_id][exact_route][payload]`"));
}

#[test]
fn should_keep_runtime_ingress_payload_dispatch_free_of_payload_unwraps() {
    // Arrange
    let repo_root = repo_root();
    let files = [
        repo_root
            .join("src")
            .join("api")
            .join("runtime_ingress")
            .join("trait_impls.rs"),
        repo_root
            .join("src")
            .join("api")
            .join("runtime_ingress")
            .join("domain_frame_dispatcher.rs"),
    ];

    // Act
    let violations = files
        .iter()
        .flat_map(|path| {
            let relative_path = relative_display_path(&repo_root, path);
            read_source_file(path)
                .lines()
                .enumerate()
                .filter(|(_, line)| line.contains("payload") && line.contains(".unwrap()"))
                .map({
                    let relative_path = relative_path.clone();
                    move |(line_index, _)| {
                        format!(
                            "{}:{} contains a payload unwrap invariant",
                            relative_path,
                            line_index + 1
                        )
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let report = format_violation_report(&violations);

    // Assert
    assert!(
        report.is_empty(),
        "runtime ingress payload dispatch must stay free of payload unwrap invariants:\n{report}"
    );
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn domain_owned_source_files(repo_root: &Path) -> Vec<OwnedSourceFile> {
    let mut files = Vec::new();
    for &domain in DOMAINS {
        collect_owned_rust_files(
            &repo_root.join("src").join("domains").join(domain),
            domain,
            &mut files,
        );
    }
    files.sort();
    files
}

fn collect_owned_rust_files(
    directory: &Path,
    owner: &'static str,
    files: &mut Vec<OwnedSourceFile>,
) {
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()));

    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| {
                panic!("failed to read entry in {}: {error}", directory.display())
            })
            .path();

        if path.is_dir() {
            collect_owned_rust_files(&path, owner, files);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(OwnedSourceFile { owner, path });
        }
    }
}

fn sync_core_source_files(repo_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for directory in SYNC_CORE_DIRS {
        collect_rust_files(&repo_root.join("src").join(directory), &mut files);
    }
    files.sort();
    files
}

fn source_files_under(directory: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(directory, &mut files);
    files.sort();
    files
}

fn production_rust_source_files(repo_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(&repo_root.join("src"), &mut files);
    files.retain(|path| {
        let relative = relative_display_path(repo_root, path);
        !relative.ends_with("/tests.rs")
            && !relative.contains("/tests/")
            && !relative.contains("/test_helpers.rs")
    });
    files.sort();
    files
}

fn domain_production_source_files(repo_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(&repo_root.join("src").join("domains"), &mut files);
    files.retain(|path| {
        let relative = relative_display_path(repo_root, path);
        !relative.ends_with("/tests.rs")
            && !relative.contains("/tests/")
            && !relative.contains("/test_helpers.rs")
    });
    files.sort();
    files
}

fn admin_api_source_files(repo_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(&repo_root.join("src").join("api").join("admin"), &mut files);
    files.retain(|path| {
        let relative = relative_display_path(repo_root, path);
        !relative.ends_with("/tests.rs") && !relative.contains("/tests/")
    });
    files.sort();
    files
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()));

    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| {
                panic!("failed to read entry in {}: {error}", directory.display())
            })
            .path();

        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn collect_domain_mailbox_capacity_violations(repo_root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();

    for path in files {
        let content = read_source_file(path);
        let lines = content.lines().collect::<Vec<_>>();
        for (line_index, line) in lines.iter().enumerate() {
            if !line.contains("ManagedActor::spawn_supervised") {
                continue;
            }

            let window_end = (line_index + 8).min(lines.len());
            let window = lines[line_index..window_end].join("\n");
            if !window.contains("DOMAIN_ACTOR_MAILBOX_CAPACITY") {
                violations.push(format!(
                    "{}:{} spawns managed actor without centralized domain mailbox capacity",
                    relative_display_path(repo_root, path),
                    line_index + 1
                ));
            }
        }
    }

    violations
}

fn collect_foreign_domain_reference_violations(
    repo_root: &Path,
    files: &[OwnedSourceFile],
) -> Vec<String> {
    let mut violations = Vec::new();

    for file in files {
        let content = read_source_file(&file.path);
        for (line_index, line) in content.lines().enumerate() {
            for other_domain in DOMAINS {
                if *other_domain == file.owner {
                    continue;
                }

                let needle = format!("crate::domains::{other_domain}::");
                if line.contains(&needle) {
                    violations.push(format!(
                        "{}:{} references {other_domain} domain module",
                        relative_display_path(repo_root, &file.path),
                        line_index + 1
                    ));
                }
            }
        }
    }

    violations
}

fn collect_foreign_route_scheme_violations(
    repo_root: &Path,
    files: &[OwnedSourceFile],
) -> Vec<String> {
    let mut violations = Vec::new();

    for file in files {
        let content = read_source_file(&file.path);
        for (line_index, line) in content.lines().enumerate() {
            for other_domain in DOMAINS {
                if *other_domain == file.owner {
                    continue;
                }

                let needle = format!("{other_domain}://");
                if line.contains(&needle) {
                    violations.push(format!(
                        "{}:{} hard-codes {other_domain} route scheme",
                        relative_display_path(repo_root, &file.path),
                        line_index + 1
                    ));
                }
            }
        }
    }

    violations
}

fn report_for_patterns(repo_root: &Path, files: &[PathBuf], forbidden: &[&str]) -> String {
    report_for_patterns_with_allowed(repo_root, files, forbidden, &[])
}

fn report_for_patterns_with_allowed(
    repo_root: &Path,
    files: &[PathBuf],
    forbidden: &[&str],
    allowed: &[&str],
) -> String {
    let violations = files
        .iter()
        .flat_map(|path| {
            let content = read_source_file(path);
            let relative_path = relative_display_path(repo_root, path);

            content
                .lines()
                .enumerate()
                .flat_map(|(line_index, line)| {
                    forbidden
                        .iter()
                        .filter(move |needle| {
                            line.contains(**needle)
                                && !allowed.iter().any(|exception| line.contains(exception))
                        })
                        .map({
                            let relative_path = relative_path.clone();
                            move |needle| {
                                format!("{}:{} contains {needle}", relative_path, line_index + 1)
                            }
                        })
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    format_violation_report(&violations)
}

fn read_source_file(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
}

fn rpc_error_constants(path: &Path) -> Vec<(u16, String)> {
    let mut in_rpc_section = false;
    let mut rows = Vec::new();

    for line in read_source_file(path).lines() {
        let trimmed = line.trim();
        if trimmed == "pub mod rpc {" {
            in_rpc_section = true;
            continue;
        }
        if in_rpc_section && trimmed == "}" {
            break;
        }
        if !in_rpc_section || !trimmed.starts_with("pub const ") {
            continue;
        }

        let definition = &trimmed["pub const ".len()..];
        let Some((name, value_suffix)) = definition.split_once(": u16 = ") else {
            continue;
        };
        let digits = value_suffix
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>();
        if let Ok(code) = digits.parse::<u16>() {
            rows.push((code, name.to_string()));
        }
    }

    rows
}

fn rpc_error_rows_from_markdown(path: &Path) -> BTreeSet<(u16, String)> {
    read_source_file(path)
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if !trimmed.starts_with('|') {
                return None;
            }

            let columns = trimmed
                .trim_matches('|')
                .split('|')
                .map(str::trim)
                .collect::<Vec<_>>();
            if columns.len() < 2 {
                return None;
            }

            let Ok(code) = columns[0].parse::<u16>() else {
                return None;
            };
            if !(6001..=6013).contains(&code) {
                return None;
            }

            Some((code, columns[1].to_string()))
        })
        .collect()
}

fn relative_display_path(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn format_violation_report(violations: &[String]) -> String {
    if violations.is_empty() {
        String::new()
    } else {
        violations.join("\n")
    }
}

#[test]
fn should_document_every_defined_protocol_error_code() {
    // Arrange
    // An error code is only useful if a client can classify it. A code added
    // without a spec entry is invisible to SDKs, which is how a new schedule
    // code shipped undocumented and how a retryable code stayed unemitted for
    // months. This is a pure set comparison, so it costs nothing to keep.
    let repo_root = repo_root();
    let source = read_source_file(&repo_root.join("src/protocol/error_codes.rs"));
    let docs = collect_client_doc_text(&repo_root);

    // Act
    let defined = defined_error_codes(&source);
    assert!(
        defined.len() > 50,
        "parsed only {} error codes; the scan is not reading the module and would \
         pass vacuously",
        defined.len()
    );
    let undocumented = defined
        .into_iter()
        .filter(|(code, _)| !documents_error_code(&docs, *code))
        .map(|(code, name)| format!("{code} = {name}"))
        .collect::<Vec<_>>();

    // Assert
    let report = format_violation_report(&undocumented);
    assert!(
        report.is_empty(),
        "every protocol error code must appear in docs/clients:\n{report}"
    );
}

/// Whether the docs mention `code` as a standalone number.
///
/// A plain substring test reports a false positive whenever the digits appear
/// inside a larger number, a year, or an example payload - so `1014` would look
/// documented because `21014` exists somewhere. Requiring non-digit boundaries
/// on both sides makes the guard actually detect a missing entry.
fn documents_error_code(docs: &str, code: u16) -> bool {
    let needle = code.to_string();
    docs.match_indices(&needle).any(|(index, _)| {
        let before_is_digit = docs[..index]
            .chars()
            .next_back()
            .is_some_and(|character| character.is_ascii_digit());
        let after_is_digit = docs[index + needle.len()..]
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_digit());
        !before_is_digit && !after_is_digit
    })
}

/// Every `pub const ERR_*: u16 = N;` defined in the protocol error module.
fn defined_error_codes(source: &str) -> Vec<(u16, String)> {
    source
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let rest = line.strip_prefix("pub const ")?;
            let (name, rest) = rest.split_once(": u16 = ")?;
            if !name.starts_with("ERR_") {
                return None;
            }
            let value = rest.trim_end_matches(';').split(';').next()?.trim();
            value
                .parse::<u16>()
                .ok()
                .map(|code| (code, name.to_string()))
        })
        .collect()
}

fn collect_client_doc_text(repo_root: &Path) -> String {
    let mut text = String::new();
    let mut stack = vec![repo_root.join("docs/clients")];
    while let Some(directory) = stack.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "md") {
                text.push_str(&read_source_file(&path));
                text.push('\n');
            }
        }
    }
    text
}

#[test]
fn should_centralize_lexkey_prefix_range_bounds() {
    // Arrange
    // lexkey offers two upper bounds and they are not interchangeable.
    // `encode_range_upper`/`prefix_end` yield `prefix || 0xff`, correct only
    // when what follows the prefix is itself lexkey-encoded - UTF-8 strings and
    // fixed-width numbers can never reach 0xff. Callers that append raw client
    // bytes need `prefix_successor`, or keys beginning with 0xff sort outside
    // their own range and become invisible to scans while writes still succeed.
    //
    // `storage_key::prefix_range_end` makes that choice once. Anywhere else
    // reaching for the raw APIs re-opens the decision per call site, which is
    // how the KV scan bug happened.
    let repo_root = repo_root();
    let allowed = repo_root.join("src/utils/storage_key.rs");
    let raw_bound_apis = ["encode_range_upper", "prefix_end(", "range_upper_vec"];

    // Act
    let violations = source_files_under(&repo_root.join("src"))
        .into_iter()
        .filter(|path| *path != allowed)
        .filter_map(|path| {
            let contents = read_source_file(&path);
            let used = raw_bound_apis.iter().find(|api| contents.contains(**api))?;
            Some(format!(
                "{} calls {used}; use storage_key::prefix_range_end instead",
                relative_display_path(&repo_root, &path)
            ))
        })
        .collect::<Vec<_>>();

    // Assert
    let report = format_violation_report(&violations);
    assert!(
        report.is_empty(),
        "lexkey prefix range bounds must be chosen in one place:\n{report}"
    );
}
