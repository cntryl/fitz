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
fn should_keep_transport_and_admin_frameworks_out_of_sync_core() {
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
fn should_keep_protocol_and_domains_on_dispatch_boundary() {
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
fn should_keep_notice_backpressure_and_duplicate_paths_bounded() {
    // Arrange
    let repo_root = repo_root();
    let notice_sink_dir = repo_root
        .join("src")
        .join("domains")
        .join("notice")
        .join("sink");
    let sink = read_source_file(&repo_root.join("src/domains/notice/sink.rs"));
    let delivery_worker = read_source_file(&notice_sink_dir.join("delivery_worker.rs"));

    // Act
    let duplicate_check = sink.find("state.find_existing_id");
    let pattern_compile = sink.find(".compile_registration_pattern");
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
fn should_keep_lease_benchmark_mutation_actor_serialized() {
    // Arrange
    let repo_root = repo_root();
    let lifecycle =
        read_source_file(&repo_root.join("src/domains/lease/sink/lifecycle_and_admin.rs"));

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
fn should_keep_scheduler_and_duplicate_transport_surfaces_private() {
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
fn should_document_unified_wildcard_registration_and_exact_lease_semantics() {
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
