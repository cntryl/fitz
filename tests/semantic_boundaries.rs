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
