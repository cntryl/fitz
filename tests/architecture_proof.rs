use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::{DomainKind, Envelope, Router};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct DomainProof {
    kind: DomainKind,
    variant: &'static str,
    scheme: &'static str,
    sink_type: &'static str,
    actor_file: &'static str,
    actor_marker: &'static str,
    managed_actor_file: &'static str,
    managed_actor_type: &'static str,
    managed_command_type: &'static str,
    managed_actor_impl_file: &'static str,
    managed_actor_spawn_file: &'static str,
    doc_heading: &'static str,
    doc_phrases: &'static [&'static str],
}

const DOMAIN_PROOFS: &[DomainProof] = &[
    DomainProof {
        kind: DomainKind::Kv,
        variant: "Kv",
        scheme: "kv",
        sink_type: "KvDomainSink",
        actor_file: "src/domains/kv/actor.rs",
        actor_marker: "pub struct KvActor",
        managed_actor_file: "src/domains/kv/sink/model.rs",
        managed_actor_type: "KvDomainActor",
        managed_command_type: "KvDomainCommand",
        managed_actor_impl_file: "src/domains/kv/sink/mailbox_sink_impl.rs",
        managed_actor_spawn_file: "src/domains/kv/sink/domain_sink_impl.rs",
        doc_heading: "#### KV",
        doc_phrases: &[
            "Actor owner:",
            "Persistence:",
            "Cleanup:",
            "`RouteFamily`/`realm`:",
            "Admin path:",
            "Runtime::kv_*",
        ],
    },
    DomainProof {
        kind: DomainKind::Queue,
        variant: "Queue",
        scheme: "queue",
        sink_type: "QueueDomainSink",
        actor_file: "src/domains/queue/sink/model.rs",
        actor_marker: "pub(super) struct QueueDomainActor",
        managed_actor_file: "src/domains/queue/sink/model.rs",
        managed_actor_type: "QueueDomainActor",
        managed_command_type: "QueueDomainCommand",
        managed_actor_impl_file: "src/domains/queue/sink/mailbox_sink_impl.rs",
        managed_actor_spawn_file: "src/domains/queue/sink/domain_sink_impl.rs",
        doc_heading: "#### Queue",
        doc_phrases: &[
            "Actor owner:",
            "Persistence:",
            "Cleanup:",
            "`RouteFamily`/`realm`:",
            "Admin path:",
            "Runtime::queue_list_*",
        ],
    },
    DomainProof {
        kind: DomainKind::Notice,
        variant: "Notice",
        scheme: "notice",
        sink_type: "NoticeDomainSink",
        actor_file: "src/domains/notice/sink/actor_runtime.rs",
        actor_marker: "pub(super) struct NoticeDomainActor",
        managed_actor_file: "src/domains/notice/sink/actor_runtime.rs",
        managed_actor_type: "NoticeDomainActor",
        managed_command_type: "NoticeDomainCommand",
        managed_actor_impl_file: "src/domains/notice/sink/actor_runtime.rs",
        managed_actor_spawn_file: "src/domains/notice/sink.rs",
        doc_heading: "#### Notice",
        doc_phrases: &[
            "Actor owner:",
            "Persistence:",
            "Cleanup:",
            "`RouteFamily`/`realm`:",
            "Admin path:",
            "Runtime::notice_list_subscriptions()",
        ],
    },
    DomainProof {
        kind: DomainKind::Stream,
        variant: "Stream",
        scheme: "stream",
        sink_type: "StreamDomainSink",
        actor_file: "src/domains/stream/actor.rs",
        actor_marker: "pub struct StreamActor",
        managed_actor_file: "src/domains/stream/sink/model.rs",
        managed_actor_type: "StreamDomainActor",
        managed_command_type: "StreamDomainCommand",
        managed_actor_impl_file: "src/domains/stream/sink/mailbox_sink_impl.rs",
        managed_actor_spawn_file: "src/domains/stream/sink/domain_sink_impl.rs",
        doc_heading: "#### Stream",
        doc_phrases: &[
            "Actor owner:",
            "Persistence:",
            "Cleanup:",
            "`RouteFamily`/`realm`:",
            "Admin path:",
            "Runtime::stream_list_*",
        ],
    },
    DomainProof {
        kind: DomainKind::Rpc,
        variant: "Rpc",
        scheme: "rpc",
        sink_type: "RpcDomainSink",
        actor_file: "src/domains/rpc/actor.rs",
        actor_marker: "pub struct RpcRouteActor",
        managed_actor_file: "src/domains/rpc/sink/state_model/sink.rs",
        managed_actor_type: "RpcDomainActor",
        managed_command_type: "RpcDomainCommand",
        managed_actor_impl_file: "src/domains/rpc/sink/mailbox_sink_impl.rs",
        managed_actor_spawn_file: "src/domains/rpc/sink/domain_sink_impl.rs",
        doc_heading: "#### RPC",
        doc_phrases: &[
            "Actor owner:",
            "Persistence:",
            "Cleanup:",
            "`RouteFamily`/`realm`:",
            "Admin path:",
            "Runtime::rpc_list_workers()",
        ],
    },
    DomainProof {
        kind: DomainKind::Lease,
        variant: "Lease",
        scheme: "lease",
        sink_type: "LeaseDomainSink",
        actor_file: "src/domains/lease/actor.rs",
        actor_marker: "pub struct LeaseActor",
        managed_actor_file: "src/domains/lease/sink/model.rs",
        managed_actor_type: "LeaseDomainActor",
        managed_command_type: "LeaseDomainCommand",
        managed_actor_impl_file: "src/domains/lease/sink/mailbox_sink_impl.rs",
        managed_actor_spawn_file: "src/domains/lease/sink/lifecycle_and_admin.rs",
        doc_heading: "#### Lease",
        doc_phrases: &[
            "Actor owner:",
            "Persistence:",
            "Cleanup:",
            "`RouteFamily`/`realm`:",
            "Admin path:",
            "Runtime::lease_list_waiters()",
        ],
    },
    DomainProof {
        kind: DomainKind::Schedule,
        variant: "Schedule",
        scheme: "schedule",
        sink_type: "ScheduleDomainSink",
        actor_file: "src/domains/schedule/actor/model.rs",
        actor_marker: "pub struct ScheduleActor",
        managed_actor_file: "src/domains/schedule/sink/model.rs",
        managed_actor_type: "ScheduleDomainActor",
        managed_command_type: "ScheduleDomainCommand",
        managed_actor_impl_file: "src/domains/schedule/sink/mailbox_sink_impl.rs",
        managed_actor_spawn_file: "src/domains/schedule/sink/domain_sink_impl.rs",
        doc_heading: "#### Schedule",
        doc_phrases: &[
            "Actor owner:",
            "Persistence:",
            "Cleanup:",
            "`RouteFamily`/`realm`:",
            "Admin path:",
            "Runtime::schedule_list_schedules()",
        ],
    },
];

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

const ADMIN_BOUNDARY_ALLOWED_EXCEPTIONS: &[&str] = &[
    "crate::domains::kv::sink::AdminKvRowsRequest",
    "crate::domains::stream::sink::AdminStreamReadRequest",
];

const SYNC_CORE_TRANSPORT_FORBIDDEN: &[&str] = &[
    "hyper::",
    "axum::",
    "warp::",
    "reqwest::",
    "tokio_tungstenite",
    "crate::api::admin::",
];

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

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_repo_file(path: &str) -> String {
    let absolute = repo_root().join(path);
    fs::read_to_string(&absolute)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", absolute.display()))
}

fn section_between<'a>(contents: &'a str, start: &str, end: &str) -> &'a str {
    let start_index = contents
        .find(start)
        .unwrap_or_else(|| panic!("missing section start: {start}"));
    let tail = &contents[start_index..];
    let end_index = tail
        .find(end)
        .unwrap_or_else(|| panic!("missing section end: {end}"));
    &tail[..end_index]
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", dir.display()));

    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| panic!("failed to read entry in {}: {error}", dir.display()))
            .path();
        if path.is_dir() {
            collect_rs_files(&path, files);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn relative_display(path: &Path) -> String {
    path.strip_prefix(repo_root())
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[test]
fn should_prove_domain_runtime_contract_for_every_domain_kind() {
    // Arrange
    let boot_domains = read_repo_file("src/boot/domains.rs");
    let store = fitz::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
    let router = Arc::new(Router::new());
    let runtime = fitz::boot::Runtime::new(router.clone());
    let admin_read_model = runtime.admin_read_model();

    let handles = fitz::boot::domains::setup(
        &router,
        &store,
        &admin_read_model,
        &fitz::boot::domains::DomainSetupOptions {
            server_write_options: cntryl_midge::WriteOptions::best_effort(),
            queue_write_options: cntryl_midge::WriteOptions::best_effort(),
            queue_fast_flush_interval: Some(std::time::Duration::from_millis(50)),
            request_sync_write_options: cntryl_midge::WriteOptions::sync(),
            rpc_request_timeout: None,
            stream_storage_layout: fitz::domains::stream::StreamStorageLayout::default(),
        },
    )
    .expect("setup domains");

    // Act
    for proof in DOMAIN_PROOFS {
        let descriptor = proof.kind.descriptor();
        let actor_source = read_repo_file(proof.actor_file);
        let managed_actor_source = read_repo_file(proof.managed_actor_file);
        let managed_actor_impl_source = read_repo_file(proof.managed_actor_impl_file);
        let managed_actor_spawn_source = read_repo_file(proof.managed_actor_spawn_file);

        // Assert
        assert_eq!(descriptor.scheme, proof.scheme);
        assert_eq!(descriptor.wildcard_route, format!("{}://**", proof.scheme));
        assert_eq!(proof.kind.as_str(), proof.scheme);
        assert_eq!(
            proof.kind.cleanup_route(),
            Route::new(format!("{}://cleanup", proof.scheme))
        );
        assert!(
            boot_domains.contains(&format!("DomainKind::{}", proof.variant))
                && boot_domains.contains(proof.sink_type)
                && boot_domains.contains(".register_sink("),
            "missing boot registration proof for {}",
            proof.scheme
        );
        assert!(
            actor_source.contains(proof.actor_marker),
            "missing actor marker {} in {}",
            proof.actor_marker,
            proof.actor_file
        );
        assert!(
            managed_actor_source.contains(&format!("struct {}", proof.managed_actor_type)),
            "missing managed actor {} in {}",
            proof.managed_actor_type,
            proof.managed_actor_file
        );
        assert!(
            managed_actor_source.contains(&format!("enum {}", proof.managed_command_type)),
            "missing managed command {} in {}",
            proof.managed_command_type,
            proof.managed_actor_file
        );
        assert!(
            managed_actor_impl_source
                .contains(&format!("impl Actor for {}", proof.managed_actor_type))
                && managed_actor_impl_source
                    .contains(&format!("type Message = {}", proof.managed_command_type)),
            "missing managed Actor impl for {} using {} in {}",
            proof.managed_actor_type,
            proof.managed_command_type,
            proof.managed_actor_impl_file
        );
        assert!(
            managed_actor_spawn_source
                .contains(&format!("ManagedActor<{}>", proof.managed_command_type))
                && managed_actor_spawn_source.contains("ManagedActor::spawn"),
            "missing managed actor spawn for {} in {}",
            proof.managed_actor_type,
            proof.managed_actor_spawn_file
        );
        let result = router.route(Envelope::new(
            RouteAddress::new(RouteFamily::new(1), proof.kind.cleanup_route()),
            fitz::runtime::SessionCleanup { session_id: 42 },
        ));
        assert!(
            result.is_ok(),
            "cleanup route not registered for {}",
            proof.scheme
        );
    }

    handles.stop();
}

#[test]
fn should_require_documented_domain_contract_for_every_domain() {
    // Arrange
    let architecture = read_repo_file("docs/development/architecture.md");
    let contract_section = section_between(
        &architecture,
        "### Domain Actor, Data, And Admin Contracts",
        "## Authentication & TLS",
    );

    // Act
    for proof in DOMAIN_PROOFS {
        let domain_section = section_between(
            contract_section,
            proof.doc_heading,
            DOMAIN_PROOFS
                .iter()
                .skip_while(|candidate| candidate.doc_heading != proof.doc_heading)
                .nth(1)
                .map_or(
                    "**Historical sketch (outdated shape, not the current implementation):**",
                    |next| next.doc_heading,
                ),
        );

        // Assert
        for phrase in proof.doc_phrases {
            assert!(
                domain_section.contains(phrase),
                "missing doc phrase {phrase} in {} contract section",
                proof.scheme
            );
        }
    }
}

#[test]
fn should_keep_admin_surface_on_runtime_facades() {
    // Arrange
    let mut files = Vec::new();
    collect_rs_files(
        &repo_root().join("src").join("api").join("admin"),
        &mut files,
    );
    files.sort();

    let violations = files
        .into_iter()
        .filter(|path| {
            !path
                .components()
                .any(|component| component.as_os_str() == "tests.rs")
                && !relative_display(path).contains("/tests/")
        })
        .flat_map(|path| {
            let content = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            let relative_path = relative_display(&path);
            content
                .lines()
                .enumerate()
                .flat_map(|(index, line)| {
                    ADMIN_BOUNDARY_FORBIDDEN
                        .iter()
                        .filter(move |needle| {
                            line.contains(**needle)
                                && !ADMIN_BOUNDARY_ALLOWED_EXCEPTIONS
                                    .iter()
                                    .any(|allowed| line.contains(allowed))
                        })
                        .map({
                            let relative_path = relative_path.clone();
                            move |needle| {
                                format!(
                                    "{}:{} contains forbidden dependency {}",
                                    relative_path,
                                    index + 1,
                                    needle
                                )
                            }
                        })
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    // Act
    let report = violations.join("\n");

    // Assert
    assert!(report.is_empty(), "admin boundary violations:\n{report}");
}

#[test]
fn should_keep_domain_handles_from_exposing_concrete_sinks() {
    // Arrange
    let source = read_repo_file("src/boot/domains.rs");
    let forbidden_fields = [
        "pub kv:",
        "pub queue:",
        "pub notice:",
        "pub stream:",
        "pub rpc:",
        "pub lease:",
        "pub schedule:",
    ];

    // Act
    let violations = forbidden_fields
        .into_iter()
        .filter(|field| source.contains(field))
        .collect::<Vec<_>>();

    // Assert
    assert!(
        violations.is_empty(),
        "DomainHandles exposes concrete sink fields: {violations:?}"
    );
}

#[test]
fn should_keep_boot_domains_from_reexporting_concrete_sinks() {
    // Arrange
    let source = read_repo_file("src/boot/domains.rs");
    let forbidden_reexports = [
        "pub use crate::domains::kv::sink::KvDomainSink",
        "pub use crate::domains::queue::sink::QueueDomainSink",
        "pub use crate::domains::notice::sink::NoticeDomainSink",
        "pub use crate::domains::stream::sink::StreamDomainSink",
        "pub use crate::domains::rpc::sink::RpcDomainSink",
        "pub use crate::domains::lease::sink::LeaseDomainSink",
        "pub use crate::domains::schedule::sink::ScheduleDomainSink",
    ];

    // Act
    let violations = forbidden_reexports
        .into_iter()
        .filter(|reexport| source.contains(reexport))
        .collect::<Vec<_>>();

    // Assert
    assert!(
        violations.is_empty(),
        "boot::domains reexports concrete sinks: {violations:?}"
    );
}

fn stream_live_core_public_helper_violations() -> Vec<&'static str> {
    let source = read_repo_file("src/domains/stream/sink/domain_sink_impl.rs");
    let core_impl = section_between(
        &source,
        "impl StreamDomainCore {",
        "impl StreamDomainRuntime<'_> {",
    );
    let forbidden_public_helpers = [
        "pub fn append_session_count(&self)",
        "pub fn refresh_admin_snapshot_if_dirty(&self)",
        "pub fn unsubscribe_all(&self",
        "pub fn subscription_count(&self)",
        "pub fn stream_count(&self)",
    ];

    forbidden_public_helpers
        .into_iter()
        .filter(|helper| core_impl.contains(helper))
        .collect::<Vec<_>>()
}

#[test]
fn should_keep_stream_live_core_helpers_private() {
    // Arrange

    // Act
    let violations = stream_live_core_public_helper_violations();

    // Assert
    assert!(
        violations.is_empty(),
        "StreamDomainCore exposes live state helpers outside the mailbox facade: {violations:?}"
    );
}

fn notice_live_core_public_helper_violations() -> Vec<&'static str> {
    let source = read_repo_file("src/domains/notice/sink.rs");
    let core_impl = section_between(
        &source,
        "impl NoticeDomainCore {",
        "impl MailboxSink for NoticeDomainSink {",
    );
    let forbidden_public_helpers = [
        "pub fn refresh_admin_snapshot_if_dirty(&self)",
        "pub fn unsubscribe_all_for_session(&self",
        "pub fn subscription_count(&self)",
    ];

    forbidden_public_helpers
        .into_iter()
        .filter(|helper| core_impl.contains(helper))
        .collect::<Vec<_>>()
}

#[test]
fn should_keep_notice_live_core_helpers_private() {
    // Arrange

    // Act
    let violations = notice_live_core_public_helper_violations();

    // Assert
    assert!(
        violations.is_empty(),
        "NoticeDomainCore exposes live state helpers outside the mailbox facade: {violations:?}"
    );
}

fn queue_live_core_public_helper_violations() -> Vec<&'static str> {
    let source = read_repo_file("src/domains/queue/sink/domain_sink_impl.rs");
    let core_impl = section_between(
        &source,
        "impl QueueDomainCore {",
        "\n#[cfg(test)]\nfn test_protocol_channel_from_client",
    );
    let forbidden_public_helpers = [
        "pub fn refresh_admin_snapshot_if_dirty(&self)",
        "pub fn cleanup_session(&self",
        "pub fn pending_message_count(&self)",
        "pub fn ready_message_count(&self)",
        "pub fn delayed_message_count(&self)",
        "pub fn active_inflight_count(&self)",
        "pub fn dead_letter_count(&self)",
        "pub fn replay_dead_letter(",
        "pub fn purge_dead_letter(",
    ];

    forbidden_public_helpers
        .into_iter()
        .filter(|helper| core_impl.contains(helper))
        .collect::<Vec<_>>()
}

#[test]
fn should_keep_queue_live_core_helpers_private() {
    // Arrange

    // Act
    let violations = queue_live_core_public_helper_violations();

    // Assert
    assert!(
        violations.is_empty(),
        "QueueDomainCore exposes live state helpers outside the mailbox facade: {violations:?}"
    );
}

#[test]
fn should_keep_sync_core_free_of_transport_dependencies() {
    // Arrange
    let mut files = Vec::new();
    for directory in ["src/runtime", "src/protocol", "src/session", "src/domains"] {
        collect_rs_files(&repo_root().join(directory), &mut files);
    }
    files.sort();

    let violations = files
        .into_iter()
        .flat_map(|path| {
            let content = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            let relative_path = relative_display(&path);
            content
                .lines()
                .enumerate()
                .flat_map(|(index, line)| {
                    SYNC_CORE_TRANSPORT_FORBIDDEN
                        .iter()
                        .filter(move |needle| line.contains(**needle))
                        .map({
                            let relative_path = relative_path.clone();
                            move |needle| {
                                format!(
                                    "{}:{} contains forbidden dependency {}",
                                    relative_path,
                                    index + 1,
                                    needle
                                )
                            }
                        })
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    // Act
    let report = violations.join("\n");

    // Assert
    assert!(
        report.is_empty(),
        "sync core dependency violations:\n{report}"
    );
}

#[test]
fn should_keep_sync_core_free_of_async_runtime_dependencies() {
    // Arrange
    let mut files = Vec::new();
    for directory in ["src/runtime", "src/protocol", "src/session", "src/domains"] {
        collect_rs_files(&repo_root().join(directory), &mut files);
    }
    files.sort();

    let violations = files
        .into_iter()
        .flat_map(|path| {
            let content = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            let relative_path = relative_display(&path);
            content
                .lines()
                .enumerate()
                .flat_map(|(index, line)| {
                    SYNC_CORE_ASYNC_FORBIDDEN
                        .iter()
                        .filter(move |needle| line.contains(**needle))
                        .map({
                            let relative_path = relative_path.clone();
                            move |needle| {
                                format!(
                                    "{}:{} contains forbidden async dependency {}",
                                    relative_path,
                                    index + 1,
                                    needle
                                )
                            }
                        })
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    // Act
    let report = violations.join("\n");

    // Assert
    assert!(
        report.is_empty(),
        "sync core async dependency violations:\n{report}"
    );
}
