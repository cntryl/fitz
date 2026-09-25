use std::path::{Path, PathBuf};

const MAX_RUST_FILE_LINES: usize = 1_000;
const SCANNED_ROOTS: [&str; 3] = ["src", "tests", "benches"];

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    let mut entries = std::fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| panic!("failed to enumerate {}: {error}", directory.display()));
    entries.sort_by_key(std::fs::DirEntry::path);

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn production_source(mut source: String) -> String {
    const TEST_ATTRIBUTE: &str = "#[cfg(test)]";
    while let Some(attribute_start) = source.find(TEST_ATTRIBUTE) {
        let item_start = attribute_start + TEST_ATTRIBUTE.len();
        let remainder = &source[item_start..];
        let semicolon = remainder.find(';');
        let opening_brace = remainder.find('{');
        let end = match (semicolon, opening_brace) {
            (Some(semicolon), Some(opening_brace)) if semicolon < opening_brace => {
                item_start + semicolon + 1
            }
            (_, Some(opening_brace)) => {
                let opening_brace = item_start + opening_brace;
                let mut depth = 0usize;
                let mut closing_brace = opening_brace;
                for (offset, byte) in source[opening_brace..].bytes().enumerate() {
                    match byte {
                        b'{' => depth += 1,
                        b'}' => {
                            depth -= 1;
                            if depth == 0 {
                                closing_brace = opening_brace + offset + 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                closing_brace
            }
            _ => source[item_start..]
                .find('\n')
                .map_or(source.len(), |newline| item_start + newline + 1),
        };
        source.replace_range(attribute_start..end, "");
    }
    source
}

#[test]
fn should_keep_every_repository_rust_file_below_one_thousand_lines() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    for root in SCANNED_ROOTS {
        collect_rust_files(&workspace.join(root), &mut rust_files);
    }

    // Act
    let oversized = rust_files
        .into_iter()
        .filter_map(|path| {
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            let line_count = source.lines().count();
            (line_count >= MAX_RUST_FILE_LINES).then_some((path, line_count))
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        oversized.is_empty(),
        "Rust files must stay below {MAX_RUST_FILE_LINES} lines: {oversized:#?}"
    );
}

#[test]
fn should_keep_domain_models_from_acting_as_implicit_preludes() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let model_files = [
        "src/domains/lease/sink/model.rs",
        "src/domains/queue/sink/model.rs",
        "src/domains/rpc/sink/state_model/mod.rs",
        "src/domains/schedule/actor/model.rs",
        "src/domains/schedule/store/model.rs",
        "src/domains/stream/sink/model.rs",
    ];

    // Act
    let prelude_exports = model_files
        .into_iter()
        .filter(|relative_path| {
            let source = std::fs::read_to_string(workspace.join(relative_path))
                .unwrap_or_else(|error| panic!("failed to read {relative_path}: {error}"));
            source.lines().any(|line| {
                let line = line.trim_start();
                line.starts_with("pub(super) use crate::")
                    || line.starts_with("pub(crate) use crate::")
                    || line.starts_with("pub(super) use std::")
                    || line.starts_with("pub(crate) use std::")
                    || line.starts_with("pub(super) use chrono::")
                    || line.starts_with("pub(super) use parking_lot::")
                    || line.starts_with("pub(super) use rustc_hash::")
            })
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        prelude_exports.is_empty(),
        "Domain model modules must not re-export dependency preludes: {prelude_exports:?}"
    );
}

#[test]
fn should_keep_runtime_ingress_collaborators_owned() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dispatcher = std::fs::read_to_string(
        workspace.join("src/api/runtime_ingress/domain_frame_dispatcher.rs"),
    )
    .expect("read domain dispatcher");
    let ingress =
        std::fs::read_to_string(workspace.join("src/api/runtime_ingress/types_and_helpers.rs"))
            .expect("read runtime ingress");

    // Act
    let dispatcher_borrows_monolith =
        dispatcher.contains("ingress: &'a RuntimeIngress") || dispatcher.contains("self.ingress");
    let ingress_owns_collaborators = [
        "registry: super::session_registry::SessionRegistry",
        "authenticator: super::session_authenticator::SessionAuthenticator",
        "cleanup: super::session_cleanup_coordinator::SessionCleanupCoordinator",
        "dispatcher: super::domain_frame_dispatcher::DomainFrameDispatcher",
    ]
    .iter()
    .all(|field| ingress.contains(field));

    // Assert
    assert!(!dispatcher_borrows_monolith);
    assert!(ingress_owns_collaborators);
}

#[test]
fn should_keep_queue_transactions_opaque() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source =
        std::fs::read_to_string(workspace.join("src/domains/queue/actor/recovery_store.rs"))
            .expect("read queue store adapter");

    // Act
    let exposes_transaction_by_deref = source.contains("impl std::ops::Deref for QueueTransaction")
        || source.contains("impl std::ops::DerefMut for QueueTransaction");

    // Assert
    assert!(!exposes_transaction_by_deref);
}

#[test]
fn should_keep_domain_inventory_owned_by_domain_kind() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    collect_rust_files(&workspace.join("src"), &mut rust_files);

    // Act
    let legacy_registry_references = rust_files
        .into_iter()
        .filter_map(|path| {
            let source = production_source(
                std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display())),
            );
            source.contains("DomainRegistry").then_some(path)
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        legacy_registry_references.is_empty(),
        "DomainKind is the sole domain inventory: {legacy_registry_references:#?}"
    );
}

#[test]
fn should_convert_write_policy_only_toward_storage_options() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = std::fs::read_to_string(workspace.join("src/storage/write_policy.rs"))
        .expect("read storage write-policy adapter");

    // Act
    let has_forward_conversion = source.contains("impl From<WritePolicy> for WriteOptions");
    let has_reverse_conversion = source.contains("impl From<WriteOptions> for WritePolicy");

    // Assert
    assert!(has_forward_conversion);
    assert!(!has_reverse_conversion);
}

#[test]
fn should_use_domain_names_instead_of_sink_compatibility_names() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    collect_rust_files(&workspace.join("src"), &mut rust_files);
    let legacy_names = [
        "KvDomainSink",
        "QueueDomainSink",
        "NoticeDomainSink",
        "StreamDomainSink",
        "RpcDomainSink",
        "LeaseDomainSink",
        "ScheduleDomainSink",
    ];

    // Act
    let references = rust_files
        .into_iter()
        .filter_map(|path| {
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            legacy_names
                .iter()
                .any(|name| source.contains(name))
                .then_some(path)
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        references.is_empty(),
        "obsolete domain-sink compatibility names remain: {references:#?}"
    );
}

#[test]
fn should_keep_domain_composition_and_endpoints_crate_private() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let declarations = [
        ("src/boot/mod.rs", "pub mod domains;"),
        ("src/boot/domains.rs", "pub struct BrokerDomains"),
        ("src/boot/domains.rs", "pub struct DomainSetupOptions"),
        ("src/domains/kv/mod.rs", "pub mod sink;"),
        ("src/domains/queue/mod.rs", "pub mod sink;"),
        ("src/domains/notice/mod.rs", "pub mod sink;"),
        ("src/domains/stream/mod.rs", "pub mod sink;"),
        ("src/domains/rpc/mod.rs", "pub mod sink;"),
        ("src/domains/lease/mod.rs", "pub mod sink;"),
        ("src/domains/schedule/mod.rs", "pub mod sink;"),
        ("src/domains/kv/mod.rs", "pub mod actor;"),
        ("src/domains/kv/mod.rs", "pub use actor::KvActor;"),
        ("src/domains/queue/mod.rs", "pub mod actor;"),
        ("src/domains/queue/mod.rs", "pub use actor::QueueActor;"),
        ("src/domains/schedule/mod.rs", "pub mod actor;"),
        ("src/domains/schedule/mod.rs", "pub mod store;"),
        (
            "src/domains/schedule/mod.rs",
            "pub use actor::ScheduleActor;",
        ),
        (
            "src/domains/schedule/mod.rs",
            "pub use store::ScheduleStore;",
        ),
        ("src/domains/stream/mod.rs", "pub mod actor;"),
        ("src/domains/stream/mod.rs", "pub mod store;"),
        ("src/domains/stream/mod.rs", "pub mod storage;"),
        ("src/domains/stream/mod.rs", "pub use actor::StreamActor;"),
        ("src/domains/stream/mod.rs", "pub use store::StreamStore;"),
    ];

    // Act
    let public_boundaries = declarations
        .into_iter()
        .filter(|(relative_path, declaration)| {
            std::fs::read_to_string(workspace.join(relative_path))
                .unwrap_or_else(|error| panic!("failed to read {relative_path}: {error}"))
                .lines()
                .any(|line| line.trim() == *declaration)
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        public_boundaries.is_empty(),
        "domain composition and endpoints must remain crate-private: {public_boundaries:?}"
    );
}

#[test]
fn should_route_admin_queries_through_domain_specific_ports() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let domains = std::fs::read_to_string(workspace.join("src/boot/domains.rs"))
        .expect("read domain composition");
    let admin_queries = std::fs::read_to_string(workspace.join("src/boot/stats/admin_queries.rs"))
        .expect("read admin queries");
    let domain_stats = std::fs::read_to_string(workspace.join("src/boot/stats/domain_stats.rs"))
        .expect("read domain stats");

    // Act
    let missing_ports = [
        "KvAdmin",
        "QueueAdmin",
        "NoticeAdmin",
        "StreamAdmin",
        "RpcAdmin",
        "LeaseAdmin",
        "ScheduleAdmin",
    ]
    .into_iter()
    .filter(|port| !domains.contains(&format!("type {port} =")))
    .collect::<Vec<_>>();

    // Assert
    assert!(
        missing_ports.is_empty(),
        "missing admin ports: {missing_ports:?}"
    );
    assert!(!admin_queries.contains(".domains"));
    assert!(!domain_stats.contains(".domains"));
}

#[test]
fn should_keep_production_async_at_api_edge() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    collect_rust_files(&workspace.join("src"), &mut rust_files);

    // Act
    let violations = rust_files
        .into_iter()
        .filter(|path| {
            let relative = path
                .strip_prefix(workspace)
                .expect("workspace-relative path");
            !relative.starts_with("src/api")
                && !relative.starts_with("src/testkit")
                && !relative.starts_with("src/benchkit")
                && !relative
                    .components()
                    .any(|part| part.as_os_str() == "tests")
                && !relative.file_name().is_some_and(|name| {
                    name == "tests.rs" || name.to_string_lossy().ends_with("_tests.rs")
                })
        })
        .filter_map(|path| {
            let source = production_source(
                std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display())),
            );
            ["async fn", ".await", "tokio::"]
                .iter()
                .any(|token| source.contains(token))
                .then_some(path)
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        violations.is_empty(),
        "production async must live under src/api: {violations:#?}"
    );
}

#[test]
fn should_keep_midge_inside_storage_adapters_and_boot_composition() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    collect_rust_files(&workspace.join("src"), &mut rust_files);

    // Act
    let violations = rust_files
        .into_iter()
        .filter(|path| {
            let relative = path
                .strip_prefix(workspace)
                .expect("workspace-relative path");
            let text = relative.to_string_lossy();
            !relative.starts_with("src/boot")
                && relative != Path::new("src/api/broker.rs")
                && relative != Path::new("src/api/broker_shutdown.rs")
                && relative != Path::new("src/api/storage_runtime.rs")
                && !relative.starts_with("src/api/storage_runtime")
                && !relative.starts_with("src/storage")
                && relative != Path::new("src/storage.rs")
                && !relative.starts_with("src/testkit")
                && !relative.starts_with("src/benchkit")
                && !relative
                    .components()
                    .any(|part| part.as_os_str() == "tests")
                && relative.file_name().is_none_or(|name| name != "tests.rs")
                && !text.contains("/store/")
                && !text.ends_with("/store.rs")
                && !relative
                    .file_stem()
                    .is_some_and(|stem| stem.to_string_lossy().ends_with("_store"))
        })
        .filter_map(|path| {
            let source = production_source(
                std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display())),
            );
            (source.contains("cntryl_midge::") || source.contains("FitzStorageEngine"))
                .then_some(path)
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        violations.is_empty(),
        "Midge must remain inside boot, storage, and domain stores: {violations:#?}"
    );
}

#[test]
fn should_keep_family_state_directly_worker_owned() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let family_state_files = [
        "src/domains/notice/sink/state.rs",
        "src/domains/rpc/sink/state_model/sink.rs",
        "src/domains/lease/sink/model.rs",
        "src/domains/queue/sink/model.rs",
        "src/domains/schedule/sink/model.rs",
        "src/domains/stream/sink/model.rs",
    ];

    // Act
    let violations = family_state_files
        .into_iter()
        .filter(|relative_path| {
            let source = std::fs::read_to_string(workspace.join(relative_path))
                .unwrap_or_else(|error| panic!("failed to read {relative_path}: {error}"));
            source.contains("DomainCore")
                || source.contains("family_cores")
                || source.contains("actors: Mutex")
                || source.contains("actor: Arc<Mutex")
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        violations.is_empty(),
        "family state must be directly owned by its worker: {violations:?}"
    );
}

/// Whether an `impl SessionScoped` block redefines a provided protocol method.
fn overrides_session_protocol(compact: &str) -> bool {
    compact.split("implSessionScopedfor").skip(1).any(|block| {
        let body = block.split("impl").next().unwrap_or(block);
        [
            "fncleanup_session(",
            "fnhandle_cleanup_envelope(",
            "fnis_cleaned_up_session(",
        ]
        .iter()
        .any(|provided| body.contains(provided))
    })
}

#[test]
fn should_keep_session_cleanup_protocol_owned_by_runtime() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect_rust_files(&workspace.join("src/domains"), &mut files);

    // Act
    let offenders = files
        .iter()
        .filter_map(|path| {
            let source = production_source(std::fs::read_to_string(path).ok()?);
            let compact: String = source.chars().filter(|c| !c.is_whitespace()).collect();
            let hand_rolled = (compact.contains(".payload::<")
                && compact.contains("SessionCleanup>"))
                || compact.contains("cleaned_up_sessions.mark(")
                || compact.contains("cleaned_up_sessions.contains(")
                || compact.contains("CleanedUpSessions::mark(")
                || compact.contains("CleanedUpSessions::contains(")
                || overrides_session_protocol(&compact);
            hand_rolled.then(|| {
                path.strip_prefix(workspace)
                    .unwrap_or(path)
                    .display()
                    .to_string()
            })
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        offenders.is_empty(),
        "domains must use runtime::SessionScoped / session_cleanup_id: {offenders:?}"
    );
}

fn production_sources_under(workspace: &Path, directory: &str) -> Vec<(String, String)> {
    let mut files = Vec::new();
    collect_rust_files(&workspace.join(directory), &mut files);
    files
        .iter()
        .filter_map(|path| {
            let relative = path.strip_prefix(workspace).unwrap_or(path);
            let is_test_file = relative.file_name().is_some_and(|name| name == "tests.rs")
                || relative
                    .components()
                    .any(|part| part.as_os_str() == "tests");
            if is_test_file {
                return None;
            }
            let source = production_source(std::fs::read_to_string(path).ok()?);
            Some((relative.display().to_string(), source))
        })
        .collect()
}

#[test]
fn should_keep_auth_route_grammar_owned_by_domains() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = production_sources_under(workspace, "src/api/runtime_ingress");

    // Act
    let offenders = sources
        .iter()
        .filter(|(_, source)| {
            source.contains("route_grammar::")
                || source.contains("auth_route::")
                || source.contains("route_triplet(")
                || source.contains("route_exact_triplet(")
                || source.contains("fn canonicalize_stream_route_str")
                || source.contains("fn canonicalize_lease_route_str")
                || source.contains("fn canonicalize_triplet_route_str")
        })
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();

    // Assert
    assert!(
        offenders.is_empty(),
        "ingress must call each domain's canonical_auth_route: {offenders:?}"
    );
}

#[test]
fn should_keep_schedule_run_now_validation_owned_by_schedule() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = production_sources_under(workspace, "src/api/admin");
    sources.extend(production_sources_under(workspace, "src/boot"));

    // Act
    let offenders = sources
        .iter()
        .filter(|(_, source)| {
            source.contains("validate_concrete_schedule_route")
                || source.contains("schedule::sink::ScheduleRunNow")
                || source.contains("schedule::sink::{")
        })
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();

    // Assert
    assert!(
        offenders.is_empty(),
        "admin/boot must use the schedule domain's public run-now API: {offenders:?}"
    );
}

#[test]
fn should_import_domains_only_through_their_public_modules() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = production_sources_under(workspace, "src/api");
    sources.extend(production_sources_under(workspace, "src/boot"));
    let domains = [
        "kv", "lease", "notice", "queue", "rpc", "schedule", "stream",
    ];

    // Act
    let offenders = sources
        .iter()
        .filter(|(_, source)| {
            // Flatten `use` trees so `domains::{kv::{sink::X}}` and
            // `use crate::domains::kv; kv::sink::X` read as plain paths.
            let flat: String = source
                .chars()
                .filter(|c| !c.is_whitespace() && !matches!(c, '{' | '}' | ','))
                .collect();
            domains.iter().any(|domain| {
                ["sink", "store"].iter().any(|internal| {
                    let path = format!("{domain}::{internal}");
                    flat.match_indices(&path).any(|(index, _)| {
                        let before = flat[..index].chars().next_back();
                        let after = flat[index + path.len()..].chars().next();
                        !before.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                            && !after.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                    })
                })
            })
        })
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();

    // Assert
    assert!(
        offenders.is_empty(),
        "api/boot must import domain types from the domain module, not sink/store: {offenders:?}"
    );
}

#[test]
fn should_keep_domain_metric_keys_out_of_admin_transport() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = production_sources_under(workspace, "src/api");

    // Act
    let offenders = sources
        .iter()
        .filter(|(_, source)| {
            let flat: String = source.chars().filter(|c| !c.is_whitespace()).collect();
            [
                "kv", "lease", "notice", "queue", "rpc", "schedule", "stream",
            ]
            .iter()
            .any(|domain| {
                flat.contains(&format!("{domain}::metrics::")) && flat.contains("METRIC_")
            }) || flat.contains("metrics::*")
        })
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();

    // Assert
    assert!(
        offenders.is_empty(),
        "admin must read domain metric snapshots, not domain metric keys: {offenders:?}"
    );
}

#[test]
fn should_keep_per_domain_tables_free_of_positional_and_message_id_quirks() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    for directory in [
        "src/api",
        "src/runtime",
        "src/dispatch",
        "src/boot",
        "src/protocol",
    ] {
        sources.extend(production_sources_under(workspace, directory));
    }
    let quirks = [
        "DOMAIN_DESCRIPTORS[0]",
        "DOMAIN_DESCRIPTORS[6]",
        "(DispatchDomain::Schedule,703",
        "msg_type.as_u16()==302",
        "msg_type.as_u16()!=706",
    ];

    // Act
    let offenders = sources
        .iter()
        .filter_map(|(path, source)| {
            let flat: String = source.chars().filter(|c| !c.is_whitespace()).collect();
            let mut found = quirks
                .iter()
                .filter(|quirk| flat.contains(*quirk))
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            // A `"kv" =>` match arm (after `{`, `,` or `}`) is a per-domain
            // table keyed by scheme string; `scheme == "kv" =>` guards are not.
            let schemes = [
                "kv", "queue", "notice", "stream", "rpc", "lease", "schedule",
            ];
            let lists_every_scheme_as_arm = schemes.iter().all(|scheme| {
                let quoted = format!("\"{scheme}\"");
                let as_arm = ["{", ",", "}", "|"].iter().any(|before| {
                    ["=>", "|"]
                        .iter()
                        .any(|after| flat.contains(&format!("{before}{quoted}{after}")))
                });
                as_arm || flat.contains(&format!("|{quoted})"))
            });
            if lists_every_scheme_as_arm {
                found.push("scheme-string match arm".to_string());
            }
            (!found.is_empty()).then(|| format!("{path}: {found:?}"))
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        offenders.is_empty(),
        "per-domain facts belong in DomainDescriptor / IngressDomainDescriptor: {offenders:?}"
    );
}

#[test]
fn should_keep_storage_facade_from_leaking_the_raw_engine() {
    // Arrange
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = production_source(
        std::fs::read_to_string(workspace.join("src/storage.rs")).expect("read storage facade"),
    );
    let flat: String = source.chars().filter(|c| !c.is_whitespace()).collect();

    // Act
    let leaks = [
        "fninner(",
        "fnclone_inner(",
        "implAsRef<cntryl_midge::Engine>",
    ]
    .into_iter()
    .filter(|leak| flat.contains(leak))
    .collect::<Vec<_>>();

    // Assert
    assert!(
        leaks.is_empty(),
        "FitzStorageEngine must expose operations, not the Midge engine: {leaks:?}"
    );
}
