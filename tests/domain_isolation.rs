use std::fs;
use std::path::{Path, PathBuf};

const DOMAINS: &[&str] = &["kv", "lease", "notice", "queue", "rpc", "schedule", "stream"];

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct OwnedSourceFile {
    owner: &'static str,
    path: PathBuf,
}

#[test]
fn should_disallow_foreign_domain_module_references() {
    // Arrange
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let files = domain_owned_source_files(&repo_root);
    let violations = collect_foreign_domain_module_reference_violations(&repo_root, &files);

    // Act
    let report = format_violation_report(&violations);

    // Assert
    assert!(
        report.is_empty(),
        "found cross-domain module references:\n{report}"
    );
}

#[test]
fn should_disallow_foreign_domain_route_schemes() {
    // Arrange
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let files = domain_owned_source_files(&repo_root);
    let violations = collect_foreign_domain_route_scheme_violations(&repo_root, &files);

    // Act
    let report = format_violation_report(&violations);

    // Assert
    assert!(
        report.is_empty(),
        "found cross-domain route schemes:\n{report}"
    );
}

fn domain_owned_source_files(repo_root: &Path) -> Vec<OwnedSourceFile> {
    let mut files = Vec::new();

    for &domain in DOMAINS {
        collect_rust_files(
            &repo_root.join("src").join("domains").join(domain),
            domain,
            &mut files,
        );
    }

    collect_boot_domain_files(&repo_root.join("src").join("boot").join("domains"), &mut files);
    files.sort();
    files
}

fn collect_rust_files(dir: &Path, owner: &'static str, files: &mut Vec<OwnedSourceFile>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|error| panic!("failed to read {}: {error}", dir.display()));

    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("failed to read entry in {}: {error}", dir.display()));
        let path = entry.path();

        if path.is_dir() {
            collect_rust_files(&path, owner, files);
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            files.push(OwnedSourceFile { owner, path });
        }
    }
}

fn collect_boot_domain_files(dir: &Path, files: &mut Vec<OwnedSourceFile>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|error| panic!("failed to read {}: {error}", dir.display()));

    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("failed to read entry in {}: {error}", dir.display()));
        let path = entry.path();

        if path.is_dir() {
            collect_boot_domain_files(&path, files);
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }

        if let Some(owner) = boot_domain_owner(&path) {
            files.push(OwnedSourceFile { owner, path });
        }
    }
}

fn boot_domain_owner(path: &Path) -> Option<&'static str> {
    let stem = path.file_stem()?.to_str()?;

    DOMAINS
        .iter()
        .copied()
        .find(|domain| stem == *domain || stem.starts_with(&format!("{domain}_")))
}

fn collect_foreign_domain_module_reference_violations(
    repo_root: &Path,
    files: &[OwnedSourceFile],
) -> Vec<String> {
    let mut violations = Vec::new();

    for file in files {
        let content = fs::read_to_string(&file.path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.path.display()));

        for (index, line) in content.lines().enumerate() {
            for other_domain in DOMAINS {
                if *other_domain == file.owner {
                    continue;
                }

                let needle = format!("crate::domains::{other_domain}::");
                if line.contains(&needle) {
                    violations.push(format!(
                        "{}:{} references {} domain module",
                        relative_display_path(repo_root, &file.path),
                        index + 1,
                        other_domain
                    ));
                }
            }
        }
    }

    violations
}

fn collect_foreign_domain_route_scheme_violations(
    repo_root: &Path,
    files: &[OwnedSourceFile],
) -> Vec<String> {
    let mut violations = Vec::new();

    for file in files {
        let content = fs::read_to_string(&file.path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.path.display()));

        for (index, line) in content.lines().enumerate() {
            for other_domain in DOMAINS {
                if *other_domain == file.owner {
                    continue;
                }

                let needle = format!("{other_domain}://");
                if line.contains(&needle) {
                    violations.push(format!(
                        "{}:{} hard-codes {} route scheme",
                        relative_display_path(repo_root, &file.path),
                        index + 1,
                        other_domain
                    ));
                }
            }
        }
    }

    violations
}

fn relative_display_path(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn format_violation_report(violations: &[String]) -> String {
    if violations.is_empty() {
        return String::new();
    }

    violations.join("\n")
}