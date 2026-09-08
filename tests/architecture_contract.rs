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
        "src/domains/queue/sink/model.rs",
        "src/domains/schedule/actor/model.rs",
        "src/domains/schedule/store/model.rs",
    ];

    // Act
    let prelude_exports = model_files
        .into_iter()
        .filter(|relative_path| {
            let source = std::fs::read_to_string(workspace.join(relative_path))
                .unwrap_or_else(|error| panic!("failed to read {relative_path}: {error}"));
            source.contains("pub(super) use") || source.contains("pub(crate) use")
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        prelude_exports.is_empty(),
        "Domain model modules must not re-export dependency preludes: {prelude_exports:?}"
    );
}
