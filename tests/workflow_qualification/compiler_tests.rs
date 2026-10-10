use super::*;

fn variants() -> (Value, Value) {
    let before = json!({"fixtures":{"benches/gate.rs":"fixture"},"manifest_sha256":"old-manifest","lock_sha256":"old-lock",
        "dependencies":[["rmcp","3.5.0","registry"]],"compiled_source_inputs":{"src/lib.rs":"unchanged"}});
    let mut after = before.clone();
    after["manifest_sha256"] = json!("new-manifest");
    after["lock_sha256"] = json!("new-lock");
    after["dependencies"] = json!([["rmcp", "3.5.1", "registry"]]);
    let hashes: Value = super::super::stream::groups()
        .into_iter()
        .map(|(target, _)| (target, json!("a".repeat(64))))
        .collect();
    (
        json!({"before":before,"after":after}),
        json!({"before":hashes,"after":hashes}),
    )
}

#[test]
fn should_accept_independently_built_equivalent_dependency_artifacts() {
    // Arrange
    let (provenance, hashes) = variants();
    // Act
    let equivalent = binary_equivalence("dependencies", &provenance, &hashes).unwrap();
    // Assert
    assert!(equivalent);
}

#[test]
fn should_reject_reused_binary_when_compiled_source_changed() {
    // Arrange
    let (mut provenance, hashes) = variants();
    provenance["after"]["compiled_source_inputs"]["src/lib.rs"] = json!("changed");
    // Act
    let result = binary_equivalence("dependencies", &provenance, &hashes);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_retain_source_comparison_alias_guard() {
    // Arrange
    let (mut provenance, hashes) = variants();
    for field in ["manifest_sha256", "lock_sha256", "dependencies"] {
        provenance["after"][field] = provenance["before"][field].clone();
    }
    // Act
    let result = binary_equivalence("source", &provenance, &hashes);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_normalized_dependency_comparison() {
    // Arrange
    let (mut provenance, hashes) = variants();
    for field in ["manifest_sha256", "lock_sha256"] {
        provenance["after"][field] = provenance["before"][field].clone();
    }
    // Act
    let result = binary_equivalence("dependencies", &provenance, &hashes);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_mismatched_benchmark_fixtures() {
    // Arrange
    let (mut provenance, hashes) = variants();
    provenance["after"]["fixtures"]["benches/gate.rs"] = json!("changed");
    // Act
    let result = binary_equivalence("dependencies", &provenance, &hashes);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_a_missing_benchmark_artifact() {
    // Arrange
    let (provenance, mut hashes) = variants();
    hashes["after"]
        .as_object_mut()
        .unwrap()
        .remove("tier4_stream_gate");
    // Act
    let result = binary_equivalence("dependencies", &provenance, &hashes);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_measure_when_a_dependency_benchmark_binary_differs() {
    // Arrange
    let (provenance, mut hashes) = variants();
    hashes["after"]["tier4_stream_gate"] = json!("b".repeat(64));
    // Act
    let equivalent = binary_equivalence("dependencies", &provenance, &hashes).unwrap();
    // Assert
    assert!(!equivalent);
}

#[test]
fn should_measure_changed_source_with_distinct_binaries() {
    // Arrange
    let (mut provenance, mut hashes) = variants();
    provenance["after"]["compiled_source_inputs"]["src/lib.rs"] = json!("changed");
    for value in hashes["after"].as_object_mut().unwrap().values_mut() {
        *value = json!("b".repeat(64));
    }
    // Act
    let equivalent = binary_equivalence("dependencies", &provenance, &hashes).unwrap();
    // Assert
    assert!(!equivalent);
}

fn library_fixture(root: &Path) -> (PathBuf, Value) {
    fs::create_dir(root.join("src")).unwrap();
    fs::write(root.join("src/lib.rs"), "pub fn value() {}\n").unwrap();
    fs::write(
        root.join("src/value with spaces.rs"),
        "pub const VALUE: u8 = 1;\n",
    )
    .unwrap();
    fs::write(root.join("src/boundary_tests.rs"), "test-only fixture\n").unwrap();
    let library = root.join("libfitz-example.rlib");
    fs::write(&library, "").unwrap();
    fs::write(
        root.join("fitz-example.d"),
        format!(
            "{}: src/lib.rs src/value\\ with\\ spaces.rs\n",
            library.display()
        ),
    )
    .unwrap();
    let row = json!({"reason":"compiler-artifact","package_id":"fitz-package","target":{"kind":["lib"],"src_path":root.join("src/lib.rs")},"fresh":false,"filenames":[library]});
    let log = root.join("build.log");
    fs::write(&log, row.to_string()).unwrap();
    (log, row)
}

#[test]
fn should_record_actual_compiler_inputs_including_escaped_spaces() {
    // Arrange
    let directory = tempfile::Builder::new()
        .prefix("stream inputs ")
        .tempdir()
        .unwrap();
    let (log, _) = library_fixture(directory.path());
    // Act
    let witness = compiled_inputs(directory.path(), &log, "fitz-package").unwrap();
    // Assert
    assert_eq!(
        witness["hashes"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["src/lib.rs", "src/value with spaces.rs"]
    );
    assert!(witness["hashes"].get("src/boundary_tests.rs").is_none());
}

#[test]
fn should_reject_a_cached_library_artifact() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let (log, mut row) = library_fixture(directory.path());
    row["fresh"] = json!(true);
    fs::write(&log, row.to_string()).unwrap();
    // Act
    let result = compiled_inputs(directory.path(), &log, "fitz-package");
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_an_artifact_from_another_package() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let (log, _) = library_fixture(directory.path());
    // Act
    let result = compiled_inputs(directory.path(), &log, "wrong-package");
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_an_artifact_from_another_source() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let (log, _) = library_fixture(directory.path());
    fs::write(directory.path().join("src/other.rs"), "").unwrap();
    // Act
    let result = fresh_artifact(
        &log,
        "fitz-package",
        "lib",
        &directory.path().join("src/other.rs"),
        None,
    );
    // Assert
    assert!(result.is_err());
}
