use std::path::Path;
use std::process::Command;

#[path = "policy/rules.rs"]
mod rules;

use rules::{prohibited_file, prohibited_shebang, prohibited_workflow};

#[test]
fn should_keep_system_test_programs_in_rust() {
    // Arrange
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let listing = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .current_dir(root)
        .output()
        .expect("list repository files");
    assert!(listing.status.success());
    let names = String::from_utf8(listing.stdout).expect("UTF-8 repository paths");

    // Act
    let prohibited: Vec<_> = names
        .split('\0')
        .filter(|name| !name.is_empty() && root.join(name).is_file() && prohibited_file(name))
        .collect();

    // Assert
    assert!(
        prohibited.is_empty(),
        "Use Rust tests and Cargo commands: {prohibited:?}"
    );
}

#[test]
fn should_keep_workflow_tests_in_rust() {
    // Arrange
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".github");
    let mut files = super::support::files(&root, "yml").unwrap();
    files.extend(super::support::files(&root, "yaml").unwrap());
    let mut prohibited = Vec::new();
    // Act
    for path in files {
        let source = std::fs::read_to_string(&path).unwrap();
        prohibited.extend(
            prohibited_workflow(&source)
                .into_iter()
                .map(|line| format!("{}: {line}", path.display())),
        );
    }
    // Assert
    assert!(
        prohibited.is_empty(),
        "Invoke Rust tests directly through Cargo: {prohibited:?}"
    );
}

#[test]
fn should_reject_extensionless_interpreter_scripts() {
    // Arrange
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .current_dir(root)
        .output()
        .unwrap();
    assert!(files.status.success());
    // Act
    let prohibited: Vec<_> = String::from_utf8(files.stdout)
        .unwrap()
        .split('\0')
        .filter(|name| !name.is_empty())
        .filter_map(|name| {
            std::fs::read_to_string(root.join(name))
                .ok()
                .filter(|source| prohibited_shebang(name, source))
        })
        .collect();
    // Assert
    assert!(prohibited.is_empty(), "System test programs must be Rust");
}
