use std::path::Path;
use std::process::Command;

fn prohibited_workflow(source: &str) -> Vec<&str> {
    let mut frontend = false;
    let mut ui_directory = false;
    source
        .lines()
        .filter(|line| {
            if line.starts_with("  ") && !line.starts_with("   ") && line.trim_end().ends_with(':')
            {
                frontend = line.trim() == "frontend:";
                ui_directory = false;
            }
            if frontend && line.trim() == "working-directory: ui" {
                ui_directory = true;
            }
            let lower = line.to_ascii_lowercase();
            let tokens: Vec<_> = lower
                .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-' && ch != '.')
                .collect();
            let python = tokens.iter().any(|token| {
                token.starts_with("python")
                    || matches!(
                        *token,
                        "pip"
                            | "pip3"
                            | "poetry"
                            | "uv"
                            | "setup-python"
                            | "setup-uv"
                            | "ruby"
                            | "perl"
                            | "deno"
                            | "bun"
                    )
            });
            let scripting = tokens
                .iter()
                .any(|token| matches!(*token, "node" | "nodejs" | "npm" | "npx" | "setup-node"));
            python || (scripting && !(frontend && ui_directory))
        })
        .collect()
}

fn prohibited_file(path: &str) -> bool {
    let path = path.to_ascii_lowercase();
    let name = path.rsplit('/').next().unwrap_or_default();
    let extension = Path::new(&path)
        .extension()
        .and_then(|value| value.to_str());
    matches!(
        extension,
        Some(
            "py" | "pyi"
                | "pyw"
                | "pyc"
                | "pyo"
                | "ipynb"
                | "sh"
                | "bash"
                | "ps1"
                | "rb"
                | "pl"
                | "lua"
        )
    ) || matches!(
        name,
        "pyproject.toml"
            | "pipfile"
            | "pipfile.lock"
            | "poetry.lock"
            | "uv.lock"
            | ".python-version"
            | "tox.ini"
            | "pytest.ini"
            | "mypy.ini"
            | "ruff.toml"
            | ".ruff.toml"
    ) || (name.starts_with("requirements") && extension == Some("txt"))
        || (!path.starts_with("ui/")
            && matches!(extension, Some("js" | "mjs" | "cjs" | "ts" | "tsx" | "jsx")))
}

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
                .filter(|source| {
                    source.lines().next().is_some_and(|line| {
                        line.starts_with("#!")
                            && !line.starts_with("#![")
                            && ["python", "bash", "/sh", "ruby", "perl", "deno"]
                                .iter()
                                .any(|interpreter| line.contains(interpreter))
                            || (!name.starts_with("ui/")
                                && line.starts_with("#!")
                                && line.contains("node"))
                    })
                })
        })
        .collect();
    // Assert
    assert!(prohibited.is_empty(), "System test programs must be Rust");
}

#[test]
fn should_reject_inline_python_workflow_programs() {
    // Arrange
    let source = "  backend:\n    run: python3 - <<'PY'\n";
    // Act
    let result = prohibited_workflow(source);
    // Assert
    assert_eq!(result.len(), 1);
}

#[test]
fn should_reject_inline_javascript_system_tests() {
    // Arrange
    let source = "  backend:\n    run: node --eval 'test()'\n";
    // Act
    let result = prohibited_workflow(source);
    // Assert
    assert_eq!(result.len(), 1);
}

#[test]
fn should_allow_existing_ui_build_commands() {
    assert_eq!(prohibited_workflow("  frontend:\n    defaults:\n      run:\n        working-directory: ui\n    steps:\n      - run: npm run build\n"), [] as [&str; 0]);
}

#[test]
fn should_reject_nested_python_files() {
    assert!(prohibited_file("tests/helpers/nested.PY"));
}

#[test]
fn should_reject_python_dependency_manifests() {
    assert!(prohibited_file("tools/pyproject.toml"));
}

#[test]
fn should_reject_javascript_system_test_helpers() {
    assert!(prohibited_file("tests/helpers/check.js"));
}

#[test]
fn should_reject_standalone_shell_test_helpers() {
    assert!(prohibited_file("tools/check.sh"));
}

#[test]
fn should_allow_existing_ui_application_tooling() {
    assert!(!prohibited_file("ui/vite.config.js"));
}
