use std::path::Path;

pub(crate) fn prohibited_workflow(source: &str) -> Vec<&str> {
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
                    || token.starts_with("pypy")
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

pub(crate) fn prohibited_file(path: &str) -> bool {
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
                | "zsh"
                | "ksh"
                | "fish"
                | "ps1"
                | "psm1"
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
            && (matches!(
                extension,
                Some("js" | "mjs" | "cjs" | "ts" | "mts" | "cts" | "tsx" | "jsx")
            ) || matches!(
                name,
                "package.json"
                    | "package-lock.json"
                    | "npm-shrinkwrap.json"
                    | "pnpm-lock.yaml"
                    | "yarn.lock"
                    | "bun.lock"
                    | "bun.lockb"
            )))
}

pub(crate) fn prohibited_shebang(path: &str, source: &str) -> bool {
    let Some(line) = source
        .lines()
        .next()
        .filter(|line| line.starts_with("#!") && !line.starts_with("#!["))
    else {
        return false;
    };
    line.trim_start_matches("#!")
        .split_whitespace()
        .any(|token| {
            let name = Path::new(token)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            name.starts_with("python")
                || name.starts_with("pypy")
                || matches!(
                    name,
                    "sh" | "bash"
                        | "zsh"
                        | "ksh"
                        | "dash"
                        | "ash"
                        | "fish"
                        | "csh"
                        | "tcsh"
                        | "pwsh"
                        | "powershell"
                        | "ruby"
                        | "perl"
                        | "lua"
                )
                || (!path.starts_with("ui/") && matches!(name, "node" | "nodejs" | "deno" | "bun"))
        })
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

#[test]
fn should_reject_typescript_module_system_test_helpers() {
    // Arrange
    let paths = ["tests/helpers/check.mts", "tools/check.cts"];
    // Act
    let prohibited: Vec<_> = paths
        .into_iter()
        .filter(|path| prohibited_file(path))
        .collect();
    // Assert
    assert_eq!(prohibited.len(), paths.len());
}

#[test]
fn should_reject_node_package_manifests_outside_the_ui() {
    assert!(prohibited_file("tools/package.json"));
}

#[test]
fn should_allow_typescript_ui_test_modules() {
    // Arrange
    let paths = [
        "ui/tests/check.mts",
        "ui/tests/check.cts",
        "ui/package.json",
    ];
    // Act
    let prohibited: Vec<_> = paths
        .into_iter()
        .filter(|path| prohibited_file(path))
        .collect();
    // Assert
    assert_eq!(prohibited, [] as [&str; 0]);
}

#[test]
fn should_reject_extensionless_env_shell_helpers() {
    // Arrange
    let commands = [
        "#!/usr/bin/env sh",
        "#!/bin/zsh",
        "#!/usr/bin/env -S bash -e",
    ];
    // Act
    let prohibited: Vec<_> = commands
        .into_iter()
        .filter(|source| prohibited_shebang("tools/check", source))
        .collect();
    // Assert
    assert_eq!(prohibited.len(), commands.len());
}

#[test]
fn should_reject_alternate_python_interpreters() {
    assert!(prohibited_shebang("tools/check", "#!/usr/bin/env pypy3"));
}

#[test]
fn should_allow_extensionless_ui_node_tooling() {
    assert!(!prohibited_shebang("ui/tools/check", "#!/usr/bin/env node"));
}

#[test]
fn should_allow_rust_attributes_mentioning_node() {
    // Arrange
    let source = r#"#![cfg(feature = "node")]"#;
    // Act
    let prohibited = prohibited_shebang("tests/check.rs", source);
    // Assert
    assert!(!prohibited);
}
