use super::support;
use std::fs;
use std::process::Command;

fn workflow_step(name: &str) -> (String, String) {
    let source = fs::read_to_string(support::root().join(".github/workflows/ci.yml")).unwrap();
    let marker = format!("      - name: {name}\n");
    let step = source
        .split_once(&marker)
        .unwrap()
        .1
        .split("\n      - name:")
        .next()
        .unwrap();
    let (settings, body) = step.split_once("        run: |\n").unwrap();
    (
        settings.into(),
        body.lines()
            .map(|line| line.strip_prefix("          ").unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

#[test]
fn should_propagate_s3_test_failure_while_retaining_its_log() {
    // Arrange
    let (settings, command) =
        workflow_step("Qualify S3 WAL retention and crash recovery at the resource floor");
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("cargo.rs");
    let executable = root.path().join("cargo");
    fs::write(
        &source,
        "fn main() { println!(\"campaign failure evidence\"); std::process::exit(17); }\n",
    )
    .unwrap();
    assert!(Command::new("rustc")
        .arg(&source)
        .arg("-o")
        .arg(&executable)
        .status()
        .unwrap()
        .success());
    let mut shell = Command::new("bash");
    shell.args(["--noprofile", "--norc", "-e"]);
    if settings.lines().any(|line| line == "        shell: bash") {
        shell.args(["-o", "pipefail"]);
    }
    let path = format!(
        "{}:{}",
        root.path().display(),
        std::env::var("PATH").unwrap()
    );
    // Act
    let result = shell
        .args(["-c", &command])
        .current_dir(root.path())
        .env("PATH", path)
        .output()
        .unwrap();
    // Assert
    assert_eq!(
        result.status.code(),
        Some(17),
        "tee must not hide failed S3 tests"
    );
    assert_eq!(
        fs::read_to_string(
            root.path()
                .join("target/fitz-stress/s3-recovery/campaign.log")
        )
        .unwrap(),
        "campaign failure evidence\n"
    );
}

#[test]
fn should_pin_remote_actions_to_named_release_commits() {
    // Arrange
    let directory = support::root().join(".github");
    let mut paths = support::files(&directory, "yml").unwrap();
    paths.extend(support::files(&directory, "yaml").unwrap());
    let mut invalid = Vec::new();
    // Act
    for path in paths {
        for line in fs::read_to_string(&path).unwrap().lines() {
            let line = line.trim().trim_start_matches("- ");
            let Some(rest) = line.strip_prefix("uses:") else {
                continue;
            };
            let mut tokens = rest.split_whitespace();
            let action = tokens.next().unwrap();
            if action.starts_with("./") {
                continue;
            }
            let pinned = action.rsplit_once('@').is_some_and(|(repo, sha)| {
                repo.contains('/')
                    && sha.len() == 40
                    && sha
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            });
            let comment = tokens.collect::<Vec<_>>();
            let named = comment.len() == 2
                && comment[0] == "#"
                && comment[1].strip_prefix('v').is_some_and(|version| {
                    version.split('.').count() == 3
                        && version.split('.').all(|part| {
                            !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())
                        })
                });
            if !pinned || !named {
                invalid.push(format!("{}: {line}", path.display()));
            }
        }
    }
    // Assert
    assert!(
        invalid.is_empty(),
        "Pin action commits with exact release comments: {invalid:?}"
    );
}
