use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

const MAX_MARKDOWN_LINES: usize = 1_000;

const FORBIDDEN_STALE_PHRASES: &[&str] = &[
    "docs/todos",
    "todo-all",
    "roadmap/",
    "architecture-drift",
    "architecture-remediation",
    "backend-cleanup-audit",
    "production-credibility",
    "one-dot-zero",
    "stability-policy",
    "the-big-idea",
    "pre-1.0",
    "early prototype",
    "not production-ready",
    "production-ready: not yet",
    "experimental: yes",
    "readiness documentation",
    "prototype",
];

const FORBIDDEN_PRODUCT_TERMS: &[&str] = &[
    "aws",
    "azure",
    "google cloud",
    "gcp",
    "gcs",
    "s3",
    "ecs",
    "fargate",
    "alb",
    "kafka",
    "rabbitmq",
    "redis",
    "nats",
    "pulsar",
    "dynamodb",
    "kinesis",
    "sqs",
    "sns",
    "pub/sub",
];

fn collect_markdown_files(path: &Path, files: &mut Vec<PathBuf>) {
    if path.is_file() {
        if path.extension().is_some_and(|extension| extension == "md") {
            files.push(path.to_path_buf());
        }
        return;
    }

    let entries = fs::read_dir(path).unwrap_or_else(|error| {
        panic!("failed to read directory {}: {error}", path.display());
    });

    for entry in entries {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "failed to read directory entry under {}: {error}",
                path.display()
            );
        });
        collect_markdown_files(&entry.path(), files);
    }
}

fn markdown_files() -> Vec<PathBuf> {
    let mut files = vec![PathBuf::from("README.md"), PathBuf::from("CONTRIBUTING.md")];
    collect_markdown_files(Path::new("docs"), &mut files);
    files.sort();
    files
}

fn read_to_string(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", path.display());
    })
}

fn has_token(haystack: &str, needle: &str) -> bool {
    haystack.match_indices(needle).any(|(index, _)| {
        let before = haystack[..index].chars().next_back();
        let after = haystack[index + needle.len()..].chars().next();
        before.is_none_or(|character| !character.is_ascii_alphanumeric())
            && after.is_none_or(|character| !character.is_ascii_alphanumeric())
    })
}

fn markdown_links(contents: &str) -> Vec<(usize, String)> {
    let mut links = Vec::new();
    let mut in_fence = false;

    for (line_index, line) in contents.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }

        let mut offset = 0;
        while let Some(open) = line[offset..].find("](") {
            let target_start = offset + open + 2;
            let Some(close) = line[target_start..].find(')') else {
                break;
            };
            let raw_target = line[target_start..target_start + close].trim();
            let target = raw_target.trim_matches(|character| character == '<' || character == '>');
            links.push((line_index + 1, target.to_string()));
            offset = target_start + close + 1;
        }
    }

    links
}

fn normalize_join(base: &Path, relative: &str) -> PathBuf {
    let mut normalized = PathBuf::new();
    let joined = base.join(relative);

    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }

    normalized
}

fn slugify_heading(heading: &str) -> String {
    let mut slug = String::new();

    for character in heading.trim().to_lowercase().chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            slug.push(character);
        } else if character.is_whitespace() || character == '-' {
            slug.push('-');
        }
    }

    slug
}

fn markdown_anchors(path: &Path) -> HashSet<String> {
    let contents = read_to_string(path);
    contents
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            let heading_marks = trimmed
                .chars()
                .take_while(|character| *character == '#')
                .count();
            if heading_marks == 0 || heading_marks > 6 {
                return None;
            }
            let heading = trimmed.get(heading_marks..)?;
            heading.strip_prefix(' ').map(slugify_heading)
        })
        .collect()
}

fn is_external_link(target: &str) -> bool {
    target.starts_with("http://")
        || target.starts_with("https://")
        || target.starts_with("mailto:")
        || target.starts_with("tel:")
}

fn split_link_target(target: &str) -> (&str, Option<&str>) {
    match target.split_once('#') {
        Some((path, anchor)) => (path, Some(anchor)),
        None => (target, None),
    }
}

#[test]
fn should_keep_markdown_docs_under_line_limit() {
    // Arrange
    let files = markdown_files();

    // Act
    let oversized = files
        .iter()
        .filter_map(|path| {
            let line_count = read_to_string(path).lines().count();
            (line_count > MAX_MARKDOWN_LINES)
                .then(|| format!("{} has {line_count} lines", path.display()))
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(
        oversized.is_empty(),
        "Markdown docs must stay under {MAX_MARKDOWN_LINES} lines:\n{}",
        oversized.join("\n")
    );
}

#[test]
fn should_not_reintroduce_stale_public_positioning() {
    // Arrange
    let files = markdown_files();

    // Act
    let mut failures = Vec::new();
    for path in files {
        let contents = read_to_string(&path);
        let lowered = contents.to_lowercase();

        for phrase in FORBIDDEN_STALE_PHRASES {
            if lowered.contains(phrase) {
                failures.push(format!(
                    "{} contains stale phrase `{phrase}`",
                    path.display()
                ));
            }
        }

        for (line_index, line) in lowered.lines().enumerate() {
            for term in FORBIDDEN_PRODUCT_TERMS {
                if has_token(line, term) {
                    failures.push(format!(
                        "{}:{} contains external product term `{term}`",
                        path.display(),
                        line_index + 1
                    ));
                }
            }
        }
    }

    // Assert
    assert!(
        failures.is_empty(),
        "Documentation hygiene failures:\n{}",
        failures.join("\n")
    );
}

#[test]
fn should_keep_internal_markdown_links_valid() {
    // Arrange
    let files = markdown_files();

    // Act
    let mut failures = Vec::new();
    for path in files {
        let contents = read_to_string(&path);
        let base = path.parent().unwrap_or_else(|| Path::new("."));

        for (line, target) in markdown_links(&contents) {
            if target.is_empty() || is_external_link(&target) {
                continue;
            }

            let (target_path, anchor) = split_link_target(&target);
            let resolved = if target_path.is_empty() {
                path.clone()
            } else {
                normalize_join(base, target_path)
            };

            if !resolved.exists() {
                failures.push(format!(
                    "{}:{line} links to missing target `{target}`",
                    path.display()
                ));
                continue;
            }

            if let Some(anchor) = anchor {
                if resolved.is_file()
                    && resolved
                        .extension()
                        .is_some_and(|extension| extension == "md")
                {
                    let anchors = markdown_anchors(&resolved);
                    if !anchors.contains(anchor) {
                        failures.push(format!(
                            "{}:{line} links to missing anchor `{anchor}` in {}",
                            path.display(),
                            resolved.display()
                        ));
                    }
                }
            }
        }
    }

    // Assert
    assert!(
        failures.is_empty(),
        "Broken Markdown links:\n{}",
        failures.join("\n")
    );
}
