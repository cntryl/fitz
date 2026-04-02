use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use regex::Regex;
use walkdir::WalkDir;

use crate::GenerateInventoryArgs;

pub fn run(args: GenerateInventoryArgs) -> Result<i32> {
    let root = args
        .root
        .canonicalize()
        .with_context(|| format!("failed to resolve repository root {}", args.root.display()))?;

    let src_tests = gather_tests(&root, "src")?;
    let integration_tests = gather_tests(&root, "tests")?;
    let benches = gather_benches(&root, "benches")?;
    let markdown = render_markdown(&src_tests, &integration_tests, &benches);

    let output_path = root.join(&args.output);
    fs::write(&output_path, markdown.as_bytes())
        .with_context(|| format!("failed to write {}", output_path.display()))?;

    println!("Wrote inventory to {}", output_path.display());
    println!(
        "Found {} src tests in {} files",
        src_tests.values().map(Vec::len).sum::<usize>(),
        src_tests.len()
    );
    println!(
        "Found {} integration tests in {} files",
        integration_tests.values().map(Vec::len).sum::<usize>(),
        integration_tests.len()
    );
    println!(
        "Found {} benches/stress in {} files",
        benches.values().map(Vec::len).sum::<usize>(),
        benches.len()
    );

    if args.replace {
        let target = root.join("inventory.md");
        fs::write(&target, markdown.as_bytes())
            .with_context(|| format!("failed to write {}", target.display()))?;
        println!("Replaced {} with generated inventory", target.display());
    }

    Ok(0)
}

fn gather_tests(root: &Path, subdir: &str) -> Result<BTreeMap<String, Vec<String>>> {
    let pattern = Regex::new(r"(?m)^\s*fn\s+(should_[A-Za-z0-9_]+)")?;
    gather_named_functions(root, subdir, &pattern)
}

fn gather_benches(root: &Path, subdir: &str) -> Result<BTreeMap<String, Vec<String>>> {
    let bench_pattern = Regex::new(r"(?m)^\s*fn\s+(bench_[A-Za-z0-9_]+)")?;
    let stress_pattern =
        Regex::new(r"(?m)^\s*#\s*\[\s*stress_test.*\]\s*\n\s*fn\s+([A-Za-z0-9_]+)")?;

    let mut mapping = gather_named_functions(root, subdir, &bench_pattern)?;
    let bench_root = root.join(subdir);
    if !bench_root.exists() {
        return Ok(mapping);
    }

    for file in rust_files_under(&bench_root) {
        let text = fs::read_to_string(&file)
            .with_context(|| format!("failed to read {}", file.display()))?;
        let names = mapping.entry(relative_path(root, &file)).or_default();
        for captures in stress_pattern.captures_iter(&text) {
            let matched = captures
                .get(1)
                .map(|value| value.as_str())
                .unwrap_or_default();
            if !names.iter().any(|name| name == matched) {
                names.push(matched.to_string());
            }
        }
        names.sort();
    }

    mapping.retain(|_, names| !names.is_empty());
    Ok(mapping)
}

fn gather_named_functions(
    root: &Path,
    subdir: &str,
    pattern: &Regex,
) -> Result<BTreeMap<String, Vec<String>>> {
    let search_root = root.join(subdir);
    let mut mapping = BTreeMap::new();
    if !search_root.exists() {
        return Ok(mapping);
    }

    for file in rust_files_under(&search_root) {
        let text = fs::read_to_string(&file)
            .with_context(|| format!("failed to read {}", file.display()))?;
        let mut names = Vec::new();
        for captures in pattern.captures_iter(&text) {
            let matched = captures
                .get(1)
                .map(|value| value.as_str())
                .unwrap_or_default();
            if is_commented_out(
                &text,
                captures.get(0).map(|value| value.start()).unwrap_or(0),
            ) {
                continue;
            }
            names.push(matched.to_string());
        }

        if !names.is_empty() {
            names.sort();
            names.dedup();
            mapping.insert(relative_path(root, &file), names);
        }
    }

    Ok(mapping)
}

fn is_commented_out(text: &str, match_start: usize) -> bool {
    let line_start = text[..match_start]
        .rfind('\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    let prefix = text[line_start..match_start].trim();
    prefix.starts_with("//") || prefix.starts_with("///") || prefix.starts_with("/*")
}

fn rust_files_under(root: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "rs"))
        .map(|entry| entry.into_path())
        .collect();
    files.sort();
    files
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn render_markdown(
    src_tests: &BTreeMap<String, Vec<String>>,
    integration_tests: &BTreeMap<String, Vec<String>>,
    benches: &BTreeMap<String, Vec<String>>,
) -> String {
    let mut lines = Vec::new();
    let generated_at = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    lines.push("# Test & Bench Inventory".to_string());
    lines.push(String::new());
    lines.push(format!(
        "_Generated {} by `cargo fitz-tools generate-inventory`._",
        generated_at
    ));
    lines.push(String::new());
    lines.push("Complete inventory of all test and benchmark functions across Fitz.".to_string());
    lines.push(String::new());
    lines.push("**Src Tests**".to_string());
    lines.push(String::new());
    append_inventory_section(&mut lines, src_tests, "tests");
    lines.push("**Integration Tests (tests/)**".to_string());
    lines.push(String::new());
    append_inventory_section(&mut lines, integration_tests, "tests");
    lines.push("**Benches & Stress (benches/)**".to_string());
    lines.push(String::new());
    append_inventory_section(&mut lines, benches, "benches & stress");
    lines.join("\n")
}

fn append_inventory_section(
    lines: &mut Vec<String>,
    mapping: &BTreeMap<String, Vec<String>>,
    label: &str,
) {
    if mapping.is_empty() {
        lines.push("(none)".to_string());
        lines.push(String::new());
        return;
    }

    for (file, names) in mapping {
        lines.push(format!("- `{file}`"));
        lines.push(format!("  - {label}:"));
        for name in names {
            lines.push(format!("    - `{name}`"));
        }
        lines.push(String::new());
    }
}
