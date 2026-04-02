use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use regex::Regex;
use serde::Serialize;
use walkdir::WalkDir;

use crate::ValidateTestsArgs;

#[derive(Debug, Clone, Serialize)]
struct TestResult {
    file: String,
    test: String,
    line: usize,
    line_count: usize,
    issues: Vec<String>,
}

impl TestResult {
    fn is_compliant(&self) -> bool {
        self.issues.is_empty()
    }
}

pub fn run(args: ValidateTestsArgs) -> Result<i32> {
    match (args.summary, args.file) {
        (true, maybe_file) => {
            let results = collect_repo_results()?;
            print_summary(&results);
            if let Some(path) = args.json {
                write_json(&path, &results)?;
                println!("Wrote JSON report to {}", path.display());
            }
            if maybe_file.is_some() {
                eprintln!("warning: ignoring --file because --summary was provided");
            }
        }
        (false, Some(file)) => {
            let results = find_tests_in_file(&file)?;
            print_file_results(&file, &results);
            if let Some(path) = args.json {
                write_json(&path, &results)?;
                println!("Wrote JSON report to {}", path.display());
            }
        }
        (false, None) => {
            println!("Test Validation Helper");
            println!("======================");
            println!();
            println!("Usage:");
            println!("  cargo fitz-tools validate-tests --summary");
            println!("  cargo fitz-tools validate-tests --file src/backup.rs");
        }
    }

    Ok(0)
}

fn write_json(path: &Path, results: &[TestResult]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let content = serde_json::to_string_pretty(results)?;
    fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn collect_repo_results() -> Result<Vec<TestResult>> {
    let mut results = Vec::new();
    for directory in [Path::new("src"), Path::new("tests")] {
        if !directory.exists() {
            continue;
        }

        for file in find_all_rust_files(directory) {
            results.extend(find_tests_in_file(&file)?);
        }
    }

    Ok(results)
}

fn find_all_rust_files(dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(dir)
        .into_iter()
        .filter_entry(|entry| {
            let name = entry.file_name().to_string_lossy();
            entry.depth() == 0 || (!name.starts_with('.') && name != "target")
        })
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "rs"))
        .map(|entry| entry.into_path())
        .collect()
}

fn find_tests_in_file(file_path: &Path) -> Result<Vec<TestResult>> {
    let content = fs::read_to_string(file_path)
        .with_context(|| format!("failed to read {}", file_path.display()))?;
    let lines: Vec<&str> = content.lines().collect();

    let test_attr_re = Regex::new(r"^\s*#\[test\]\s*$")?;
    let fn_name_re = Regex::new(r"^\s*fn\s+(\w+)")?;

    let mut results = Vec::new();
    for index in 0..lines.len() {
        if !test_attr_re.is_match(lines[index]) || index + 1 >= lines.len() {
            continue;
        }

        if let Some(captures) = fn_name_re.captures(lines[index + 1]) {
            let test_name = captures
                .get(1)
                .map(|value| value.as_str())
                .unwrap_or_default();
            results.push(analyze_test(file_path, test_name, index + 2, &lines)?);
        }
    }

    Ok(results)
}

fn analyze_test(
    file_path: &Path,
    test_name: &str,
    fn_line_number: usize,
    lines: &[&str],
) -> Result<TestResult> {
    let test_start = fn_line_number.saturating_sub(1);
    let mut test_end = lines.len().saturating_sub(1);
    let mut brace_depth = 0usize;
    let mut found_open_brace = false;

    for (index, line) in lines.iter().enumerate().skip(test_start) {
        for ch in line.chars() {
            match ch {
                '{' => {
                    brace_depth = brace_depth.saturating_add(1);
                    found_open_brace = true;
                }
                '}' => {
                    brace_depth = brace_depth.saturating_sub(1);
                    if found_open_brace && brace_depth == 0 {
                        test_end = index;
                        break;
                    }
                }
                _ => {}
            }
        }

        if found_open_brace && brace_depth == 0 {
            break;
        }
    }

    let test_body = lines[test_start..=test_end].join("\n");
    let test_lines = test_end.saturating_sub(test_start) + 1;
    let mut issues = Vec::new();

    if !test_name.starts_with("should_") {
        issues.push("NAMING: Does not start with 'should_'".to_string());
    }

    if test_lines > 5 {
        let arrange_re = Regex::new(r"//\s*Arrange")?;
        let act_re = Regex::new(r"//\s*Act")?;
        let assert_re = Regex::new(r"//\s*Assert")?;
        let combined_re = Regex::new(r"//\s*(Arrange|Act|Assert)\s*[+&]")?;

        if !arrange_re.is_match(&test_body) {
            issues.push("AAA: Missing '// Arrange' comment".to_string());
        }
        if !act_re.is_match(&test_body) {
            issues.push("AAA: Missing '// Act' comment".to_string());
        }
        if !assert_re.is_match(&test_body) {
            issues.push("AAA: Missing '// Assert' comment".to_string());
        }
        if combined_re.is_match(&test_body) {
            issues.push("AAA: Has combined AAA comment (e.g., '// Arrange + Act')".to_string());
        }
    }

    let act_count_re = Regex::new(r"^\s*//\s*Act(\s|$)")?;
    let mut act_count = 0usize;
    let mut in_string = false;

    for line in test_body.lines() {
        let trimmed = line.trim();
        let quote_count = trimmed.matches('"').count();
        if quote_count % 2 == 1 {
            in_string = !in_string;
        }
        if !in_string && act_count_re.is_match(line) {
            act_count += 1;
        }
    }

    if act_count > 1 {
        issues.push(format!(
            "MULTI-BEHAVIOR: Has {} '// Act' sections",
            act_count
        ));
    }

    if test_name.contains("_and_") {
        let action_part = if let Some((before, _)) = test_name.split_once("_given_") {
            before
        } else if let Some((before, _)) = test_name.split_once("_when_") {
            before
        } else {
            test_name
        };

        if action_part.contains("_and_") {
            let allowed_patterns = [
                "with_id_and_name",
                "point_writes_and_range_deletes",
                "memtable_and_sst",
                "large_keys_and_values",
            ];
            if !allowed_patterns
                .iter()
                .any(|pattern| test_name.contains(pattern))
            {
                issues.push(
                    "MULTI-BEHAVIOR: Test name contains '_and_' in action (may test multiple behaviors)".to_string(),
                );
            }
        }
    }

    Ok(TestResult {
        file: file_path.display().to_string(),
        test: test_name.to_string(),
        line: fn_line_number,
        line_count: test_lines,
        issues,
    })
}

fn print_summary(results: &[TestResult]) {
    println!("Scanning all tests for guideline violations...\n");

    let total_count = results.len();
    let compliant_count = results
        .iter()
        .filter(|result| result.is_compliant())
        .count();
    let non_compliant: Vec<&TestResult> = results
        .iter()
        .filter(|result| !result.is_compliant())
        .collect();

    println!("Summary:");
    println!("  Total tests: {total_count}");
    let compliant_pct = if total_count == 0 {
        0.0
    } else {
        (compliant_count as f64 / total_count as f64) * 100.0
    };
    println!("  Compliant: {compliant_count} ({compliant_pct:.1}%)");
    let non_compliant_pct = if total_count == 0 {
        0.0
    } else {
        (non_compliant.len() as f64 / total_count as f64) * 100.0
    };
    println!(
        "  Non-compliant: {} ({non_compliant_pct:.1}%)\n",
        non_compliant.len()
    );

    let naming_issues = non_compliant
        .iter()
        .filter(|result| {
            result
                .issues
                .iter()
                .any(|issue| issue.starts_with("NAMING:"))
        })
        .count();
    let aaa_issues = non_compliant
        .iter()
        .filter(|result| result.issues.iter().any(|issue| issue.starts_with("AAA:")))
        .count();
    let multi_issues = non_compliant
        .iter()
        .filter(|result| {
            result
                .issues
                .iter()
                .any(|issue| issue.starts_with("MULTI-BEHAVIOR:"))
        })
        .count();

    println!("Issue breakdown:");
    println!("  Naming violations: {naming_issues}");
    println!("  AAA structure violations: {aaa_issues}");
    println!("  Multi-behavior violations: {multi_issues}\n");

    if non_compliant.is_empty() {
        return;
    }

    println!("Sample of non-compliant tests (first 20):");
    for result in non_compliant.iter().take(20) {
        println!("  {}::{}  (line {})", result.file, result.test, result.line);
        for issue in &result.issues {
            println!("    - {issue}");
        }
    }

    let multi_violations: Vec<&TestResult> = non_compliant
        .into_iter()
        .filter(|result| {
            result
                .issues
                .iter()
                .any(|issue| issue.starts_with("MULTI-BEHAVIOR:"))
        })
        .collect();
    if multi_violations.is_empty() {
        return;
    }

    println!("\nAll Multi-behavior violations:");
    for result in multi_violations {
        println!("  {}::{}  (line {})", result.file, result.test, result.line);
        for issue in &result.issues {
            if issue.starts_with("MULTI-BEHAVIOR:") {
                println!("    - {issue}");
            }
        }
    }
}

fn print_file_results(file: &Path, results: &[TestResult]) {
    println!("Checking tests in: {}\n", file.display());

    if results.is_empty() {
        println!("No tests found in file");
        return;
    }

    let compliant = results
        .iter()
        .filter(|result| result.is_compliant())
        .count();
    let total = results.len();
    let pct = (compliant as f64 / total as f64) * 100.0;
    println!("Results: {compliant}/{total} compliant ({pct:.1}%)\n");

    for result in results {
        if result.is_compliant() {
            println!("[OK] {} (line {})", result.test, result.line);
        } else {
            println!("[!!] {} (line {})", result.test, result.line);
            for issue in &result.issues {
                println!("    - {issue}");
            }
        }
    }
}
