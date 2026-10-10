use super::support::{require, sha256, TestResult};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

pub fn fresh_artifact(
    log: &Path,
    package: &str,
    kind: &str,
    source: &Path,
    name: Option<&str>,
) -> TestResult<Value> {
    let artifacts = fs::read_to_string(log)?
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|row| {
            row["reason"] == "compiler-artifact"
                && row["package_id"] == package
                && row["target"]["kind"] == json!([kind])
                && name.is_none_or(|name| row["target"]["name"] == name)
        })
        .collect::<Vec<_>>();
    require(artifacts.len() == 1, "Need the exact compiler artifact")?;
    let artifact = &artifacts[0];
    require(
        artifact["fresh"] == false,
        "Source variant reused a cached compiler artifact",
    )?;
    let actual = Path::new(
        artifact["target"]["src_path"]
            .as_str()
            .ok_or("Missing compiler source")?,
    )
    .canonicalize()?;
    require(
        actual == source.canonicalize()?,
        "Compiler artifact belongs to another source",
    )?;
    Ok(artifact.clone())
}

fn dependency_inputs(line: &str) -> TestResult<Vec<String>> {
    let (_, inputs) = line.split_once(": ").ok_or("Invalid library dep-info")?;
    let mut result = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in inputs.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch.is_whitespace() {
            if !current.is_empty() {
                result.push(std::mem::take(&mut current));
            }
        } else {
            current.push(ch);
        }
    }
    require(!escaped, "Truncated library dep-info escape")?;
    if !current.is_empty() {
        result.push(current);
    }
    Ok(result)
}

pub fn compiled_inputs(source: &Path, log: &Path, package: &str) -> TestResult<Value> {
    let source = source.canonicalize()?;
    let artifact = fresh_artifact(log, package, "lib", &source.join("src/lib.rs"), None)?;
    let filenames = artifact["filenames"]
        .as_array()
        .ok_or("Missing compiler filenames")?;
    let mut libraries: Vec<_> = filenames
        .iter()
        .filter_map(Value::as_str)
        .filter(|name| {
            Path::new(name)
                .extension()
                .is_some_and(|ext| ext == "rmeta")
        })
        .collect();
    if libraries.is_empty() {
        libraries = filenames
            .iter()
            .filter_map(Value::as_str)
            .filter(|name| Path::new(name).extension().is_some_and(|ext| ext == "rlib"))
            .collect();
    }
    require(libraries.len() == 1, "Need one library artifact")?;
    let library = PathBuf::from(libraries[0]);
    let name = library
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("Invalid library name")?;
    let stripped = name
        .strip_prefix("lib")
        .ok_or("Invalid library artifact prefix")?;
    let candidates: BTreeSet<_> = [
        library.with_extension("d"),
        library.with_file_name(stripped).with_extension("d"),
    ]
    .into_iter()
    .filter(|path| path.is_file())
    .collect();
    require(candidates.len() == 1, "Need unambiguous library dep-info")?;
    let dep_info = fs::read_to_string(candidates.first().ok_or("Missing dep-info")?)?;
    let mut hashes = serde_json::Map::new();
    for input in dependency_inputs(dep_info.lines().next().ok_or("Empty dep-info")?)? {
        let path = source.join(input).canonicalize()?;
        if let Ok(relative) = path.strip_prefix(&source) {
            if relative.starts_with("src") {
                hashes.insert(
                    relative.to_string_lossy().into_owned(),
                    json!(sha256(&path)?),
                );
            }
        }
    }
    require(
        hashes.contains_key("src/lib.rs"),
        "Missing compiler-recorded library source inputs",
    )?;
    Ok(json!({"hashes":hashes,"dep_info":dep_info,"compiler_artifact":artifact}))
}

pub fn validate_inputs(kind: &str, provenance: &Value) -> TestResult {
    let before = &provenance["before"];
    let after = &provenance["after"];
    require(
        before["fixtures"]
            .as_object()
            .is_some_and(|fixtures| !fixtures.is_empty())
            && before["fixtures"] == after["fixtures"],
        "Benchmark fixtures differ",
    )?;
    if kind == "dependencies" {
        require(
            before["manifest_sha256"] != after["manifest_sha256"]
                || before["lock_sha256"] != after["lock_sha256"],
            "Dependency comparison normalized production inputs",
        )
    } else {
        require(kind == "source", "Unknown Stream comparison kind")?;
        require(
            before["lock_sha256"] == after["lock_sha256"]
                && before["dependencies"] == after["dependencies"],
            "Source comparison dependencies differ",
        )
    }
}

pub fn binary_equivalence(kind: &str, provenance: &Value, hashes: &Value) -> TestResult<bool> {
    validate_inputs(kind, provenance)?;
    let targets: BTreeSet<_> = super::stream::groups()
        .into_iter()
        .map(|(target, _)| target)
        .collect();
    require(
        hashes.as_object().is_some_and(|rows| {
            rows.len() == 2 && rows.contains_key("before") && rows.contains_key("after")
        }),
        "Missing build hashes",
    )?;
    for phase in ["before", "after"] {
        let rows = hashes[phase]
            .as_object()
            .ok_or("Missing benchmark hashes")?;
        require(
            rows.keys().cloned().collect::<BTreeSet<_>>() == targets
                && rows
                    .values()
                    .all(|hash| hash.as_str().is_some_and(|hash| !hash.is_empty())),
            "Missing benchmark artifact hash",
        )?;
        require(
            provenance[phase]["compiled_source_inputs"]
                .get("src/lib.rs")
                .is_some(),
            "Missing compiled library inputs",
        )?;
    }
    if kind == "dependencies"
        && provenance["before"]["compiled_source_inputs"]
            == provenance["after"]["compiled_source_inputs"]
    {
        return Ok(hashes["before"] == hashes["after"]);
    }
    for target in targets {
        require(
            hashes["before"][&target] != hashes["after"][&target],
            "Source variants reused the same binary",
        )?;
    }
    Ok(false)
}

#[path = "compiler_tests.rs"]
mod tests;
