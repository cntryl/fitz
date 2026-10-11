use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
pub type Environment = BTreeMap<String, String>;

pub fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

pub fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn variable(name: &str) -> TestResult<String> {
    Ok(std::env::var(name)?)
}

pub fn save(path: &Path, value: &Value) -> TestResult {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

pub fn load(path: &Path) -> TestResult<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

pub fn sha256(path: &Path) -> TestResult<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 8192];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex::encode(digest.finalize()))
}

pub fn output(source: &Path, env: &Environment, command: &[&str]) -> TestResult<Vec<u8>> {
    let result = Command::new(command[0])
        .args(&command[1..])
        .current_dir(source)
        .envs(env)
        .output()?;
    require(
        result.status.success(),
        &format!(
            "Command failed: {}: {}",
            command.join(" "),
            String::from_utf8_lossy(&result.stderr)
        ),
    )?;
    Ok(result.stdout)
}

pub fn text(source: &Path, env: &Environment, command: &[&str]) -> TestResult<String> {
    Ok(String::from_utf8(output(source, env, command)?)?
        .trim()
        .to_owned())
}

pub fn execute(source: &Path, env: &Environment, command: &[&str]) -> TestResult {
    let status = Command::new(command[0])
        .args(&command[1..])
        .current_dir(source)
        .envs(env)
        .status()?;
    require(
        status.success(),
        &format!("Command failed: {}", command.join(" ")),
    )
}

pub fn recorded(
    source: &Path,
    env: &Environment,
    command: &[String],
    log: &Path,
) -> TestResult<i32> {
    let file = File::create(log)?;
    let status = Command::new(&command[0])
        .args(&command[1..])
        .current_dir(source)
        .envs(env)
        .stdout(Stdio::from(file.try_clone()?))
        .stderr(Stdio::from(file))
        .status()?;
    Ok(status.code().unwrap_or(-1))
}

pub fn files(directory: &Path, extension: &str) -> TestResult<Vec<PathBuf>> {
    let mut result = Vec::new();
    if !directory.exists() {
        return Ok(result);
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            result.extend(files(&path, extension)?);
        } else if path.extension().is_some_and(|ext| ext == extension) {
            result.push(path);
        }
    }
    result.sort();
    Ok(result)
}

pub fn source_hashes(source: &Path, directory: &str) -> TestResult<Value> {
    let mut hashes = serde_json::Map::new();
    for path in files(&source.join(directory), "rs")? {
        hashes.insert(
            path.strip_prefix(source)?.to_string_lossy().into_owned(),
            json!(sha256(&path)?),
        );
    }
    Ok(Value::Object(hashes))
}

pub fn host(paths: &[&str]) -> Value {
    paths
        .iter()
        .map(|path| {
            (
                (*path).to_owned(),
                fs::read_to_string(path).map_or(Value::Null, Value::String),
            )
        })
        .collect()
}

pub fn summary(body: &str) -> TestResult {
    File::options()
        .create(true)
        .append(true)
        .open(variable("GITHUB_STEP_SUMMARY")?)?
        .write_all(body.as_bytes())?;
    Ok(())
}

pub fn dependencies(metadata: &Value) -> TestResult<Value> {
    let mut result = metadata["packages"]
        .as_array()
        .ok_or("Missing Cargo packages")?
        .iter()
        .filter(|package| package["name"] != "fitz")
        .map(|package| json!([package["name"], package["version"], package["source"]]))
        .collect::<Vec<_>>();
    result.sort_by_key(Value::to_string);
    Ok(json!(result))
}

pub fn number(value: &Value) -> TestResult<f64> {
    let number = value.as_f64().ok_or("Missing numeric measurement")?;
    require(number.is_finite(), "Non-finite measurement")?;
    Ok(number)
}

pub fn positive(value: &Value) -> TestResult<f64> {
    let number = number(value)?;
    require(number > 0.0, "Expected positive measurement")?;
    Ok(number)
}
