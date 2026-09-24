//! Golden-fixture assertions for wire and persistence contracts.
//!
//! Fixtures live in `tests/fixtures/golden/<name>.txt`. They pin bytes that
//! clients or on-disk data depend on, so a mismatch is a contract break, not a
//! stale fixture. Never regenerate a fixture to make a refactor pass.

use std::fmt::Write as _;
use std::path::PathBuf;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/golden")
        .join(format!("{name}.txt"))
}

/// Assert `actual` matches the committed golden fixture `name`.
///
/// # Panics
///
/// Panics when the fixture is missing or differs from `actual`.
pub fn assert_golden(name: &str, actual: &str) {
    let path = fixture_path(name);
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("missing golden fixture {}: {error}", path.display()));
    assert!(
        expected.trim_end() == actual.trim_end(),
        "golden contract `{name}` changed ({}).\n--- expected\n{expected}\n--- actual\n{actual}",
        path.display()
    );
}

/// Render bytes as lowercase hex for golden fixtures.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Replace the numeric value of `"key":` in compact JSON with `<n>`.
///
/// Used to pin payload layout when a field (such as a wall-clock timestamp)
/// is not deterministic.
///
/// # Panics
///
/// Panics when `key` is absent.
#[must_use]
pub fn mask_json_number(json: &str, key: &str) -> String {
    let marker = format!("\"{key}\":");
    let (head, tail) = json
        .split_once(&marker)
        .unwrap_or_else(|| panic!("missing JSON key {key} in {json}"));
    let rest = tail.trim_start_matches(|c: char| c.is_ascii_digit());
    format!("{head}{marker}<n>{rest}")
}

/// Replace the string value of `"key":"..."` in compact JSON with `<s>`,
/// leaving the JSON unchanged when `key` is absent.
#[must_use]
pub fn mask_json_string(json: &str, key: &str) -> String {
    let marker = format!("\"{key}\":\"");
    let Some((head, tail)) = json.split_once(&marker) else {
        return json.to_string();
    };
    let rest = tail.split_once('"').map_or("", |(_, rest)| rest);
    format!("{head}{marker}<s>\"{rest}")
}
