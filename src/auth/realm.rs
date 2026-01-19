//! Realm validation utility for Fitz multi-tenancy enforcement
//!
//! This module provides the core realm validation logic used by all domains.
//!
//! **Invariants enforced:**
//! - Realm is opaque (no normalization, prefix matching, or inference)
//! - Realm comparison is strict equality only
//! - Empty realms are rejected
//! - Realm format (basic validation) is checked

use std::fmt;

/// Realm validation error
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RealmError {
    /// Realm is empty string
    EmptyRealm,
    /// Realm contains invalid characters (whitespace, control chars)
    MalformedRealm(String),
    /// Realm does not match the authorized realm for this session
    UnauthorizedRealm { claimed: String, authorized: String },
    /// Realm was not resolved (no default_realm in token and no explicit realm provided)
    RealmNotResolved,
}

impl fmt::Display for RealmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RealmError::EmptyRealm => write!(f, "Realm cannot be empty"),
            RealmError::MalformedRealm(_) => {
                write!(f, "Invalid realm format (opaque identifiers only)")
            }
            RealmError::UnauthorizedRealm { .. } => {
                write!(f, "Realm mismatch")
            }
            RealmError::RealmNotResolved => write!(f, "Realm must be explicitly provided or derived from token"),
        }
    }
}

/// Validates realm format and returns the validated realm
///
/// **Rules:**
/// - Realm must not be empty
/// - Realm must be treated as opaque (UUID-like identifier)
/// - No whitespace allowed
/// - No control characters allowed
///
/// **Returns:** `Ok(realm)` if valid, `Err(RealmError)` otherwise
pub fn validate_realm_format(realm: &str) -> Result<&str, RealmError> {
    // Empty realm
    if realm.is_empty() {
        return Err(RealmError::EmptyRealm);
    }

    // Check for whitespace or control characters
    if realm.chars().any(|c| c.is_whitespace()) {
        return Err(RealmError::MalformedRealm(realm.to_string()));
    }

    if realm.chars().any(|c| c.is_control()) {
        return Err(RealmError::MalformedRealm(realm.to_string()));
    }

    // Opaque identifier check: reject very short strings that aren't valid UUIDs
    // But we allow any alphanumeric + hyphen + underscore for flexibility
    if !realm
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(RealmError::MalformedRealm(realm.to_string()));
    }

    Ok(realm)
}

/// Enforces strict realm equality (case-sensitive, no normalization)
///
/// **Invariant:** Realm comparison MUST be strict string equality.
/// No case folding, no prefix matching, no fuzzy matching.
pub fn realm_matches(claimed: &str, authorized: &str) -> bool {
    claimed == authorized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_valid_uuid_realms() {
        assert!(validate_realm_format("550e8400-e29b-41d4-a716-446655440000").is_ok());
    }

    #[test]
    fn should_accept_alphanumeric_realms() {
        assert!(validate_realm_format("acme").is_ok());
        assert!(validate_realm_format("realm-123").is_ok());
        assert!(validate_realm_format("realm_456").is_ok());
    }

    #[test]
    fn should_reject_empty_realm() {
        assert_eq!(validate_realm_format(""), Err(RealmError::EmptyRealm));
    }

    #[test]
    fn should_reject_whitespace() {
        assert!(validate_realm_format("realm ").is_err());
        assert!(validate_realm_format(" realm").is_err());
        assert!(validate_realm_format("realm\t123").is_err());
    }

    #[test]
    fn should_reject_special_chars() {
        assert!(validate_realm_format("realm!").is_err());
        assert!(validate_realm_format("realm@123").is_err());
        assert!(validate_realm_format("realm#").is_err());
    }

    #[test]
    fn should_enforce_strict_equality() {
        assert!(realm_matches("acme", "acme"));
        assert!(!realm_matches("acme", "ACME")); // Case-sensitive
        assert!(!realm_matches("acme", "acme1")); // No prefix match
        assert!(!realm_matches("acm", "acme")); // No fuzzy match
    }
}
