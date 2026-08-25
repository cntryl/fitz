use base64::Engine;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::AuthClaimsConfig;

const MAX_DIAGNOSTIC_ARRAY_VALUES: usize = 16;
const MAX_DIAGNOSTIC_VALUE_CHARS: usize = 256;
const TOKEN_FINGERPRINT_HEX_CHARS: usize = 16;

/// A bounded view of one untrusted JWT claim for failure logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwtClaimDiagnostics {
    pub name: String,
    pub value_type: String,
    pub values: Vec<String>,
    pub omitted_values: usize,
    pub values_truncated: bool,
}

/// Safe, bounded details extracted from a rejected JWT.
///
/// This deliberately excludes the compact token, signature, subject, identity
/// values, and unrelated claims. Header and payload data are untrusted and are
/// exposed only as bounded diagnostic values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwtFailureDiagnostics {
    pub token_fingerprint: String,
    pub algorithm: Option<String>,
    pub key_id: Option<String>,
    pub payload_status: String,
    pub issuer: Option<JwtClaimDiagnostics>,
    pub audience: Option<JwtClaimDiagnostics>,
    pub expires_at: Option<JwtClaimDiagnostics>,
    pub not_before: Option<JwtClaimDiagnostics>,
    pub expected_permission_sources: Vec<String>,
    pub presented_permission_sources: Vec<JwtClaimDiagnostics>,
}

/// Extract bounded, non-secret diagnostics from a JWT that failed validation.
///
/// The payload is decoded without signature verification strictly for logging
/// after the normal verification path has rejected the token. Callers must not
/// use this result for authentication or authorization decisions.
#[must_use]
pub fn jwt_failure_diagnostics(
    compact: &str,
    claims_config: &AuthClaimsConfig,
) -> JwtFailureDiagnostics {
    let header = jsonwebtoken::decode_header(compact).ok();
    let algorithm = header.as_ref().map(|header| format!("{:?}", header.alg));
    let key_id = header
        .and_then(|header| header.kid)
        .map(|key_id| bounded_value(&key_id).0);

    let (payload_status, payload) = match decode_payload(compact) {
        Ok(Value::Object(payload)) => ("decoded".to_string(), Some(payload)),
        Ok(_) => ("payload is not a JSON object".to_string(), None),
        Err(status) => (status.to_string(), None),
    };

    let expected_permission_sources = expected_permission_sources(claims_config);
    let presented_permission_sources = payload.as_ref().map_or_else(Vec::new, |payload| {
        presented_permission_sources(payload, claims_config)
    });

    JwtFailureDiagnostics {
        token_fingerprint: token_fingerprint(compact),
        algorithm,
        key_id,
        payload_status,
        issuer: payload
            .as_ref()
            .and_then(|payload| claim_diagnostics(payload, "iss")),
        audience: payload
            .as_ref()
            .and_then(|payload| claim_diagnostics(payload, "aud")),
        expires_at: payload
            .as_ref()
            .and_then(|payload| claim_diagnostics(payload, "exp")),
        not_before: payload
            .as_ref()
            .and_then(|payload| claim_diagnostics(payload, "nbf")),
        expected_permission_sources,
        presented_permission_sources,
    }
}

fn decode_payload(compact: &str) -> Result<Value, &'static str> {
    let mut parts = compact.split('.');
    let Some(_) = parts.next() else {
        return Err("invalid compact JWT format");
    };
    let Some(payload) = parts.next() else {
        return Err("invalid compact JWT format");
    };
    let Some(_) = parts.next() else {
        return Err("invalid compact JWT format");
    };
    if parts.next().is_some() {
        return Err("invalid compact JWT format");
    }

    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .map_err(|_| "JWT payload is not valid base64url")?;
    serde_json::from_slice(&decoded).map_err(|_| "JWT payload is not valid JSON")
}

fn token_fingerprint(compact: &str) -> String {
    let digest = hex::encode(Sha256::digest(compact.as_bytes()));
    digest[..TOKEN_FINGERPRINT_HEX_CHARS].to_string()
}

fn expected_permission_sources(claims_config: &AuthClaimsConfig) -> Vec<String> {
    let mut sources = Vec::new();
    if let Some(custom_claim) = &claims_config.custom_claim {
        push_unique(&mut sources, format!("{custom_claim}.permissions"));
    }
    push_unique(&mut sources, "permissions".to_string());
    if let Some(permissions_claim) = &claims_config.permissions_claim_override {
        push_unique(&mut sources, permissions_claim.clone());
    }
    push_unique(&mut sources, claims_config.role_claim.clone());
    push_unique(&mut sources, "scp".to_string());
    push_unique(&mut sources, "scope".to_string());
    sources
}

fn presented_permission_sources(
    payload: &Map<String, Value>,
    claims_config: &AuthClaimsConfig,
) -> Vec<JwtClaimDiagnostics> {
    let mut diagnostics = Vec::new();

    if let Some(custom_claim) = &claims_config.custom_claim {
        if let Some(value) = payload.get(custom_claim) {
            if let Some(object) = value.as_object() {
                if let Some(permissions) = object.get("permissions") {
                    diagnostics.push(JwtClaimDiagnostics::from_value(
                        format!("{custom_claim}.permissions"),
                        permissions,
                    ));
                } else {
                    diagnostics.push(JwtClaimDiagnostics {
                        name: custom_claim.clone(),
                        value_type: "object without permissions".to_string(),
                        values: Vec::new(),
                        omitted_values: 0,
                        values_truncated: false,
                    });
                }
            } else {
                diagnostics.push(JwtClaimDiagnostics::from_value(custom_claim.clone(), value));
            }
        }
    }

    push_claim_if_present(&mut diagnostics, payload, "permissions");
    if let Some(permissions_claim) = &claims_config.permissions_claim_override {
        push_claim_if_present(&mut diagnostics, payload, permissions_claim);
    }
    push_claim_if_present(&mut diagnostics, payload, &claims_config.role_claim);
    push_claim_if_present(&mut diagnostics, payload, "scp");
    push_claim_if_present(&mut diagnostics, payload, "scope");

    diagnostics
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn push_claim_if_present(
    diagnostics: &mut Vec<JwtClaimDiagnostics>,
    payload: &Map<String, Value>,
    name: &str,
) {
    if diagnostics.iter().any(|existing| existing.name == name) {
        return;
    }
    if let Some(value) = payload.get(name) {
        diagnostics.push(JwtClaimDiagnostics::from_value(name.to_string(), value));
    }
}

fn claim_diagnostics(payload: &Map<String, Value>, name: &str) -> Option<JwtClaimDiagnostics> {
    payload
        .get(name)
        .map(|value| JwtClaimDiagnostics::from_value(name.to_string(), value))
}

impl JwtClaimDiagnostics {
    fn from_value(name: String, value: &Value) -> Self {
        let value_type = json_value_type(value).to_string();
        let (values, omitted_values, values_truncated) = match value {
            Value::Array(values) => {
                let mut truncated = values.len() > MAX_DIAGNOSTIC_ARRAY_VALUES;
                let summaries = values
                    .iter()
                    .take(MAX_DIAGNOSTIC_ARRAY_VALUES)
                    .map(|value| {
                        let (summary, value_truncated) = summarized_value(value);
                        truncated |= value_truncated;
                        summary
                    })
                    .collect();
                (
                    summaries,
                    values.len().saturating_sub(MAX_DIAGNOSTIC_ARRAY_VALUES),
                    truncated,
                )
            }
            Value::Object(_) => (Vec::new(), 0, false),
            _ => {
                let (summary, truncated) = summarized_value(value);
                (vec![summary], 0, truncated)
            }
        };

        Self {
            name,
            value_type,
            values,
            omitted_values,
            values_truncated,
        }
    }
}

fn summarized_value(value: &Value) -> (String, bool) {
    match value {
        Value::String(value) => bounded_value(value),
        Value::Number(value) => (value.to_string(), false),
        Value::Bool(value) => (value.to_string(), false),
        Value::Null => ("null".to_string(), false),
        Value::Array(_) => ("<array>".to_string(), false),
        Value::Object(_) => ("<object>".to_string(), false),
    }
}

fn bounded_value(value: &str) -> (String, bool) {
    let mut characters = value.chars();
    let bounded = characters
        .by_ref()
        .take(MAX_DIAGNOSTIC_VALUE_CHARS)
        .collect::<String>();
    let truncated = characters.next().is_some();
    if truncated {
        (format!("{bounded}..."), true)
    } else {
        (bounded, false)
    }
}

fn json_value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use serde_json::json;

    use super::*;

    #[test]
    fn should_extract_permission_details_without_exposing_raw_jwt_or_subject() {
        // Arrange
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("key-42".to_string());
        let token = jsonwebtoken::encode(
            &header,
            &json!({
                "iss": "https://idp.example/",
                "aud": ["fitz", "other-api"],
                "sub": "private-subject",
                "exp": 9_999_999_999_u64,
                "nbf": 1_700_000_000_u64,
                "permissions": ["notice://prod/orders/**#read", "bad\npermission"],
                "roles": ["queue.write"],
                "scope": "stream.read kv.write"
            }),
            &EncodingKey::from_secret(b"diagnostic-test-secret"),
        )
        .unwrap();

        // Act
        let diagnostics = jwt_failure_diagnostics(&token, &AuthClaimsConfig::default());
        let rendered = format!("{diagnostics:?}");

        // Assert
        assert_eq!(diagnostics.algorithm.as_deref(), Some("HS256"));
        assert_eq!(diagnostics.key_id.as_deref(), Some("key-42"));
        assert_eq!(diagnostics.token_fingerprint.len(), 16);
        assert_eq!(
            diagnostics.expected_permission_sources,
            ["permissions", "roles", "scp", "scope"]
        );
        assert!(diagnostics
            .presented_permission_sources
            .iter()
            .any(|source| source.name == "permissions"
                && source.values == ["notice://prod/orders/**#read", "bad\npermission"]));
        assert!(!rendered.contains(&token));
        assert!(!rendered.contains("private-subject"));
        assert!(!rendered.contains("diagnostic-test-secret"));
    }

    #[test]
    fn should_bound_permission_claim_values_in_failure_diagnostics() {
        // Arrange
        let permission = format!("notice://realm/{}/**#read", "x".repeat(400));
        let permissions = vec![permission; MAX_DIAGNOSTIC_ARRAY_VALUES + 4];
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &json!({
                "iss": "",
                "aud": "fitz",
                "sub": "subject",
                "exp": 9_999_999_999_u64,
                "permissions": permissions
            }),
            &EncodingKey::from_secret(b"diagnostic-test-secret"),
        )
        .unwrap();

        // Act
        let diagnostics = jwt_failure_diagnostics(&token, &AuthClaimsConfig::default());
        let permissions = diagnostics
            .presented_permission_sources
            .iter()
            .find(|source| source.name == "permissions")
            .unwrap();

        // Assert
        assert_eq!(permissions.values.len(), MAX_DIAGNOSTIC_ARRAY_VALUES);
        assert_eq!(permissions.omitted_values, 4);
        assert!(permissions.values_truncated);
        assert!(permissions
            .values
            .iter()
            .all(|value| value.chars().count() <= MAX_DIAGNOSTIC_VALUE_CHARS + 3));
    }

    #[test]
    fn should_report_malformed_configured_custom_permission_claim() {
        // Arrange
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &json!({
                "iss": "",
                "aud": "fitz",
                "sub": "subject",
                "exp": 9_999_999_999_u64,
                "https://fitz.example/claims": {"roles": ["notice.read"]}
            }),
            &EncodingKey::from_secret(b"diagnostic-test-secret"),
        )
        .unwrap();
        let config = AuthClaimsConfig::new(
            "tid",
            Some("https://fitz.example/claims".to_string()),
            "roles",
        );

        // Act
        let diagnostics = jwt_failure_diagnostics(&token, &config);

        // Assert
        assert_eq!(
            diagnostics.expected_permission_sources[0],
            "https://fitz.example/claims.permissions"
        );
        assert!(diagnostics
            .presented_permission_sources
            .iter()
            .any(|source| source.name == "https://fitz.example/claims"
                && source.value_type == "object without permissions"));
    }
}
