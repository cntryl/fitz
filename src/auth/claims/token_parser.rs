use base64::Engine;

use super::RawClaims;

/// Parse a compact JWS/JWT and return the deserialized `RawClaims` WITHOUT verifying signature.
///
/// This function only supports compact serialization (header.payload.signature) and
/// base64-url decoding of the payload. Signature verification is done in a later step.
pub fn parse_jwt_noverify(compact: &str) -> Result<RawClaims, String> {
    let parts: Vec<&str> = compact.split('.').collect();
    if parts.len() != 3 {
        return Err("invalid jwt format".to_string());
    }

    let payload = parts[1];
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|e| format!("base64 decode error: {}", e))?;

    let s = String::from_utf8(decoded).map_err(|e| format!("utf8 error: {}", e))?;
    let claims: RawClaims =
        serde_json::from_str(&s).map_err(|e| format!("json parse error: {}", e))?;
    Ok(claims)
}
