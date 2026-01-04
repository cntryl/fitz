use jsonwebtoken::{Algorithm, decode, decode_header, DecodingKey, Validation};
use serde_json::Value;

/// Verify the JWT signature using the provided RSA public key (PEM) and return the decoded payload as JSON.
pub fn verify_jwt_with_rsa_pem(token: &str, public_pem: &[u8]) -> Result<Value, String> {
    // Determine algorithm from header
    let header = decode_header(token).map_err(|e| format!("invalid jwt header: {}", e))?;
    let alg = header.alg;

    let mut validation = Validation::new(alg);
    // For now, allow default settings. In future we may configure audience/issuer.

    let decoding_key = DecodingKey::from_rsa_pem(public_pem)
        .map_err(|e| format!("invalid public key: {}", e))?;

    let token_data = decode::<serde_json::Value>(token, &decoding_key, &validation)
        .map_err(|e| format!("signature verification failed: {}", e))?;

    Ok(token_data.claims)
}

/// Verify the JWT signature using an HMAC secret (HS256) and return the decoded payload as JSON.
pub fn verify_jwt_with_hmac_secret(token: &str, secret: &[u8]) -> Result<Value, String> {
    let header = decode_header(token).map_err(|e| format!("invalid jwt header: {}", e))?;
    let alg = header.alg;

    if alg != Algorithm::HS256 {
        return Err("unsupported HMAC algorithm; expected HS256".to_string());
    }

    let decoding_key = DecodingKey::from_secret(secret);
    let mut validation = Validation::new(Algorithm::HS256);

    let token_data = decode::<serde_json::Value>(token, &decoding_key, &validation)
        .map_err(|e| format!("signature verification failed: {}", e))?;

    Ok(token_data.claims)
}
