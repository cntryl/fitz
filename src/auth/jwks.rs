use once_cell::sync::Lazy;
use dashmap::DashMap;
use serde::Deserialize;
use base64::Engine;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// JWKS caching layer.
///
/// **Strictly:** In-memory cache only. No HTTP, no I/O, no domain logic.
/// - In-memory cache with TTL
/// - Key lookup helpers
/// - Test injection support
///
/// HTTP fetch is done in the *transport layer* only (see `fetch_and_cache_jwks`/`ensure_jwks_cached`).
/// Minimal JWK representation sufficient for our needs
#[derive(Debug, Deserialize)]
struct Jwk {
    pub kty: String,
    pub kid: Option<String>,
    pub _alg: Option<String>,
    #[serde(rename = "use")]
    pub _use_: Option<String>,
    pub n: Option<String>,
    pub e: Option<String>,
    pub k: Option<String>,
    pub _x5c: Option<Vec<String>>,
} 

#[derive(Debug, Deserialize)]
struct Jwks {
    pub keys: Vec<Jwk>,
}

/// Lightweight cache representation for a JWK
#[derive(Debug, Clone)]
enum CachedJwk {
    Oct(Vec<u8>),            // symmetric secret bytes (k)
    Rsa { n: String, e: String }, // RSA components (base64url strings)
}

/// Cached JWKS entry with timestamp + TTL
#[derive(Debug, Clone)]
struct CachedJwksEntry {
    pub keys: HashMap<String, CachedJwk>,
    pub fetched_at: u64, // epoch seconds
    pub ttl_seconds: u64,
}

static JWKS_CACHE: Lazy<DashMap<String, CachedJwksEntry>> = Lazy::new(DashMap::new);

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Parse JWKS JSON and insert into the in-memory cache under the supplied `jwks_url` key.
/// This allows tests to inject JWKS without running an HTTP server.
pub fn cache_jwks_from_json(jwks_url: &str, jwks_json: &str) -> Result<(), String> {
    cache_jwks_from_json_with_ttl(jwks_url, jwks_json, 3600)
}

/// Parse JWKS JSON and cache with explicit TTL (seconds)
pub fn cache_jwks_from_json_with_ttl(jwks_url: &str, jwks_json: &str, ttl_seconds: u64) -> Result<(), String> {
    let jwks: Jwks = serde_json::from_str(jwks_json).map_err(|e| format!("jwks json parse error: {}", e))?;

    let mut map: HashMap<String, CachedJwk> = HashMap::new();
    for k in jwks.keys.into_iter() {
        let kid = k.kid.clone().unwrap_or_else(|| "".to_string());
        match k.kty.as_str() {
            "oct" => {
                if let Some(kval) = k.k {
                    // base64url decode (no pad)
                    let secret = base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .decode(kval)
                        .map_err(|e| format!("invalid oct secret: {}", e))?;
                    map.insert(kid, CachedJwk::Oct(secret));
                }
            }
            "RSA" | "rsa" => {
                if let (Some(n), Some(e)) = (k.n.clone(), k.e.clone()) {
                    map.insert(kid, CachedJwk::Rsa { n, e });
                }
            }
            other => {
                // Skip unsupported types for now
                tracing::debug!("skipping unsupported jwk kty={}", other);
            }
        }
    }

    let entry = CachedJwksEntry {
        keys: map,
        fetched_at: now_epoch_secs(),
        ttl_seconds,
    };

    JWKS_CACHE.insert(jwks_url.to_string(), entry);
    Ok(())
}

/// Check whether a JWKS entry is stale or missing
pub fn is_jwks_stale(jwks_url: &str) -> bool {
    if let Some(entry) = JWKS_CACHE.get(jwks_url) {
        let now = now_epoch_secs();
        return now > entry.fetched_at + entry.ttl_seconds;
    }
    true
}

/// Try to get a `jsonwebtoken::DecodingKey` for the given jwks_url and kid from cache.
pub fn get_decoding_key_from_cache(jwks_url: &str, kid: &str) -> Option<jsonwebtoken::DecodingKey> {
    use jsonwebtoken::DecodingKey;

    let guard = JWKS_CACHE.get(jwks_url)?;
    let m = &guard.keys;

    // If a specific kid requested, try it; otherwise, fall back to the first available key.
    let entry = if !kid.is_empty() {
        m.get(kid).or_else(|| m.get(""))
    } else {
        m.values().next()
    }?;

    match entry {
        CachedJwk::Oct(secret) => Some(DecodingKey::from_secret(secret.as_slice())),
        CachedJwk::Rsa { n, e } => {
            // jsonwebtoken supports creating a key from base64url components
            DecodingKey::from_rsa_components(n, e).ok()
        }
    }
}

/// Async fetch JWKS from a well-known jwks URL and cache the result.
pub async fn fetch_and_cache_jwks(jwks_url: &str) -> Result<(), String> {
    // Simple HTTP fetch, cache with default TTL of 1 hour
    let resp = reqwest::get(jwks_url)
        .await
        .map_err(|e| format!("fetch jwks error: {}", e))?;

    let text = resp
        .text()
        .await
        .map_err(|e| format!("read jwks body error: {}", e))?;

    cache_jwks_from_json_with_ttl(jwks_url, &text, 3600)
}

/// Attempt to ensure JWKS is cached for `jwks_url`; if missing or stale, fetch and cache.
pub async fn ensure_jwks_cached(jwks_url: &str) -> Result<(), String> {
    if is_jwks_stale(jwks_url) {
        fetch_and_cache_jwks(jwks_url).await
    } else {
        Ok(())
    }
}

/// Derive a default JWKS URL from issuer (e.g. "https://idp.example" -> "https://idp.example/.well-known/jwks.json")
pub fn derive_jwks_url_from_issuer(iss: &str) -> Result<String, String> {
    let base = iss.trim_end_matches('/');
    // validate url
    let _ = url::Url::parse(base).map_err(|e| format!("invalid issuer url: {}", e))?;
    Ok(format!("{}/.well-known/jwks.json", base))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_cache_jwks_from_json() {
        // Arrange
        let secret = b"test_secret".to_vec();
        let k_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&secret);
        let jwks_json = serde_json::json!({
            "keys": [
                { "kty": "oct", "kid": "k1", "k": k_b64 }
            ]
        })
        .to_string();

        // Act
        cache_jwks_from_json_with_ttl("inline://local", &jwks_json, 1).unwrap();

        // Assert
        assert!(!is_jwks_stale("inline://local"));
    }

    #[test]
    fn should_detect_jwks_staleness_after_ttl() {
        // Arrange
        let secret = b"test_secret".to_vec();
        let k_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&secret);
        let jwks_json = serde_json::json!({
            "keys": [
                { "kty": "oct", "kid": "k1", "k": k_b64 }
            ]
        })
        .to_string();
        cache_jwks_from_json_with_ttl("inline://local2", &jwks_json, 1).unwrap();

        // Act: Simulate staleness by manipulating the cache entry
        if let Some(mut entry) = JWKS_CACHE.get_mut("inline://local2") {
            entry.fetched_at = 0;
        }

        // Assert
        assert!(is_jwks_stale("inline://local2"));
    }

    #[test]
    fn should_derive_jwks_url_with_trailing_slash() {
        // Arrange
        let iss = "https://idp.example/";

        // Act
        let url = derive_jwks_url_from_issuer(iss).unwrap();

        // Assert
        assert_eq!(url, "https://idp.example/.well-known/jwks.json");
    }

    #[test]
    fn should_derive_jwks_url_without_trailing_slash() {
        // Arrange
        let iss = "https://idp.example";

        // Act
        let url = derive_jwks_url_from_issuer(iss).unwrap();

        // Assert
        assert_eq!(url, "https://idp.example/.well-known/jwks.json");
    }
}

