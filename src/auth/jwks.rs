use base64::Engine;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const JWKS_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const JWKS_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const JWKS_READ_TIMEOUT: Duration = Duration::from_secs(3);
const JWKS_MAX_BODY_BYTES: usize = 256 * 1024;
const JWKS_FORCED_REFRESH_COOLDOWN_SECS: u64 = 30;

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
    Oct(Vec<u8>),                 // symmetric secret bytes (k)
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
static JWKS_REFRESH_LOCKS: Lazy<DashMap<String, Arc<tokio::sync::Mutex<()>>>> =
    Lazy::new(DashMap::new);
static JWKS_FORCED_REFRESH_AT: Lazy<DashMap<String, u64>> = Lazy::new(DashMap::new);
static JWKS_HTTP_CLIENT: Lazy<Result<reqwest::Client, String>> = Lazy::new(|| {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(JWKS_CONNECT_TIMEOUT)
        .read_timeout(JWKS_READ_TIMEOUT)
        .timeout(JWKS_REQUEST_TIMEOUT)
        .build()
        .map_err(|error| format!("build jwks client error: {error}"))
});

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn is_cache_entry_stale(entry: &CachedJwksEntry, now: u64) -> bool {
    now > entry.fetched_at.saturating_add(entry.ttl_seconds)
}

fn usable_key_from_jwk(jwk: Jwk) -> Result<Option<(String, CachedJwk)>, String> {
    let kid = jwk.kid.unwrap_or_default();
    match jwk.kty.as_str() {
        "oct" => {
            let Some(kval) = jwk.k else {
                return Err("invalid oct secret: missing k".to_string());
            };
            let secret = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(kval)
                .map_err(|e| format!("invalid oct secret: {e}"))?;
            Ok(Some((kid, CachedJwk::Oct(secret))))
        }
        "RSA" | "rsa" => {
            let (Some(n), Some(e)) = (jwk.n, jwk.e) else {
                return Err("invalid RSA key: missing n or e".to_string());
            };
            jsonwebtoken::DecodingKey::from_rsa_components(&n, &e)
                .map_err(|error| format!("invalid RSA key material: {error}"))?;
            Ok(Some((kid, CachedJwk::Rsa { n, e })))
        }
        other => {
            tracing::warn!(kty = other, "Skipping unsupported JWKS key type");
            Ok(None)
        }
    }
}

/// Parse JWKS JSON and insert into the in-memory cache under the supplied `jwks_url` key.
/// This allows tests to inject JWKS without running an HTTP server.
///
/// # Errors
///
/// Returns an error when the provided JWKS JSON cannot be parsed or contains
/// invalid key material for a supported key type.
pub fn cache_jwks_from_json(jwks_url: &str, jwks_json: &str) -> Result<(), String> {
    cache_jwks_from_json_with_ttl(jwks_url, jwks_json, 3600)
}

/// Parse JWKS JSON and cache with explicit TTL (seconds).
///
/// # Errors
///
/// Returns an error when the provided JWKS JSON cannot be parsed or contains
/// invalid key material for a supported key type.
pub fn cache_jwks_from_json_with_ttl(
    jwks_url: &str,
    jwks_json: &str,
    ttl_seconds: u64,
) -> Result<(), String> {
    let jwks: Jwks =
        serde_json::from_str(jwks_json).map_err(|e| format!("jwks json parse error: {e}"))?;

    let mut map: HashMap<String, CachedJwk> = HashMap::new();
    for jwk in jwks.keys {
        if let Some((kid, cached_jwk)) = usable_key_from_jwk(jwk)? {
            map.insert(kid, cached_jwk);
        }
    }

    if map.is_empty() {
        return Err("jwks contains zero usable keys".to_string());
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
        let stale = is_cache_entry_stale(&entry, now);
        drop(entry);
        if stale {
            JWKS_CACHE.remove(jwks_url);
        }
        return stale;
    }
    true
}

/// Try to get a `jsonwebtoken::DecodingKey` for the given `jwks_url` and kid
/// from cache.
pub fn get_decoding_key_from_cache(jwks_url: &str, kid: &str) -> Option<jsonwebtoken::DecodingKey> {
    use jsonwebtoken::DecodingKey;

    let guard = JWKS_CACHE.get(jwks_url)?;
    if is_cache_entry_stale(&guard, now_epoch_secs()) {
        drop(guard);
        JWKS_CACHE.remove(jwks_url);
        return None;
    }
    let m = &guard.keys;

    // If a specific kid requested, try it; otherwise, fall back to the first available key.
    let entry = if kid.is_empty() {
        m.values().next()
    } else {
        m.get(kid).or_else(|| m.get(""))
    }?;

    match entry {
        CachedJwk::Oct(secret) => Some(DecodingKey::from_secret(secret.as_slice())),
        CachedJwk::Rsa { n, e } => {
            // jsonwebtoken supports creating a key from base64url components
            DecodingKey::from_rsa_components(n, e).ok()
        }
    }
}

/// Async fetch JWKS from a well-known JWKS URL and cache the result.
///
/// # Errors
///
/// Returns an error when the URL is invalid, when the HTTP fetch fails, when a
/// redirect or non-success response is returned, or when the body cannot be
/// read or parsed into supported JWKS key material.
async fn fetch_and_cache_jwks_unlocked(jwks_url: &str) -> Result<(), String> {
    super::validate_jwks_url(jwks_url, super::allow_insecure_jwks_http())
        .map_err(|error| format!("invalid JWKS URL: {error}"))?;

    let client = JWKS_HTTP_CLIENT
        .as_ref()
        .map_err(std::clone::Clone::clone)?;

    let mut resp = client
        .get(jwks_url)
        .send()
        .await
        .map_err(|e| format!("fetch jwks error: {e}"))?;

    if resp.status().is_redirection() {
        return Err(format!(
            "fetch jwks error: redirect responses are not allowed: {}",
            resp.status()
        ));
    }
    if !resp.status().is_success() {
        return Err(format!(
            "fetch jwks error: unexpected status {}",
            resp.status()
        ));
    }

    if resp
        .content_length()
        .is_some_and(|length| length > JWKS_MAX_BODY_BYTES as u64)
    {
        return Err(format!(
            "read jwks body error: response exceeds {JWKS_MAX_BODY_BYTES} bytes"
        ));
    }
    let mut body = Vec::with_capacity(
        resp.content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(JWKS_MAX_BODY_BYTES),
    );
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|error| format!("read jwks body error: {error}"))?
    {
        if body.len().saturating_add(chunk.len()) > JWKS_MAX_BODY_BYTES {
            return Err(format!(
                "read jwks body error: response exceeds {JWKS_MAX_BODY_BYTES} bytes"
            ));
        }
        body.extend_from_slice(&chunk);
    }
    let text =
        std::str::from_utf8(&body).map_err(|error| format!("read jwks body error: {error}"))?;

    cache_jwks_from_json_with_ttl(jwks_url, text, 3600)
}

fn refresh_lock(jwks_url: &str) -> Arc<tokio::sync::Mutex<()>> {
    JWKS_REFRESH_LOCKS
        .entry(jwks_url.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn claim_forced_refresh(jwks_url: &str) -> bool {
    let now = now_epoch_secs();
    if JWKS_FORCED_REFRESH_AT
        .get(jwks_url)
        .is_some_and(|last| now < last.saturating_add(JWKS_FORCED_REFRESH_COOLDOWN_SECS))
    {
        return false;
    }
    JWKS_FORCED_REFRESH_AT.insert(jwks_url.to_string(), now);
    true
}

/// Fetches and caches the current key set for an HTTPS JWKS URL.
///
/// # Errors
///
/// Returns an error when the URL is invalid, the request fails or times out,
/// the response is unsuccessful or oversized, or the document is malformed.
pub async fn fetch_and_cache_jwks(jwks_url: &str) -> Result<(), String> {
    let lock = refresh_lock(jwks_url);
    let _guard = lock.lock().await;
    fetch_and_cache_jwks_unlocked(jwks_url).await
}

/// Attempt to ensure JWKS is cached for `jwks_url`; if missing or stale, fetch
/// and cache.
///
/// # Errors
///
/// Returns an error when cache refresh is required and the JWKS fetch or parse
/// path fails.
pub async fn ensure_jwks_cached(jwks_url: &str) -> Result<(), String> {
    if !is_jwks_stale(jwks_url) {
        return Ok(());
    }
    let lock = refresh_lock(jwks_url);
    let _guard = lock.lock().await;
    if !is_jwks_stale(jwks_url) {
        return Ok(());
    }
    fetch_and_cache_jwks_unlocked(jwks_url).await
}

pub(crate) async fn refresh_jwks_for_missing_kid(jwks_url: &str, kid: &str) -> Result<(), String> {
    if get_decoding_key_from_cache(jwks_url, kid).is_some() {
        return Ok(());
    }
    let lock = refresh_lock(jwks_url);
    let _guard = lock.lock().await;
    if get_decoding_key_from_cache(jwks_url, kid).is_some() || !claim_forced_refresh(jwks_url) {
        return Ok(());
    }
    fetch_and_cache_jwks_unlocked(jwks_url).await
}

/// Derive a default JWKS URL from issuer.
///
/// Example: `<https://idp.example>` becomes
/// `<https://idp.example/.well-known/jwks.json>`.
///
/// # Errors
///
/// Returns an error when `iss` is not a valid URL.
pub fn derive_jwks_url_from_issuer(iss: &str) -> Result<String, String> {
    let base = iss.trim_end_matches('/');
    // validate url
    let _ = url::Url::parse(base).map_err(|e| format!("invalid issuer url: {e}"))?;
    Ok(format!("{base}/.well-known/jwks.json"))
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
    fn should_reject_ec_only_jwks_as_zero_usable_keys() {
        // Arrange
        let jwks_json = serde_json::json!({
            "keys": [
                { "kty": "EC", "kid": "ec1", "crv": "P-256", "x": "abc", "y": "def" }
            ]
        })
        .to_string();

        // Act
        let result = cache_jwks_from_json_with_ttl("inline://ec-only", &jwks_json, 1);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("zero usable keys"));
        assert!(is_jwks_stale("inline://ec-only"));
    }

    #[test]
    fn should_cache_mixed_ec_and_oct_jwks_given_one_usable_key() {
        // Arrange
        let secret = b"test_secret".to_vec();
        let k_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&secret);
        let jwks_json = serde_json::json!({
            "keys": [
                { "kty": "EC", "kid": "ec1", "crv": "P-256", "x": "abc", "y": "def" },
                { "kty": "oct", "kid": "oct1", "k": k_b64 }
            ]
        })
        .to_string();

        // Act
        let result = cache_jwks_from_json_with_ttl("inline://mixed-ec-oct", &jwks_json, 1);

        // Assert
        assert!(result.is_ok());
        assert!(get_decoding_key_from_cache("inline://mixed-ec-oct", "oct1").is_some());
    }

    #[test]
    fn should_reject_malformed_oct_key_even_when_unsupported_keys_are_present() {
        // Arrange
        let jwks_json = serde_json::json!({
            "keys": [
                { "kty": "EC", "kid": "ec1", "crv": "P-256", "x": "abc", "y": "def" },
                { "kty": "oct", "kid": "bad-oct", "k": "not valid base64url!" }
            ]
        })
        .to_string();

        // Act
        let result = cache_jwks_from_json_with_ttl("inline://mixed-bad-oct", &jwks_json, 1);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid oct secret"));
        assert!(is_jwks_stale("inline://mixed-bad-oct"));
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
        assert!(!JWKS_CACHE.contains_key("inline://local2"));
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

    #[test]
    fn should_throttle_forced_refreshes_for_same_jwks_url() {
        // Arrange
        let url = "https://idp.example/throttle-test";
        JWKS_FORCED_REFRESH_AT.remove(url);

        // Act
        let first = claim_forced_refresh(url);
        let second = claim_forced_refresh(url);

        // Assert
        assert!(first);
        assert!(!second);
    }
}
