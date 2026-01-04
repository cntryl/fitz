#[cfg(test)]
mod jwks_discovery_tests {
    use super::super::jwks::*;

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
        let iss2 = "https://idp.example";

        // Act
        let url2 = derive_jwks_url_from_issuer(iss2).unwrap();

        // Assert
        assert_eq!(url2, "https://idp.example/.well-known/jwks.json");
    }

    #[test]
    fn should_cache_jwks_from_json() {
        // Arrange
        let jwks = serde_json::json!({
            "keys": [
                { "kty": "oct", "kid": "k1", "k": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("s") }
            ]
        })
        .to_string();

        // Act
        cache_jwks_from_json_with_ttl("inline://local", &jwks, 1).unwrap();

        // Assert
        assert!(!is_jwks_stale("inline://local"));
    }

    #[test]
    fn should_detect_jwks_staleness_after_ttl() {
        // Arrange
        let jwks = serde_json::json!({
            "keys": [
                { "kty": "oct", "kid": "k1", "k": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("s") }
            ]
        })
        .to_string();
        cache_jwks_from_json_with_ttl("inline://local", &jwks, 1).unwrap();

        // Act: Simulate staleness by manipulating the cache entry
        if let Some(mut entry) = super::super::jwks::JWKS_CACHE.get_mut("inline://local") {
            entry.fetched_at = 0;
        }

        // Assert
        assert!(is_jwks_stale("inline://local"));
    }
}
