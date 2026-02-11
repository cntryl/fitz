//! Authentication - JWT token generation for testing

use crate::error::{FitzError, Result};
use jsonwebtoken::{encode, Header, EncodingKey};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
pub struct TestTokenClaims {
    pub sub: String,
    pub realm: String,
    pub scope: Vec<String>,
    pub exp: u64,
}

pub struct TestTokenGenerator {
    secret: String,
}

impl TestTokenGenerator {
    pub fn new(secret: &str) -> Self {
        Self {
            secret: secret.to_string(),
        }
    }

    pub fn generate(&self, realm: &str, user: &str) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| FitzError::AuthFailed("System time error".into()))?
            .as_secs();

        let exp = now + 3600; // 1 hour

        let claims = TestTokenClaims {
            sub: user.to_string(),
            realm: realm.to_string(),
            scope: vec![
                "kv://*//**#*".to_string(),
                "queue://*//**#*".to_string(),
                "rpc://*//**#*".to_string(),
                "notice://*//**#*".to_string(),
                "lease://*//**#*".to_string(),
                "stream://*//**#*".to_string(),
                "schedule://*//**#*".to_string(),
            ],
            exp,
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|e| FitzError::AuthFailed(format!("JWT encode failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_generate_test_token() {
        let gen = TestTokenGenerator::new("test-secret");
        let token = gen.generate("test-realm", "test-user").unwrap();
        assert!(!token.is_empty());
    }

    #[test]
    fn should_create_valid_jwt_structure() {
        let gen = TestTokenGenerator::new("test-secret");
        let token = gen.generate("test-realm", "test-user").unwrap();

        // JWT is 3 parts separated by dots
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }
}
