//! Transport-edge JWT verification and JWKS retrieval orchestration.

use crate::auth::{AuthClaimsConfig, AuthConfig, JwksIssuerConfig, RawClaims, VerifiedJwt};

pub(crate) async fn verified_jwt_using_jwks_with_claims_config(
    compact: &str,
    issuer: &JwksIssuerConfig,
    audiences: &[String],
    claims_config: &AuthClaimsConfig,
) -> Result<VerifiedJwt, String> {
    super::jwks::ensure_jwks_cached(&issuer.jwks_url)
        .await
        .map_err(|error| format!("failed to ensure jwks: {error}"))?;
    let header = jsonwebtoken::decode_header(compact)
        .map_err(|error| format!("invalid jwt header: {error}"))?;
    let kid = header.kid.as_deref().unwrap_or("");
    if super::jwks::get_decoding_key_from_cache(&issuer.jwks_url, kid).is_none() {
        super::jwks::refresh_jwks_for_missing_kid(&issuer.jwks_url, kid)
            .await
            .map_err(|error| format!("failed to fetch jwks: {error}"))?;
    }
    let key = super::jwks::get_decoding_key_from_cache(&issuer.jwks_url, kid)
        .ok_or_else(|| "no matching key in jwks".to_string())?;
    let mut validation = jsonwebtoken::Validation::new(header.alg);
    validation.validate_aud = false;
    validation.validate_exp = false;
    validation.validate_nbf = false;
    let token_data = jsonwebtoken::decode::<serde_json::Value>(compact, &key, &validation)
        .map_err(|error| format!("signature verification failed: {error}"))?;
    let raw_claims: RawClaims = serde_json::from_value(token_data.claims)
        .map_err(|error| format!("json parse error: {error}"))?;
    crate::auth::verified_session_claims(
        raw_claims,
        &[issuer.issuer.as_str()],
        audiences,
        claims_config,
    )
}

pub(crate) async fn verified_jwt_with_claims_config(
    compact: &str,
    auth_config: &AuthConfig,
    claims_config: &AuthClaimsConfig,
) -> Result<VerifiedJwt, String> {
    let raw_claims = crate::auth::parse_jwt_noverify(compact)?;
    match auth_config {
        AuthConfig::Disabled => Err("authentication is disabled".to_string()),
        AuthConfig::Hmac(config) => {
            if !raw_claims.iss.trim().is_empty() {
                return Err("issuer-based tokens are not allowed in HMAC mode".to_string());
            }
            let claims_value =
                crate::auth::verify_jwt_with_hmac_secret(compact, config.secret.as_bytes())?;
            let verified_raw: RawClaims = serde_json::from_value(claims_value)
                .map_err(|error| format!("json parse error: {error}"))?;
            crate::auth::verified_session_claims(
                verified_raw,
                &[],
                auth_config.audiences(),
                claims_config,
            )
        }
        AuthConfig::Jwks(_) => {
            let issuer = auth_config
                .find_issuer(&raw_claims.iss)
                .ok_or_else(|| "issuer not allowed".to_string())?;
            verified_jwt_using_jwks_with_claims_config(
                compact,
                issuer,
                auth_config.audiences(),
                claims_config,
            )
            .await
        }
    }
}

#[cfg(test)]
pub(crate) async fn permissions_from_verified_jwt(
    compact: &str,
    auth_config: &AuthConfig,
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    let verified =
        verified_jwt_with_claims_config(compact, auth_config, &AuthClaimsConfig::default()).await?;
    Ok((verified.permissions, verified.claims))
}

#[cfg(test)]
pub(crate) async fn permissions_from_jwt_using_jwks(
    compact: &str,
    issuer: &JwksIssuerConfig,
    audiences: &[String],
) -> Result<
    (
        crate::session::permissions::SessionPermissions,
        crate::auth::Claims,
    ),
    String,
> {
    let verified = verified_jwt_using_jwks_with_claims_config(
        compact,
        issuer,
        audiences,
        &AuthClaimsConfig::default(),
    )
    .await?;
    Ok((verified.permissions, verified.claims))
}
