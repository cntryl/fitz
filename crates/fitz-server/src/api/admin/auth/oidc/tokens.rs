//! Verification of the identity and admin API tokens from the code exchange.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

use super::{fingerprint, Rejected, Settings};
use crate::api::admin::auth::AdminRouteFamilyAccess;

const MAX_TOKEN_BYTES: usize = 16 * 1024;
const MAX_TOKEN_LIFETIME: u64 = 24 * 60 * 60;

#[derive(Deserialize)]
pub(super) struct TokenPair {
    pub id_token: String,
    pub access_token: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    Single(String),
    Multiple(Vec<String>),
}

impl Audience {
    fn values(&self) -> &[String] {
        match self {
            Self::Single(value) => std::slice::from_ref(value),
            Self::Multiple(values) => values,
        }
    }
}

#[derive(Deserialize)]
struct Claims {
    iss: String,
    aud: Audience,
    sub: String,
    iat: u64,
    exp: u64,
    nbf: Option<u64>,
    azp: Option<String>,
    nonce: Option<String>,
    at_hash: Option<String>,
    #[serde(flatten)]
    other: Map<String, Value>,
}

pub(super) struct Verified {
    pub subject: String,
    pub access: AdminRouteFamilyAccess,
    pub expires_at: u64,
}

pub(super) fn verify(
    settings: &Settings,
    key_document: &[u8],
    pair: &TokenPair,
    nonce: &str,
    now: u64,
) -> Result<Verified, Rejected> {
    let keys: JwkSet = serde_json::from_slice(key_document).map_err(|_| Rejected)?;
    if keys.keys.len() > 128 {
        return Err(Rejected);
    }

    let id = claims(settings, &keys, &pair.id_token, &settings.client_id, now)?;
    let api = claims(settings, &keys, &pair.access_token, &settings.audience, now)?;
    if id.aud.values() != [settings.client_id.as_str()]
        || id.nonce.as_deref() != Some(nonce)
        || api
            .aud
            .values()
            .iter()
            .any(|value| value == &settings.client_id || settings.broker_audiences.contains(value))
        || api.sub != id.sub
        || api
            .other
            .get("client_id")
            .is_some_and(|client| client.as_str() != Some(settings.client_id.as_str()))
    {
        return Err(Rejected);
    }
    if let Some(expected) = id.at_hash.as_deref() {
        let actual = URL_SAFE_NO_PAD.encode(&fingerprint(&pair.access_token)[..16]);
        if actual != expected {
            return Err(Rejected);
        }
    }

    let permissions = api
        .other
        .get(&settings.permissions_claim)
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty() && values.len() <= 128)
        .ok_or(Rejected)?;
    let permissions = permissions
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|text| text.len() <= 255)
                .ok_or(Rejected)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Verified {
        subject: id.sub,
        access: grants(&permissions)?,
        expires_at: id.exp.min(api.exp),
    })
}

fn claims(
    settings: &Settings,
    keys: &JwkSet,
    token: &str,
    audience: &str,
    now: u64,
) -> Result<Claims, Rejected> {
    if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
        return Err(Rejected);
    }
    let header = decode_header(token).map_err(|_| Rejected)?;
    let kid = header
        .kid
        .filter(|id| !id.is_empty() && id.len() <= 255)
        .ok_or(Rejected)?;
    if header.alg != Algorithm::RS256 {
        return Err(Rejected);
    }

    let mut matches = keys
        .keys
        .iter()
        .filter(|key| key.common.key_id.as_deref() == Some(kid.as_str()));
    let key = matches.next().ok_or(Rejected)?;
    if matches.next().is_some()
        || !matches!(key.algorithm, AlgorithmParameters::RSA(_))
        || key
            .common
            .key_algorithm
            .as_ref()
            .is_some_and(|alg| *alg != KeyAlgorithm::RS256)
        || key
            .common
            .public_key_use
            .as_ref()
            .is_some_and(|use_| *use_ != PublicKeyUse::Signature)
        || key
            .common
            .key_operations
            .as_ref()
            .is_some_and(|ops| !ops.contains(&KeyOperations::Verify))
    {
        return Err(Rejected);
    }
    let key = DecodingKey::from_jwk(key).map_err(|_| Rejected)?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[settings.issuer.as_str()]);
    validation.set_audience(&[audience]);
    validation.leeway = 0;
    validation.validate_nbf = true;
    validation.required_spec_claims = ["aud", "exp", "iss", "sub"]
        .into_iter()
        .map(str::to_owned)
        .collect();

    let claims = decode::<Claims>(token, &key, &validation)
        .map_err(|_| Rejected)?
        .claims;
    if claims.iss != settings.issuer
        || claims.sub.is_empty()
        || claims.sub.len() > 255
        || claims.sub.chars().any(char::is_control)
        || claims.iat > now
        || claims.exp <= now
        || claims.exp.saturating_sub(claims.iat) > MAX_TOKEN_LIFETIME
        || claims.nbf.is_some_and(|not_before| not_before > now)
        || claims
            .azp
            .as_ref()
            .is_some_and(|client| client != &settings.client_id)
    {
        return Err(Rejected);
    }
    Ok(claims)
}

pub(super) fn grants(permissions: &[&str]) -> Result<AdminRouteFamilyAccess, Rejected> {
    if !permissions.contains(&"fitz:admin") {
        return Err(Rejected);
    }
    let mut families = BTreeSet::new();
    let mut wildcard = false;
    for family in permissions
        .iter()
        .filter_map(|value| value.strip_prefix("route_family:"))
    {
        if family == "*" {
            wildcard = true;
        } else {
            let numeric = family.parse::<u32>().map_err(|_| Rejected)?;
            if numeric == 0 || numeric.to_string() != family {
                return Err(Rejected);
            }
            families.insert(numeric);
        }
    }
    if wildcard {
        return Ok(AdminRouteFamilyAccess::wildcard());
    }
    if families.is_empty() {
        return Err(Rejected);
    }
    Ok(AdminRouteFamilyAccess::Explicit(
        families
            .into_iter()
            .map(|family| family.to_string())
            .collect(),
    ))
}
