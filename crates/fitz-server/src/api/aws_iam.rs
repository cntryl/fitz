//! AWS outbound identity federation for broker CONNECT.

use crate::auth::{AuthConfig, Claims, Permission};
use crate::session::SessionPermissions;
use jsonwebtoken::{Algorithm, Validation};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const TRUST_FILE: &str = "FITZ_AWS_IAM_TRUST_FILE";
const AWS_ISSUER_SUFFIX: &str = ".tokens.sts.global.api.aws";
const MAX_TRUST_BYTES: u64 = 256 * 1024;
const MAX_AWS_TOKEN_BYTES: usize = 32 * 1024;
const MAX_CLOCK_SKEW_SECONDS: u64 = 30;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustFile {
    audience: String,
    accounts: Vec<AccountFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountFile {
    account_id: String,
    issuer: String,
    bindings: Vec<RoleFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleFile {
    role_arn: String,
    route_family: u32,
    realm: Option<String>,
    permissions: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct AwsIamTrust {
    audience: String,
    accounts: HashMap<String, Account>,
}

#[derive(Debug)]
struct Account {
    id: String,
    jwks_url: String,
    roles: HashMap<String, Role>,
}

#[derive(Debug)]
struct Role {
    route_family: u32,
    realm: Option<String>,
    permissions: Vec<Permission>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    List(Vec<String>),
}

impl Audience {
    fn matches(&self, expected: &str) -> bool {
        match self {
            Self::One(audience) => audience == expected,
            Self::List(audiences) => audiences.len() == 1 && audiences[0] == expected,
        }
    }
}

#[derive(Deserialize)]
struct IdentityClaims {
    aws_account: String,
    principal_id: String,
    subject_mode: Option<String>,
}

#[derive(Deserialize)]
struct AwsToken {
    iss: String,
    aud: Audience,
    sub: String,
    iat: u64,
    exp: u64,
    nbf: Option<u64>,
    subject_mode: Option<String>,
    #[serde(rename = "https://sts.amazonaws.com/")]
    aws: IdentityClaims,
}

pub(crate) struct AwsSession {
    pub(crate) route_family: u32,
    pub(crate) claims: Claims,
    pub(crate) permissions: SessionPermissions,
}

pub(crate) fn is_aws_issuer(issuer: &str) -> bool {
    url::Url::parse(issuer)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
        .is_some_and(|host| host.trim_end_matches('.').ends_with(AWS_ISSUER_SUFFIX))
}

fn canonical_issuer(issuer: &str) -> bool {
    let Some(label) = issuer
        .strip_prefix("https://")
        .and_then(|host| host.strip_suffix(AWS_ISSUER_SUFFIX))
    else {
        return false;
    };
    (1..=63).contains(&label.len())
        && !label.starts_with('-')
        && !label.ends_with('-')
        && label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn role_in_account(role: &str, account: &str) -> bool {
    role.strip_prefix("arn:aws:iam::")
        .and_then(|suffix| suffix.split_once(":role/"))
        .is_some_and(|(owner, name)| {
            owner == account
                && !name.is_empty()
                && !name.ends_with('/')
                && name.bytes().all(|byte| byte.is_ascii_graphic())
        })
}

impl Account {
    fn parse(file: AccountFile) -> Result<Self, String> {
        if file.account_id.len() != 12
            || !file.account_id.bytes().all(|byte| byte.is_ascii_digit())
            || !canonical_issuer(&file.issuer)
            || file.bindings.is_empty()
        {
            return Err(
                "AWS IAM account requires an account ID, canonical STS issuer and bindings".into(),
            );
        }
        let mut roles = HashMap::new();
        for binding in file.bindings {
            if !role_in_account(&binding.role_arn, &file.account_id)
                || binding.route_family == 0
                || binding.permissions.is_empty()
            {
                return Err(
                    "AWS IAM binding requires an exact role ARN, route family and permissions"
                        .into(),
                );
            }
            if let Some(realm) = &binding.realm {
                crate::auth::validate_realm_format(realm)
                    .map_err(|_| "AWS IAM binding contains an invalid realm".to_owned())?;
            }
            let permissions = binding
                .permissions
                .iter()
                .map(|permission| Permission::parse(permission))
                .collect::<Result<Vec<_>, _>>()?;
            let role = Role {
                route_family: binding.route_family,
                realm: binding.realm,
                permissions,
            };
            if roles.insert(binding.role_arn, role).is_some() {
                return Err("duplicate AWS IAM role binding".into());
            }
        }
        Ok(Self {
            id: file.account_id,
            jwks_url: super::jwks::derive_jwks_url_from_issuer(&file.issuer)?,
            roles,
        })
    }
}

impl AwsIamTrust {
    pub(crate) fn from_env() -> Result<Option<Arc<Self>>, String> {
        let Some(path) = std::env::var_os(TRUST_FILE) else {
            return Ok(None);
        };
        let reader = std::fs::File::open(path)
            .map_err(|error| format!("cannot open {TRUST_FILE}: {error}"))?;
        let mut bytes = Vec::new();
        reader
            .take(MAX_TRUST_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("cannot read {TRUST_FILE}: {error}"))?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_TRUST_BYTES {
            return Err(format!("{TRUST_FILE} exceeds the size limit"));
        }
        Self::parse(&bytes).map(Arc::new).map(Some)
    }

    fn parse(bytes: &[u8]) -> Result<Self, String> {
        let file: TrustFile =
            serde_json::from_slice(bytes).map_err(|err| format!("invalid {TRUST_FILE}: {err}"))?;
        if file.audience.trim().is_empty() || file.accounts.is_empty() {
            return Err(
                "AWS IAM trust requires a dedicated audience and at least one account".into(),
            );
        }
        let mut accounts = HashMap::new();
        for entry in file.accounts {
            let issuer = entry.issuer.clone();
            if accounts.insert(issuer, Account::parse(entry)?).is_some() {
                return Err("duplicate AWS IAM issuer".into());
            }
        }
        Ok(Self {
            audience: file.audience,
            accounts,
        })
    }

    pub(crate) fn validate_deployment(
        &self,
        auth_required: bool,
        external_tls: bool,
        generic: &AuthConfig,
        families: &[u32],
    ) -> Result<(), String> {
        if !auth_required || !external_tls {
            return Err(
                "AWS IAM CONNECT requires FITZ_AUTH_REQUIRED and FITZ_ASSUME_EXTERNAL_TLS".into(),
            );
        }
        if generic.audiences().contains(&self.audience) {
            return Err(
                "AWS IAM CONNECT audience must be distinct from generic JWT audiences".into(),
            );
        }
        if let AuthConfig::Jwks(config) = generic {
            if config
                .issuers
                .iter()
                .any(|issuer| is_aws_issuer(&issuer.issuer))
            {
                return Err(
                    "configure AWS STS issuers only through FITZ_AWS_IAM_TRUST_FILE".into(),
                );
            }
        }
        if self
            .accounts
            .values()
            .flat_map(|account| account.roles.values())
            .any(|role| !families.contains(&role.route_family))
        {
            return Err("AWS IAM role binds to an unprovisioned route family".into());
        }
        Ok(())
    }

    pub(crate) async fn verify(&self, compact: &str, issuer: &str) -> Result<AwsSession, String> {
        let account = self
            .accounts
            .get(issuer)
            .ok_or("AWS IAM issuer is not trusted")?;
        if compact.len() > MAX_AWS_TOKEN_BYTES {
            return Err("AWS IAM token exceeds the size limit".into());
        }
        let header = jsonwebtoken::decode_header(compact).map_err(|_| "invalid AWS IAM header")?;
        let kid = header
            .kid
            .as_deref()
            .filter(|kid| {
                !kid.is_empty()
                    && kid.len() <= 128
                    && kid.bytes().all(|byte| byte.is_ascii_graphic())
            })
            .ok_or("AWS IAM token has an invalid key ID")?;
        if header.alg != Algorithm::RS256 {
            return Err("AWS IAM CONNECT requires RS256".into());
        }
        let key = super::jwks::exact_issuer_key(&account.jwks_url, kid)
            .await
            .map_err(|_| "AWS IAM signing key is unavailable")?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = false;
        validation.validate_aud = false;
        let token = jsonwebtoken::decode::<AwsToken>(compact, &key, &validation)
            .map_err(|_| "AWS IAM signature or claims are invalid")?
            .claims;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "system clock precedes UNIX epoch")?
            .as_secs();
        self.bind(token, issuer, account, now)
    }

    fn bind(
        &self,
        token: AwsToken,
        issuer: &str,
        account: &Account,
        now: u64,
    ) -> Result<AwsSession, String> {
        if token.iss != issuer
            || !token.aud.matches(&self.audience)
            || token.aws.aws_account != account.id
            || token.sub != token.aws.principal_id
            || [
                token.subject_mode.as_deref(),
                token.aws.subject_mode.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|mode| mode != "AWSIdentity")
            || token.exp <= now
            || token.iat > now.saturating_add(MAX_CLOCK_SKEW_SECONDS)
            || token.nbf.is_some_and(|when| when > now)
            || !(1..=300).contains(&token.exp.saturating_sub(token.iat))
        {
            return Err("AWS IAM identity, audience or time claims are invalid".into());
        }
        let role = account
            .roles
            .get(&token.sub)
            .ok_or("AWS IAM role has no broker binding")?;
        let permissions = SessionPermissions::from_permissions(role.permissions.clone());
        Ok(AwsSession {
            route_family: role.route_family,
            claims: Claims {
                sub: token.sub,
                identity_claim: role.realm.as_ref().map(|_| "realm".to_owned()),
                identity_value: role.realm.clone(),
                permissions: role.permissions.clone(),
                exp: token.exp,
            },
            permissions,
        })
    }
}

#[cfg(test)]
mod tests;
