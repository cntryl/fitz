//! Cloud workload identity verification for broker CONNECT.

use crate::auth::{AuthConfig, Claims, Permission};
use crate::session::SessionPermissions;
use jsonwebtoken::{Algorithm, Validation};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

mod environment;

const TRUST_FILE: &str = "FITZ_WORKLOAD_IDENTITY_TRUST_FILE";
const AWS_TRUST_FILE: &str = "FITZ_AWS_IAM_TRUST_FILE";
const GOOGLE_ISSUER: &str = "https://accounts.google.com";
const GOOGLE_KEYS: &str = "https://www.googleapis.com/oauth2/v3/certs";
const AWS_ISSUER_SUFFIX: &str = ".tokens.sts.global.api.aws";
const MAX_TRUST_BYTES: u64 = 256 * 1024;
const MAX_WORKLOAD_TOKEN_BYTES: usize = 32 * 1024;
const MAX_CLOCK_SKEW_SECONDS: u64 = 30;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustFile {
    audience: Option<String>,
    #[serde(default)]
    accounts: Vec<AccountFile>,
    #[serde(default)]
    azure: Vec<AzureFile>,
    google: Option<GoogleFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AzureFile {
    tenant_id: String,
    token_version: AzureVersion,
    audience: String,
    bindings: Vec<RoleFile>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum AzureVersion {
    #[serde(rename = "1.0")]
    V1,
    #[serde(rename = "2.0")]
    V2,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoogleFile {
    audience: String,
    bindings: Vec<RoleFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountFile {
    account_id: String,
    issuer: String,
    #[serde(default = "default_aws_algorithms")]
    algorithms: Vec<Algorithm>,
    bindings: Vec<RoleFile>,
}

fn default_aws_algorithms() -> Vec<Algorithm> {
    vec![Algorithm::RS256]
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleFile {
    #[serde(alias = "role_arn")]
    subject: String,
    client_id: Option<String>,
    route_family: u32,
    realm: Option<String>,
    permissions: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct WorkloadIdentityTrust {
    issuers: HashMap<String, IssuerTrust>,
    audiences: HashSet<String>,
}

#[derive(Debug)]
struct IssuerTrust {
    provider: Provider,
    audience: String,
    jwks_url: String,
    roles: HashMap<String, Role>,
}

#[derive(Debug)]
enum Provider {
    Aws {
        account_id: String,
        algorithms: Vec<Algorithm>,
    },
    Azure {
        tenant_id: String,
        version: AzureVersion,
    },
    Google,
}

#[derive(Debug)]
struct Role {
    client_id: Option<String>,
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
struct WorkloadToken {
    iss: String,
    aud: Audience,
    sub: String,
    iat: u64,
    exp: u64,
    nbf: Option<u64>,
    subject_mode: Option<String>,
    #[serde(rename = "https://sts.amazonaws.com/")]
    aws: Option<IdentityClaims>,
    tid: Option<String>,
    oid: Option<String>,
    idtyp: Option<String>,
    scp: Option<String>,
    ver: Option<String>,
    appid: Option<String>,
    azp: Option<String>,
}

pub(crate) struct WorkloadSession {
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

fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.hyphenated().to_string() == value)
}

fn google_subject(value: &str) -> bool {
    (1..=32).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_digit())
}

impl AzureVersion {
    fn issuer(self, tenant: &str) -> String {
        match self {
            Self::V1 => format!("https://sts.windows.net/{tenant}/"),
            Self::V2 => format!("https://login.microsoftonline.com/{tenant}/v2.0"),
        }
    }

    fn keys(self, tenant: &str) -> String {
        match self {
            Self::V1 => format!("https://login.microsoftonline.com/{tenant}/discovery/keys"),
            Self::V2 => format!("https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys"),
        }
    }
}

impl Provider {
    fn validate_binding(&self, binding: &RoleFile) -> bool {
        match self {
            Self::Aws { account_id, .. } => {
                binding.client_id.is_none() && role_in_account(&binding.subject, account_id)
            }
            Self::Azure { .. } => {
                canonical_uuid(&binding.subject)
                    && binding.client_id.as_deref().is_some_and(canonical_uuid)
            }
            Self::Google => binding.client_id.is_none() && google_subject(&binding.subject),
        }
    }

    fn identity<'a>(&self, token: &'a WorkloadToken) -> Option<(&'a str, Option<&'a str>)> {
        match self {
            Self::Aws { account_id, .. } => {
                let aws = token.aws.as_ref()?;
                if aws.aws_account != *account_id
                    || token.sub != aws.principal_id
                    || !role_in_account(&token.sub, account_id)
                    || [token.subject_mode.as_deref(), aws.subject_mode.as_deref()]
                        .into_iter()
                        .flatten()
                        .any(|mode| mode != "AWSIdentity")
                {
                    return None;
                }
                Some((&token.sub, None))
            }
            Self::Azure { tenant_id, version } => {
                if token.idtyp.as_deref() != Some("app")
                    || token.scp.is_some()
                    || token.tid.as_deref() != Some(tenant_id)
                {
                    return None;
                }
                let client_id = match (version, token.ver.as_deref()) {
                    (AzureVersion::V1, Some("1.0")) => token.appid.as_deref()?,
                    (AzureVersion::V2, Some("2.0")) => token.azp.as_deref()?,
                    _ => return None,
                };
                let subject = token.oid.as_deref()?;
                (canonical_uuid(subject) && canonical_uuid(client_id))
                    .then_some((subject, Some(client_id)))
            }
            Self::Google if token.azp.as_deref() == Some(token.sub.as_str()) => {
                Some((&token.sub, None))
            }
            Self::Google => None,
        }
    }

    fn permits_algorithm(&self, algorithm: Algorithm) -> bool {
        match self {
            Self::Aws { algorithms, .. } => algorithms.contains(&algorithm),
            Self::Azure { .. } | Self::Google => algorithm == Algorithm::RS256,
        }
    }

    fn key_id<'a>(&self, header: &'a jsonwebtoken::Header) -> Option<&'a str> {
        header.kid.as_deref().or({
            if matches!(
                self,
                Self::Azure {
                    version: AzureVersion::V1,
                    ..
                }
            ) {
                header.x5t.as_deref()
            } else {
                None
            }
        })
    }

    fn max_duration(&self) -> u64 {
        match self {
            Self::Aws { .. } => 300,
            Self::Azure { .. } => 86_400,
            Self::Google => 3_600,
        }
    }
}

impl IssuerTrust {
    fn parse(
        provider: Provider,
        audience: String,
        jwks_url: String,
        bindings: Vec<RoleFile>,
        allow_all: bool,
    ) -> Result<Self, String> {
        if audience.trim().is_empty() || bindings.is_empty() {
            return Err("workload identity trust requires an audience and bindings".into());
        }
        let mut roles = HashMap::new();
        for binding in bindings {
            let scoped_wildcard =
                allow_all && binding.subject == "*" && !matches!(provider, Provider::Google);
            if (!scoped_wildcard && !provider.validate_binding(&binding))
                || binding.route_family == 0
                || binding.permissions.is_empty()
            {
                return Err("invalid workload subject, route family, or permissions".into());
            }
            if let Some(realm) = &binding.realm {
                crate::auth::validate_realm_format(realm).map_err(|_| {
                    "workload identity binding contains an invalid realm".to_owned()
                })?;
            }
            let permissions = binding
                .permissions
                .iter()
                .map(|permission| Permission::parse(permission))
                .collect::<Result<Vec<_>, _>>()?;
            let role = Role {
                client_id: binding.client_id,
                route_family: binding.route_family,
                realm: binding.realm,
                permissions,
            };
            if roles.insert(binding.subject, role).is_some() {
                return Err("duplicate workload identity binding".into());
            }
        }
        Ok(Self {
            provider,
            audience,
            jwks_url,
            roles,
        })
    }
}

impl WorkloadIdentityTrust {
    pub(crate) fn from_env() -> Result<Option<Arc<Self>>, String> {
        let path = match (
            std::env::var_os(TRUST_FILE),
            std::env::var_os(AWS_TRUST_FILE),
        ) {
            (Some(_), Some(_)) => {
                return Err(format!("set only one of {TRUST_FILE} and {AWS_TRUST_FILE}"))
            }
            (Some(path), None) | (None, Some(path)) => Some(path),
            (None, None) => None,
        };
        let flat = environment::read(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(format!("{name}: {error}")),
        })?;
        if path.is_some() && flat.is_some() {
            return Err(
                "choose a workload trust file or flat workload environment variables".into(),
            );
        }
        let Some(path) = path else {
            return flat
                .map(|file| Self::from_document(file, true).map(Arc::new))
                .transpose();
        };
        let reader = std::fs::File::open(path)
            .map_err(|error| format!("cannot open workload identity trust file: {error}"))?;
        let mut bytes = Vec::new();
        reader
            .take(MAX_TRUST_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("cannot read workload identity trust file: {error}"))?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_TRUST_BYTES {
            return Err("workload identity trust file exceeds the size limit".into());
        }
        Self::parse(&bytes).map(Arc::new).map(Some)
    }

    fn parse(bytes: &[u8]) -> Result<Self, String> {
        let file: TrustFile = serde_json::from_slice(bytes)
            .map_err(|err| format!("invalid workload identity trust file: {err}"))?;
        Self::from_document(file, false)
    }

    fn from_document(file: TrustFile, allow_all: bool) -> Result<Self, String> {
        let mut issuers = HashMap::new();
        let mut insert = |issuer, trust| {
            if issuers.insert(issuer, trust).is_some() {
                return Err("duplicate workload identity issuer".to_owned());
            }
            Ok(())
        };
        for account in file.accounts {
            if account.account_id.len() != 12
                || !account.account_id.bytes().all(|byte| byte.is_ascii_digit())
                || !canonical_issuer(&account.issuer)
                || account.algorithms.is_empty()
                || account
                    .algorithms
                    .iter()
                    .any(|algorithm| !matches!(algorithm, Algorithm::RS256 | Algorithm::ES384))
            {
                return Err(
                    "AWS IAM account requires an account ID and canonical STS issuer".into(),
                );
            }
            let keys = super::jwks::derive_jwks_url_from_issuer(&account.issuer)?;
            let audience = file
                .audience
                .clone()
                .ok_or("missing AWS workload audience")?;
            insert(
                account.issuer,
                IssuerTrust::parse(
                    Provider::Aws {
                        account_id: account.account_id,
                        algorithms: account.algorithms,
                    },
                    audience,
                    keys,
                    account.bindings,
                    allow_all,
                )?,
            )?;
        }
        for azure in file.azure {
            if !canonical_uuid(&azure.tenant_id)
                || (matches!(azure.token_version, AzureVersion::V2)
                    && !canonical_uuid(&azure.audience))
            {
                return Err("Azure workload identity requires a tenant GUID and a version-appropriate audience".into());
            }
            let issuer = azure.token_version.issuer(&azure.tenant_id);
            let keys = azure.token_version.keys(&azure.tenant_id);
            insert(
                issuer,
                IssuerTrust::parse(
                    Provider::Azure {
                        tenant_id: azure.tenant_id,
                        version: azure.token_version,
                    },
                    azure.audience,
                    keys,
                    azure.bindings,
                    allow_all,
                )?,
            )?;
        }
        if let Some(google) = file.google {
            insert(
                GOOGLE_ISSUER.into(),
                IssuerTrust::parse(
                    Provider::Google,
                    google.audience,
                    GOOGLE_KEYS.into(),
                    google.bindings,
                    allow_all,
                )?,
            )?;
        }
        if issuers.is_empty() {
            return Err("at least one workload identity issuer must be configured".into());
        }
        let audiences = issuers
            .values()
            .map(|trust| trust.audience.clone())
            .collect();
        Ok(Self { issuers, audiences })
    }

    pub(crate) fn selects(&self, candidate: &crate::auth::RawClaims) -> bool {
        let matches = |audience: &str| self.audiences.contains(audience);
        match &candidate.aud {
            crate::auth::AudienceClaim::String(value) => matches(value),
            crate::auth::AudienceClaim::Array(values) => values.iter().any(|value| matches(value)),
        }
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
                "workload CONNECT requires FITZ_AUTH_REQUIRED and FITZ_ASSUME_EXTERNAL_TLS".into(),
            );
        }
        if self
            .issuers
            .values()
            .any(|issuer| generic.audiences().contains(&issuer.audience))
        {
            return Err(
                "workload CONNECT audiences must be distinct from generic JWT audiences".into(),
            );
        }
        if let AuthConfig::Jwks(config) = generic {
            if config
                .issuers
                .iter()
                .any(|issuer| is_aws_issuer(&issuer.issuer))
            {
                return Err(
                    "configure AWS STS issuers only through the workload identity trust file"
                        .into(),
                );
            }
        }
        if self
            .issuers
            .values()
            .flat_map(|issuer| issuer.roles.values())
            .any(|role| !families.contains(&role.route_family))
        {
            return Err("workload identity binds to an unprovisioned route family".into());
        }
        Ok(())
    }

    pub(crate) async fn verify(
        &self,
        compact: &str,
        issuer: &str,
    ) -> Result<WorkloadSession, String> {
        let trust = self
            .issuers
            .get(issuer)
            .ok_or("workload issuer is not trusted")?;
        if compact.len() > MAX_WORKLOAD_TOKEN_BYTES {
            return Err("workload token exceeds the size limit".into());
        }
        let header =
            jsonwebtoken::decode_header(compact).map_err(|_| "invalid workload token header")?;
        let kid = trust
            .provider
            .key_id(&header)
            .filter(|kid| {
                !kid.is_empty()
                    && kid.len() <= 128
                    && kid.bytes().all(|byte| byte.is_ascii_graphic())
            })
            .ok_or("workload token has an invalid key ID")?;
        if !trust.provider.permits_algorithm(header.alg) {
            return Err("workload signing algorithm is not allowed".into());
        }
        let key = super::jwks::exact_issuer_key(&trust.jwks_url, kid)
            .await
            .map_err(|_| "workload signing key is unavailable")?;
        let mut validation = Validation::new(header.alg);
        validation.validate_exp = false;
        validation.validate_aud = false;
        let token = jsonwebtoken::decode::<WorkloadToken>(compact, &key, &validation)
            .map_err(|_| "workload signature or claims are invalid")?
            .claims;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "system clock precedes UNIX epoch")?
            .as_secs();
        Self::bind(&token, issuer, trust, now)
    }

    fn bind(
        token: &WorkloadToken,
        issuer: &str,
        trust: &IssuerTrust,
        now: u64,
    ) -> Result<WorkloadSession, String> {
        if token.iss != issuer
            || !token.aud.matches(&trust.audience)
            || token.exp <= now
            || token.iat > now.saturating_add(MAX_CLOCK_SKEW_SECONDS)
            || token.nbf.is_some_and(|when| when > now)
            || !(1..=trust.provider.max_duration()).contains(&token.exp.saturating_sub(token.iat))
        {
            return Err("workload audience, issuer, or time claims are invalid".into());
        }
        let (subject, client_id) = trust
            .provider
            .identity(token)
            .ok_or("workload identity claims are invalid")?;
        let role = trust
            .roles
            .get(subject)
            .filter(|role| role.client_id.as_deref() == client_id)
            .or_else(|| trust.roles.get("*"))
            .ok_or("workload identity has no broker binding")?;
        let permissions = SessionPermissions::from_permissions(role.permissions.clone());
        Ok(WorkloadSession {
            route_family: role.route_family,
            claims: Claims {
                sub: subject.to_owned(),
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
