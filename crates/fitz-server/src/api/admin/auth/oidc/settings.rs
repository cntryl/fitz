use url::Url;

use super::{provider, Rejected, CALLBACK_PATH, SESSION_TTL};

const ENV: [&str; 8] = [
    "FITZ_ADMIN_OIDC_ISSUER",
    "FITZ_ADMIN_OIDC_CLIENT_ID",
    "FITZ_ADMIN_OIDC_CLIENT_SECRET",
    "FITZ_ADMIN_OIDC_API_AUDIENCE",
    "FITZ_ADMIN_OIDC_PERMISSIONS_CLAIM",
    "FITZ_ADMIN_OIDC_AUDIENCE_PARAMETER",
    "FITZ_ADMIN_OIDC_CLIENT_AUTH_METHOD",
    "FITZ_ADMIN_OIDC_SCOPES",
];

pub(super) const PUBLIC_ORIGIN_ENV: &str = "FITZ_ADMIN_PUBLIC_ORIGIN";

pub(super) struct Settings {
    pub issuer: String,
    pub discovery: Url,
    pub client_id: String,
    pub client_secret: String,
    pub audience: String,
    pub broker_audiences: Vec<String>,
    pub redirect: Url,
    pub permissions_claim: String,
    pub audience_parameter: AudienceParameter,
    pub client_auth: Option<provider::ClientAuth>,
    pub authorization_scope: String,
    pub session_ttl: u64,
    pub origin: crate::api::origin::ExactOrigin,
}

#[derive(Clone, Copy)]
pub(super) enum AudienceParameter {
    Audience,
    Resource,
    None,
}

impl AudienceParameter {
    pub(super) fn key(self) -> Option<&'static str> {
        match self {
            Self::Audience => Some("audience"),
            Self::Resource => Some("resource"),
            Self::None => None,
        }
    }
}

impl Settings {
    pub(super) fn from_env() -> Result<Option<Self>, String> {
        Self::from_lookup(|name| {
            std::env::var_os(name).map(|value| value.to_string_lossy().into_owned())
        })
    }

    pub(super) fn from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<Self>, String> {
        if !ENV.iter().any(|name| lookup(name).is_some()) {
            return Ok(None);
        }
        let required = |name| {
            lookup(name)
                .filter(|value| {
                    !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
                })
                .ok_or_else(|| format!("{name} is required for admin OIDC"))
        };
        if lookup("FITZ_ADMIN_AUTH_MODE").is_some_and(|value| value.eq_ignore_ascii_case("open")) {
            return Err("Admin OIDC requires FITZ_ADMIN_AUTH_MODE=protected".into());
        }
        if lookup("FITZ_ADMIN_COOKIE_SECURE").is_some_and(|value| value.trim() != "true") {
            return Err("Admin OIDC requires secure admin cookies".into());
        }

        let issuer = required("FITZ_ADMIN_OIDC_ISSUER")?;
        let issuer_url = provider::https_url(&issuer)
            .map_err(|_| "FITZ_ADMIN_OIDC_ISSUER must be an HTTPS issuer URL")?;
        if issuer_url.query().is_some() {
            return Err("FITZ_ADMIN_OIDC_ISSUER must have no query".into());
        }
        let discovery = provider::https_url(&format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        ))
        .map_err(|_| "Invalid FITZ_ADMIN_OIDC_ISSUER")?;

        let public_origin = required(PUBLIC_ORIGIN_ENV)?;
        let origin = crate::api::origin::parse_exact_origin(&public_origin)
            .map_err(|_| "FITZ_ADMIN_PUBLIC_ORIGIN must be an exact HTTPS origin")?;
        if origin.scheme() != "https" {
            return Err("FITZ_ADMIN_PUBLIC_ORIGIN must use HTTPS for admin OIDC".into());
        }
        let redirect = provider::https_url(&format!(
            "{}{CALLBACK_PATH}",
            public_origin.trim_end_matches('/')
        ))
        .map_err(|_| "Invalid admin OIDC callback origin")?;

        let client_id = required("FITZ_ADMIN_OIDC_CLIENT_ID")?;
        let client_secret = required("FITZ_ADMIN_OIDC_CLIENT_SECRET")?;
        let audience = required("FITZ_ADMIN_OIDC_API_AUDIENCE")?;
        if audience == client_id {
            return Err("FITZ_ADMIN_OIDC_API_AUDIENCE must differ from the OIDC client ID".into());
        }
        let broker_audiences: Vec<String> = lookup("FITZ_JWT_AUDIENCES")
            .or_else(|| lookup("FITZ_JWT_AUDIENCE"))
            .as_deref()
            .unwrap_or(crate::auth::DEFAULT_BROKER_AUDIENCES)
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect();
        if broker_audiences.contains(&audience) {
            return Err("FITZ_ADMIN_OIDC_API_AUDIENCE must differ from broker audiences".into());
        }
        let audience_parameter = match lookup("FITZ_ADMIN_OIDC_AUDIENCE_PARAMETER").as_deref() {
            None | Some("audience") => AudienceParameter::Audience,
            Some("resource") => AudienceParameter::Resource,
            Some("none") => AudienceParameter::None,
            _ => return Err("Invalid FITZ_ADMIN_OIDC_AUDIENCE_PARAMETER".into()),
        };
        let client_auth = match lookup("FITZ_ADMIN_OIDC_CLIENT_AUTH_METHOD").as_deref() {
            None => None,
            Some("client_secret_post") => Some(provider::ClientAuth::Post),
            Some("client_secret_basic") => Some(provider::ClientAuth::Basic),
            _ => return Err("Invalid FITZ_ADMIN_OIDC_CLIENT_AUTH_METHOD".into()),
        };
        let permissions_claim = match lookup("FITZ_ADMIN_OIDC_PERMISSIONS_CLAIM") {
            None => "permissions".to_owned(),
            Some(_) => required("FITZ_ADMIN_OIDC_PERMISSIONS_CLAIM")?,
        };
        let authorization_scope = scopes(
            lookup("FITZ_ADMIN_OIDC_SCOPES")
                .as_deref()
                .unwrap_or_default(),
        )
        .map_err(|_| "Invalid FITZ_ADMIN_OIDC_SCOPES")?;
        let session_ttl = lookup("FITZ_ADMIN_SESSION_TTL_SECS")
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map_or(SESSION_TTL, |value| value.min(SESSION_TTL));
        Ok(Some(Self {
            issuer,
            discovery,
            client_id,
            client_secret,
            audience,
            broker_audiences,
            redirect,
            permissions_claim,
            audience_parameter,
            client_auth,
            authorization_scope,
            session_ttl,
            origin,
        }))
    }
}

fn scopes(value: &str) -> Result<String, Rejected> {
    if value.len() > 1024 {
        return Err(Rejected);
    }
    let mut scopes = vec!["openid"];
    for token in value.split(' ').filter(|token| !token.is_empty()) {
        if token.len() > 128
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\'))
        {
            return Err(Rejected);
        }
        if !scopes.contains(&token) {
            if scopes.len() == 33 {
                return Err(Rejected);
            }
            scopes.push(token);
        }
    }
    Ok(scopes.join(" "))
}
