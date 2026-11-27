//! Fixed, bounded HTTPS exchanges with the configured `OpenID Connect` provider.

use std::time::Duration;

use serde::Deserialize;
use url::Url;

use super::{tokens::TokenPair, AudienceParameter, Rejected, Settings};

const MAX_BODY: usize = 64 * 1024;

#[derive(Deserialize)]
struct Discovery {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
    token_endpoint_auth_methods_supported: Option<Vec<String>>,
}

pub(super) struct Provider {
    pub authorization: Url,
    token: Url,
    pub keys: Url,
    auth: ClientAuth,
}

#[derive(Clone, Copy)]
pub(super) enum ClientAuth {
    Post,
    Basic,
}

pub(super) fn client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
}

pub(super) fn https_url(raw: &str) -> Result<Url, Rejected> {
    if raw.len() > 4096
        || raw
            .chars()
            .any(|value| value.is_control() || value.is_whitespace())
        || raw.contains('\\')
    {
        return Err(Rejected);
    }
    let parsed = Url::parse(raw).map_err(|_| Rejected)?;
    if parsed.scheme() != "https"
        || parsed.host().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(Rejected);
    }
    Ok(parsed)
}

pub(super) async fn discover(
    http: &reqwest::Client,
    settings: &Settings,
) -> Result<Provider, Rejected> {
    let body = bytes(http.get(settings.discovery.clone())).await?;
    let discovery: Discovery = serde_json::from_slice(&body).map_err(|_| Rejected)?;
    if discovery.issuer != settings.issuer {
        return Err(Rejected);
    }
    let authorization = https_url(&discovery.authorization_endpoint)?;
    if authorization.query().is_some() {
        return Err(Rejected);
    }
    let methods = discovery.token_endpoint_auth_methods_supported.as_deref();
    let supports = |method: &str| {
        methods.map_or(method == "client_secret_basic", |values| {
            values.iter().any(|value| value == method)
        })
    };
    let auth = settings.client_auth.unwrap_or_else(|| {
        if supports("client_secret_post") {
            ClientAuth::Post
        } else {
            ClientAuth::Basic
        }
    });
    let method = match auth {
        ClientAuth::Post => "client_secret_post",
        ClientAuth::Basic => "client_secret_basic",
    };
    if !supports(method) {
        return Err(Rejected);
    }
    Ok(Provider {
        authorization,
        token: https_url(&discovery.token_endpoint)?,
        keys: https_url(&discovery.jwks_uri)?,
        auth,
    })
}

pub(super) async fn exchange(
    http: &reqwest::Client,
    settings: &Settings,
    provider: &Provider,
    code: &str,
    verifier: &str,
) -> Result<TokenPair, Rejected> {
    let mut request = http.post(provider.token.clone());
    let body = {
        let mut form = url::form_urlencoded::Serializer::new(String::new());
        form.extend_pairs([
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", settings.redirect.as_str()),
            ("code_verifier", verifier),
        ]);
        if matches!(settings.audience_parameter, AudienceParameter::Resource) {
            form.append_pair("resource", &settings.audience);
        }
        match provider.auth {
            ClientAuth::Post => {
                form.append_pair("client_id", &settings.client_id);
                form.append_pair("client_secret", &settings.client_secret);
            }
            ClientAuth::Basic => {
                let url_encode = |value: &str| {
                    url::form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>()
                };
                request = request.basic_auth(
                    url_encode(&settings.client_id),
                    Some(url_encode(&settings.client_secret)),
                );
            }
        }
        form.finish()
    };
    let body = bytes(
        request
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body),
    )
    .await?;
    serde_json::from_slice(&body).map_err(|_| Rejected)
}

pub(super) async fn get_keys(http: &reqwest::Client, endpoint: Url) -> Result<Vec<u8>, Rejected> {
    bytes(http.get(endpoint)).await
}

async fn bytes(request: reqwest::RequestBuilder) -> Result<Vec<u8>, Rejected> {
    let mut response = request.send().await.map_err(|_| Rejected)?;
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|len| len > MAX_BODY as u64)
    {
        return Err(Rejected);
    }
    let mut output = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Rejected)? {
        if chunk.len() > MAX_BODY - output.len() {
            return Err(Rejected);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}
