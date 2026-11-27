//! Browser sessions established by the configured `OpenID Connect` provider.

mod provider;
mod settings;
mod tokens;

#[cfg(test)]
mod tests;

use super::{AdminAuth, AdminPrincipal};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use settings::{AudienceParameter, Settings};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use url::{Position, Url};

pub(super) const CALLBACK_PATH: &str = "/api/v1/session/oidc/callback";
pub(super) const BROWSER_COOKIE: &str = "__Host-fitz_oidc";
pub(super) const SESSION_COOKIE: &str = "__Host-fitz_admin_oidc_session";
pub(super) const CHALLENGE_TTL: u64 = 300;
const SESSION_TTL: u64 = 8 * 60 * 60;
const MAX_CHALLENGES: usize = 512;
const MAX_SESSIONS: usize = 1_024;

#[derive(Debug)]
pub(super) struct Rejected;

pub(super) fn validate_config() -> Result<bool, String> {
    Settings::from_env().map(|settings| settings.is_some())
}

pub(super) struct Oidc {
    settings: Settings,
    http: reqwest::Client,
    store: parking_lot::Mutex<Store>,
    outbound: Semaphore,
}

impl std::fmt::Debug for Oidc {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Oidc").finish_non_exhaustive()
    }
}

pub(super) struct LoginStart {
    pub location: Url,
    pub browser: String,
}

pub(super) struct LoginComplete {
    pub location: String,
    pub token: String,
    pub max_age: u64,
}

#[derive(Clone)]
struct Session {
    principal: AdminPrincipal,
    expires_at: u64,
}

impl Oidc {
    pub(super) fn from_env() -> Result<Option<Self>, String> {
        let Some(settings) = Settings::from_env()? else {
            return Ok(None);
        };
        let http = provider::client_builder()
            .build()
            .map_err(|_| "Unable to configure the admin OIDC HTTP client")?;
        Ok(Some(Self::new(settings, http)))
    }

    fn new(settings: Settings, http: reqwest::Client) -> Self {
        Self {
            settings,
            http,
            store: parking_lot::Mutex::new(Store::default()),
            outbound: Semaphore::new(16),
        }
    }

    pub(super) fn origin(&self) -> &crate::api::origin::ExactOrigin {
        &self.settings.origin
    }

    pub(super) async fn start(&self, next: &str) -> Result<LoginStart, Rejected> {
        self.store.lock().require_challenge_slot(now()?)?;
        let _permit = self.outbound.try_acquire().map_err(|_| Rejected)?;
        let discovered = provider::discover(&self.http, &self.settings).await?;
        let next = safe_return(&self.settings.redirect, next);
        let (state, browser, nonce, verifier) = self.store.lock().begin(&next, now()?)?;
        let mut location = discovered.authorization;
        let challenge = URL_SAFE_NO_PAD.encode(fingerprint(&verifier));
        location.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("response_mode", "query"),
            ("client_id", self.settings.client_id.as_str()),
            ("redirect_uri", self.settings.redirect.as_str()),
            ("scope", self.settings.authorization_scope.as_str()),
            ("state", &state),
            ("nonce", &nonce),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
        ]);
        if let Some(key) = self.settings.audience_parameter.key() {
            location
                .query_pairs_mut()
                .append_pair(key, &self.settings.audience);
        }
        Ok(LoginStart { location, browser })
    }

    pub(super) async fn complete(
        &self,
        admin: &AdminAuth,
        state: &str,
        browser: &str,
        code: &str,
    ) -> Result<LoginComplete, Rejected> {
        let pending = self.store.lock().take(state, browser, now()?)?;
        if code.is_empty() || code.len() > 4096 {
            return Err(Rejected);
        }
        let _permit = self.outbound.try_acquire().map_err(|_| Rejected)?;
        let discovered = provider::discover(&self.http, &self.settings).await?;
        let pair = provider::exchange(
            &self.http,
            &self.settings,
            &discovered,
            code,
            &pending.verifier,
        )
        .await?;
        let keys = provider::get_keys(&self.http, discovered.keys).await?;
        let verified = tokens::verify(&self.settings, &keys, &pair, &pending.nonce, now()?)?;
        if !admin.valid_route_family_grants(&verified.access) {
            return Err(Rejected);
        }

        let issued_at = now()?;
        let expires_at = verified
            .expires_at
            .min(issued_at.saturating_add(self.settings.session_ttl));
        let max_age = expires_at
            .checked_sub(issued_at)
            .filter(|age| *age > 0)
            .ok_or(Rejected)?;
        let principal = AdminPrincipal {
            username: digest_subject(&self.settings.issuer, &verified.subject),
            route_family_access: verified.access,
        };
        let token = self.store.lock().issue(
            Session {
                principal,
                expires_at,
            },
            issued_at,
        )?;
        Ok(LoginComplete {
            location: pending.next,
            token,
            max_age,
        })
    }

    pub(super) fn principal(&self, token: &str) -> Option<AdminPrincipal> {
        self.store.lock().principal(token, now().ok()?)
    }

    pub(super) fn revoke(&self, token: &str) {
        self.store.lock().sessions.remove(&fingerprint(token));
    }
}

fn now() -> Result<u64, Rejected> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .map_err(|_| Rejected)
}

fn random() -> Result<String, Rejected> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(|_| Rejected)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn fingerprint(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

fn digest_subject(issuer: &str, subject: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(issuer.as_bytes());
    hash.update([0]);
    hash.update(subject.as_bytes());
    format!("oidc:{}", URL_SAFE_NO_PAD.encode(hash.finalize()))
}

fn safe_return(base: &Url, path: &str) -> String {
    if !path.starts_with('/')
        || path.starts_with("//")
        || path.contains('\\')
        || path.len() > 2048
        || !path.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return "/".to_owned();
    }
    let Ok(target) = base.join(path) else {
        return "/".to_owned();
    };
    let Ok(route) = percent_encoding::percent_decode_str(target.path()).decode_utf8() else {
        return "/".to_owned();
    };
    if target.origin() != base.origin()
        || route.starts_with("//")
        || route.contains('\\')
        || route.chars().any(char::is_control)
        || matches!(route.trim_end_matches('/'), "/login" | "/logout" | "/api")
        || route.starts_with("/api/")
    {
        return "/".to_owned();
    }
    target[Position::BeforePath..].to_owned()
}

#[derive(Default)]
struct Store {
    challenges: HashMap<[u8; 32], Challenge>,
    sessions: HashMap<[u8; 32], Session>,
}

struct Challenge {
    browser: [u8; 32],
    nonce: String,
    verifier: String,
    next: String,
    expires_at: u64,
}

impl Store {
    fn begin(
        &mut self,
        next: &str,
        now: u64,
    ) -> Result<(String, String, String, String), Rejected> {
        self.require_challenge_slot(now)?;
        let [state, browser, nonce, verifier] = [random()?, random()?, random()?, random()?];
        self.challenges.insert(
            fingerprint(&state),
            Challenge {
                browser: fingerprint(&browser),
                nonce: nonce.clone(),
                verifier: verifier.clone(),
                next: next.to_owned(),
                expires_at: now.saturating_add(CHALLENGE_TTL),
            },
        );
        Ok((state, browser, nonce, verifier))
    }

    fn require_challenge_slot(&mut self, now: u64) -> Result<(), Rejected> {
        if self.challenges.len() >= MAX_CHALLENGES {
            self.challenges
                .retain(|_, challenge| challenge.expires_at > now);
        }
        (self.challenges.len() < MAX_CHALLENGES)
            .then_some(())
            .ok_or(Rejected)
    }

    fn take(&mut self, state: &str, browser: &str, now: u64) -> Result<Challenge, Rejected> {
        if state.len() != 43 || browser.len() != 43 {
            return Err(Rejected);
        }
        let key = fingerprint(state);
        let browser = fingerprint(browser);
        if self
            .challenges
            .get(&key)
            .is_none_or(|challenge| challenge.expires_at <= now || challenge.browser != browser)
        {
            return Err(Rejected);
        }
        self.challenges.remove(&key).ok_or(Rejected)
    }

    fn issue(&mut self, session: Session, now: u64) -> Result<String, Rejected> {
        self.sessions.retain(|_, session| session.expires_at > now);
        if self.sessions.len() >= MAX_SESSIONS {
            return Err(Rejected);
        }
        let token = random()?;
        self.sessions.insert(fingerprint(&token), session);
        Ok(token)
    }

    fn principal(&mut self, token: &str, now: u64) -> Option<AdminPrincipal> {
        if token.len() != 43 {
            return None;
        }
        let key = fingerprint(token);
        let session = self.sessions.get(&key)?;
        if session.expires_at <= now {
            self.sessions.remove(&key);
            return None;
        }
        Some(session.principal.clone())
    }
}
