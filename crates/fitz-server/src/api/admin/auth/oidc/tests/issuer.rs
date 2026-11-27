use super::super::{
    fingerprint, now, provider, random, AdminAuth, Oidc, Settings, BROWSER_COOKIE, CALLBACK_PATH,
    SESSION_COOKIE, URL_SAFE_NO_PAD,
};
use crate::api::http::{Body, Response};
use crate::boot::Runtime;
use crate::runtime::Router;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::rsa::KeySize;
use aws_lc_rs::signature::{KeyPair, RsaKeyPair, RSA_PKCS1_SHA256};
use base64::Engine;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{
    header::{COOKIE, LOCATION, ORIGIN, SET_COOKIE},
    server::conn::http1,
    service::service_fn,
    Method, Request, StatusCode,
};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer};
use tokio_rustls::TlsAcceptor;
use url::{form_urlencoded, Url};

const ORIGIN_URL: &str = "https://admin.fitz.test";
const ADMIN_API: &str = "https://fitz-admin-api.fitz.test";
const CLIENT_ID: &str = "fitz-console";
const KEY_ID: &str = "fitz-fixture-signer";

struct TestIssuer {
    base: String,
    secret: String,
    authorization_code: String,
    signing_key: RsaKeyPair,
    keys: Value,
    flow: parking_lot::Mutex<Option<Authorization>>,
}

struct Authorization {
    nonce: String,
    challenge: String,
    audiences: Vec<&'static str>,
    api_permissions: Vec<&'static str>,
}

impl TestIssuer {
    fn new(base: String, secret: String) -> Self {
        let signing_key = RsaKeyPair::generate(KeySize::Rsa2048).expect("signing key");
        let public = signing_key.public_key();
        let keys = json!({ "keys": [{
            "kid": KEY_ID, "alg": "RS256", "kty": "RSA", "use": "sig",
            "n": URL_SAFE_NO_PAD.encode(public.modulus().big_endian_without_leading_zero()),
            "e": URL_SAFE_NO_PAD.encode(public.exponent().big_endian_without_leading_zero()),
        }] });
        Self {
            base,
            secret,
            authorization_code: random().expect("authorization code"),
            signing_key,
            keys,
            flow: parking_lot::Mutex::new(None),
        }
    }

    async fn respond(&self, request: Request<hyper::body::Incoming>) -> Response {
        match request.uri().path() {
            "/.well-known/openid-configuration" => crate::api::admin::json_response(json!({
                "issuer": self.base,
                "authorization_endpoint": format!("{}authorize", self.base),
                "token_endpoint": format!("{}token", self.base),
                "jwks_uri": format!("{}keys", self.base),
                "token_endpoint_auth_methods_supported": ["client_secret_post"],
            })),
            "/keys" => crate::api::admin::json_response(&self.keys),
            "/token" => {
                assert_eq!(request.method(), Method::POST);
                let payload = request.into_body().collect().await.unwrap().to_bytes();
                let form: HashMap<_, _> = form_urlencoded::parse(&payload).into_owned().collect();
                let flow = self.flow.lock();
                let authorization = flow.as_ref().expect("browser authorization query");
                assert_eq!(form["code"], self.authorization_code);
                assert_eq!(form["client_id"], CLIENT_ID);
                assert_eq!(form["client_secret"], self.secret);
                assert_eq!(form["redirect_uri"], format!("{ORIGIN_URL}{CALLBACK_PATH}"));
                assert_eq!(
                    URL_SAFE_NO_PAD.encode(fingerprint(&form["code_verifier"])),
                    authorization.challenge
                );
                crate::api::admin::json_response(self.tokens(authorization))
            }
            _ => crate::api::admin::not_found(),
        }
    }

    fn tokens(&self, flow: &Authorization) -> Value {
        let issued_at = now().expect("test issuance time");
        let access = sign(
            &self.signing_key,
            &json!({
                "iss": self.base,
                "aud": flow.audiences,
                "sub": "operator",
                "iat": issued_at,
                "exp": issued_at + 300,
                "azp": CLIENT_ID,
                "permissions": flow.api_permissions,
            }),
        );
        let identity = sign(
            &self.signing_key,
            &json!({
                "iss": self.base,
                "aud": CLIENT_ID,
                "sub": "operator",
                "iat": issued_at,
                "exp": issued_at + 600,
                "nonce": flow.nonce,
                "permissions": ["fitz:admin", "route_family:*"],
                "at_hash": URL_SAFE_NO_PAD.encode(&fingerprint(&access)[..16]),
            }),
        );
        json!({ "access_token": access, "id_token": identity, "token_type": "Bearer" })
    }
}

fn sign(key: &RsaKeyPair, claims: &Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&json!({ "alg": "RS256", "kid": KEY_ID })).expect("token header"),
    );
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("token claims"));
    let message = format!("{header}.{payload}");
    let mut signature = vec![0; key.public_key().modulus_len()];
    key.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        message.as_bytes(),
        &mut signature,
    )
    .expect("sign test token");
    format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature))
}

struct Fixture {
    runtime: Arc<Runtime>,
    issuer: Arc<TestIssuer>,
    task: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Fixture {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("issuer listener");
        let address = listener.local_addr().expect("bound issuer address");
        let base = format!("https://localhost:{}/", address.port());
        let secret = random().expect("fixture client secret");
        let settings = Settings::from_lookup(|name| match name {
            "FITZ_ADMIN_OIDC_ISSUER" => Some(base.clone()),
            "FITZ_ADMIN_PUBLIC_ORIGIN" => Some(ORIGIN_URL.to_owned()),
            "FITZ_ADMIN_OIDC_CLIENT_ID" => Some(CLIENT_ID.to_owned()),
            "FITZ_ADMIN_OIDC_CLIENT_SECRET" => Some(secret.clone()),
            "FITZ_ADMIN_OIDC_API_AUDIENCE" => Some(ADMIN_API.to_owned()),
            _ => None,
        })
        .expect("issuer config")
        .expect("OIDC enabled");
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("test certificate");
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("TLS protocol")
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der()).into(),
        )
        .expect("TLS certificate and key");
        let root = reqwest::Certificate::from_der(certificate.cert.der()).expect("test trust root");
        let client = provider::client_builder()
            .tls_certs_merge([root])
            .no_proxy()
            .resolve("localhost", address)
            .build()
            .expect("bound provider client");

        let mut auth = AdminAuth::from_env();
        auth.mode = super::super::super::AdminAuthMode::Protected;
        auth.settings = Arc::new(None);
        auth.oidc = Some(Arc::new(Oidc::new(settings, client)));
        auth.set_provisioned_route_families(&[1, 2]);

        let mut runtime = Runtime::new(Arc::new(Router::new()));
        runtime.admin_auth = Arc::new(auth);
        runtime.mark_storage_ready();
        runtime.mark_domains_ready();
        runtime.mark_auth_config_ready();
        runtime.mark_startup_complete();
        let issuer = Arc::new(TestIssuer::new(base, secret));
        let task = tokio::spawn(serve(listener, Arc::clone(&issuer), Arc::new(tls)));
        Self {
            runtime: Arc::new(runtime),
            issuer,
            task,
        }
    }

    async fn request(&self, method: Method, path: &str, cookie: Option<&str>) -> Response {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header(ORIGIN, ORIGIN_URL);
        if let Some(cookie) = cookie {
            request = request.header(COOKIE, cookie);
        }
        crate::api::admin::handlers::handle_request(
            request
                .body(Body::new(Bytes::new()))
                .expect("admin request"),
            Arc::clone(&self.runtime),
        )
        .await
        .expect("admin response")
    }

    async fn login_response(
        &self,
        audiences: &[&'static str],
        api_permissions: &[&'static str],
    ) -> Response {
        let next = "/sessions?family=1";
        let start = format!(
            "/api/v1/session/oidc/start?{}",
            form_urlencoded::Serializer::new(String::new())
                .append_pair("next", next)
                .finish()
        );
        let start = self.request(Method::GET, &start, None).await;
        assert_eq!(start.status(), StatusCode::SEE_OTHER);
        let browser = cookie(&start, BROWSER_COOKIE);
        let authorization = Url::parse(start.headers()[LOCATION].to_str().unwrap())
            .expect("authorization endpoint");
        assert_eq!(authorization.path(), "/authorize");
        assert_eq!(
            authorization.origin().ascii_serialization(),
            self.issuer.base.trim_end_matches('/')
        );
        let params: HashMap<_, _> = authorization.query_pairs().into_owned().collect();
        assert_eq!(params["client_id"], CLIENT_ID);
        assert_eq!(params["audience"], ADMIN_API);
        assert_eq!(params["code_challenge_method"], "S256");
        assert_eq!(
            params["redirect_uri"],
            format!("{ORIGIN_URL}{CALLBACK_PATH}")
        );
        *self.issuer.flow.lock() = Some(Authorization {
            nonce: params["nonce"].clone(),
            challenge: params["code_challenge"].clone(),
            audiences: audiences.to_vec(),
            api_permissions: api_permissions.to_vec(),
        });
        let callback = format!(
            "{CALLBACK_PATH}?{}",
            form_urlencoded::Serializer::new(String::new())
                .append_pair("code", &self.issuer.authorization_code)
                .append_pair("state", &params["state"])
                .finish()
        );
        self.request(Method::GET, &callback, Some(&browser)).await
    }

    async fn session_cookie(&self) -> String {
        let response = self
            .login_response(
                &[ADMIN_API, "https://issuer.fitz.test/userinfo"],
                &["fitz:admin", "route_family:1"],
            )
            .await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers()[LOCATION], "/sessions?family=1");
        cookie(&response, SESSION_COOKIE)
    }
}

async fn serve(listener: TcpListener, issuer: Arc<TestIssuer>, tls: Arc<rustls::ServerConfig>) {
    let acceptor = TlsAcceptor::from(tls);
    while let Ok((stream, _)) = listener.accept().await {
        let acceptor = acceptor.clone();
        let issuer = Arc::clone(&issuer);
        tokio::spawn(async move {
            let Ok(stream) = acceptor.accept(stream).await else {
                return;
            };
            let service = service_fn(move |request| {
                let issuer = Arc::clone(&issuer);
                async move { Ok::<_, Infallible>(issuer.respond(request).await) }
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

fn cookie(response: &Response, name: &str) -> String {
    let value = response
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find(|header| header.starts_with(&format!("{name}=")))
        .expect("browser cookie");
    assert!(value.contains("; Secure"));
    assert!(value.contains("; HttpOnly"));
    value.split(';').next().unwrap().to_owned()
}

#[tokio::test(flavor = "current_thread")]
async fn should_list_only_authorized_families_given_an_oidc_session() {
    // Arrange
    let fixture = Fixture::start().await;
    let session = fixture.session_cookie().await;

    // Act
    let response = fixture
        .request(Method::GET, "/api/v1/session", Some(&session))
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let session: Value = serde_json::from_slice(&bytes).expect("session JSON");
    assert_eq!(session["route_families"], json!(["1"]));
    assert_eq!(session["route_families_wildcard"], false);
}

#[tokio::test(flavor = "current_thread")]
async fn should_deny_a_second_family_given_scoped_oidc_admin_authority() {
    // Arrange
    let fixture = Fixture::start().await;
    let session = fixture.session_cookie().await;

    // Act
    let response = fixture
        .request(Method::GET, "/api/v1/2/kv/stats", Some(&session))
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "current_thread")]
async fn should_deny_a_global_operation_given_scoped_oidc_admin_authority() {
    // Arrange
    let fixture = Fixture::start().await;
    let session = fixture.session_cookie().await;

    // Act
    let response = fixture
        .request(Method::GET, "/api/v1/sessions", Some(&session))
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "current_thread")]
async fn should_reject_oidc_login_given_the_wrong_api_audience() {
    // Arrange
    let fixture = Fixture::start().await;

    // Act
    let response = fixture
        .login_response(
            &["https://broker.fitz.test"],
            &["fitz:admin", "route_family:1"],
        )
        .await;

    // Assert
    assert_eq!(response.headers()[LOCATION], "/login?oidc_error=1");
    assert!(!response.headers().contains_key(SET_COOKIE));
}

#[tokio::test(flavor = "current_thread")]
async fn should_revoke_an_oidc_admin_cookie_on_logout() {
    // Arrange
    let fixture = Fixture::start().await;
    let session = fixture.session_cookie().await;

    // Act
    let logout = fixture
        .request(Method::DELETE, "/api/v1/session", Some(&session))
        .await;

    // Assert
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    let replay = fixture
        .request(Method::GET, "/api/v1/session", Some(&session))
        .await;
    assert_eq!(replay.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "current_thread")]
async fn should_reject_admin_grants_carried_only_by_the_identity_token() {
    // Arrange
    let fixture = Fixture::start().await;

    // Act
    let response = fixture
        .login_response(&[ADMIN_API], &["route_family:*"])
        .await;

    // Assert
    assert_eq!(response.headers()[LOCATION], "/login?oidc_error=1");
    assert!(!response.headers().contains_key(SET_COOKIE));
}

#[tokio::test(flavor = "current_thread")]
async fn should_reject_an_admin_token_that_also_addresses_the_broker() {
    // Arrange
    let fixture = Fixture::start().await;

    // Act
    let response = fixture
        .login_response(
            &[ADMIN_API, "fitz-broker"],
            &["fitz:admin", "route_family:1"],
        )
        .await;

    // Assert
    assert_eq!(response.headers()[LOCATION], "/login?oidc_error=1");
    assert!(!response.headers().contains_key(SET_COOKIE));
}
