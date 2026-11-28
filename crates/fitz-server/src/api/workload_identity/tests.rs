use super::*;
use crate::api::handlers::{
    spawn_http_listener_with_bound_socket, spawn_tcp_listener_with_bound_socket, ListenerHandle,
};
use crate::auth::Access;
use crate::boot::BootConfig;
use crate::protocol::wire3::{control, header::MessageHeader};
use crate::testkit::transport::build_connect_frame;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::rsa::KeySize;
use aws_lc_rs::signature::{KeyPair, RsaKeyPair, RSA_PKCS1_SHA256};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{client_async, tungstenite::Message};

const ACCOUNT: &str = "123456789012";
const ROLE: &str = "arn:aws:iam::123456789012:role/workers/Orders";
const AUDIENCE: &str = "https://orders.fitz.test/workload";
const KID: &str = "fixture-key";

struct Fixture {
    trust: Arc<WorkloadIdentityTrust>,
    issuer: String,
    signing_key: RsaKeyPair,
}

impl Fixture {
    fn new() -> Self {
        let issuer = format!(
            "https://fixture-{}{}",
            uuid::Uuid::new_v4().simple(),
            AWS_ISSUER_SUFFIX
        );
        Self::from_document(
            issuer.clone(),
            &json!({
                "audience": AUDIENCE,
                "accounts": [{
                    "account_id": ACCOUNT, "issuer": issuer,
                    "bindings": [{
                        "role_arn": ROLE, "route_family": 2, "realm": "prod",
                        "permissions": ["notice://prod/orders/**#read"]
                    }]
                }]
            }),
        )
    }

    fn from_document(issuer: String, document: &Value) -> Self {
        let trust = WorkloadIdentityTrust::parse(&serde_json::to_vec(document).unwrap()).unwrap();
        let signing_key = RsaKeyPair::generate(KeySize::Rsa2048).unwrap();
        let public = signing_key.public_key();
        let keys = json!({"keys": [{
            "kid": KID, "kty": "RSA", "alg": "RS256", "use": "sig",
            "n": URL_SAFE_NO_PAD.encode(public.modulus().big_endian_without_leading_zero()),
            "e": URL_SAFE_NO_PAD.encode(public.exponent().big_endian_without_leading_zero())
        }]});
        crate::api::jwks::cache_jwks_from_json(&trust.issuers[&issuer].jwks_url, &keys.to_string())
            .unwrap();
        Self {
            trust: Arc::new(trust),
            issuer,
            signing_key,
        }
    }

    fn claims(&self) -> Value {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        json!({
            "iss": self.issuer, "aud": AUDIENCE, "sub": ROLE,
            "iat": now, "exp": now + 120,
            "https://sts.amazonaws.com/": {"aws_account": ACCOUNT, "principal_id": ROLE},
            "permissions": ["notice://**#*"], "tid": "token-controlled"
        })
    }

    fn workload_claims(&self, audience: &str, subject: &str) -> Value {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        json!({
            "iss": self.issuer, "aud": audience, "sub": subject, "iat": now, "exp": now + 120,
            "permissions": ["notice://**#*"]
        })
    }

    fn sign(&self, claims: &Value) -> String {
        let input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(json!({"alg": "RS256", "kid": KID}).to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string()),
        );
        let mut signature = vec![0; self.signing_key.public_modulus_len()];
        self.signing_key
            .sign(
                &RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                input.as_bytes(),
                &mut signature,
            )
            .unwrap();
        format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature))
    }

    async fn verify(&self, claims: &Value) -> Result<WorkloadSession, String> {
        self.trust.verify(&self.sign(claims), &self.issuer).await
    }

    fn config(&self) -> BootConfig {
        let mut config = BootConfig::new()
            .with_bind_addr("127.0.0.1".to_string())
            .with_assume_external_tls(true)
            .with_route_families(vec![1, 2])
            .with_auth_config(AuthConfig::Disabled)
            .with_route_family_resolver(crate::auth::RouteFamilyResolverConfig::default());
        config.auth_required = true;
        config.workload_identity = Ok(Some(self.trust.clone()));
        config
    }
}

#[tokio::test]
async fn should_grant_only_the_locally_bound_route_permissions() {
    // Arrange
    let fixture = Fixture::new();

    // Act
    let session = fixture.verify(&fixture.claims()).await.unwrap();

    // Assert
    assert_eq!(session.route_family, 2);
    assert_eq!(session.claims.identity_value.as_deref(), Some("prod"));
    assert!(session
        .permissions
        .allows_route("notice://prod/orders/new", Access::Read));
    assert!(!session
        .permissions
        .allows_route("notice://prod/orders/new", Access::Write));
    assert!(!session
        .permissions
        .allows_route("notice://other/orders/new", Access::Read));
}

#[tokio::test]
async fn should_reject_signed_tokens_outside_the_aws_trust_binding() {
    // Arrange
    let fixture = Fixture::new();
    let other_role = "arn:aws:iam::123456789012:role/Unbound";
    let mut unbound = fixture.claims();
    unbound["sub"] = json!(other_role);
    unbound["https://sts.amazonaws.com/"]["principal_id"] = json!(other_role);
    let mut cases = vec![unbound];
    for (pointer, value) in [
        ("/iss", json!("https://other.tokens.sts.global.api.aws")),
        ("/aud", json!([AUDIENCE, "also-another-service"])),
        ("/aud", json!("fitz-broker")),
        ("/exp", json!(1)),
        ("/iat", json!(u64::MAX)),
        (
            "/https:~1~1sts.amazonaws.com~1/aws_account",
            json!("210987654321"),
        ),
        (
            "/https:~1~1sts.amazonaws.com~1/principal_id",
            json!(other_role),
        ),
    ] {
        let mut claims = fixture.claims();
        *claims.pointer_mut(pointer).unwrap() = value;
        cases.push(claims);
    }
    let mut future = fixture.claims();
    future["nbf"] = json!(u64::MAX);
    cases.push(future);
    let mut origin = fixture.claims();
    origin["https://sts.amazonaws.com/"]["subject_mode"] = json!("OriginIdentity");
    cases.push(origin);

    // Act
    let outcomes =
        futures_util::future::join_all(cases.iter().map(|claims| fixture.verify(claims))).await;

    // Assert
    assert!(outcomes.iter().all(Result::is_err));
}

#[tokio::test]
async fn should_reject_a_token_signed_by_another_key() {
    // Arrange
    let fixture = Fixture::new();
    let other = Fixture::new();
    let compact = other.sign(&fixture.claims());

    // Act
    let result = fixture.trust.verify(&compact, &fixture.issuer).await;

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_require_external_tls_given_aws_connect_configuration() {
    // Arrange
    let fixture = Fixture::new();
    let config = fixture.config().with_assume_external_tls(false);

    // Act
    let result = config.validate();

    // Assert
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("workload CONNECT requires"));
}

#[tokio::test]
async fn should_authenticate_aws_roles_via_tcp_connect() {
    // Arrange
    let fixture = Fixture::new();
    let (ingress, config, runtime) = ready_runtime(&fixture);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let listener =
        spawn_tcp_listener_with_bound_socket(socket, ingress.clone(), &config, runtime).unwrap();
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Act
    let frame = tcp_connect(&mut client, &fixture.sign(&fixture.claims())).await;
    let sessions = ingress.active_sessions();

    // Assert
    assert_eq!(
        MessageHeader::decode_wire3(&frame).unwrap().message_type,
        control::HELLO
    );
    assert_eq!(
        sessions[0].route_family,
        crate::runtime::routing::RouteFamily::new(2)
    );
    drop(client);
    stop(listener).await;
}

#[tokio::test]
async fn should_authenticate_aws_roles_via_websocket_connect() {
    // Arrange
    let fixture = Fixture::new();
    let (ingress, config, runtime) = ready_runtime(&fixture);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let listener =
        spawn_http_listener_with_bound_socket(socket, ingress.clone(), &config, runtime, vec![])
            .unwrap();
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut client, _) = client_async(format!("ws://{addr}/"), stream).await.unwrap();

    // Act
    let connect = build_connect_frame("", &fixture.sign(&fixture.claims()));
    client.send(Message::Binary(connect.into())).await.unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Message::Binary(bytes) = client.next().await.unwrap().unwrap() {
                break bytes;
            }
        }
    })
    .await
    .unwrap();
    let sessions = ingress.active_sessions();

    // Assert
    assert_eq!(
        MessageHeader::decode_wire3(&frame).unwrap().message_type,
        control::HELLO
    );
    assert_eq!(
        sessions[0].route_family,
        crate::runtime::routing::RouteFamily::new(2)
    );
    client.close(None).await.unwrap();
    stop(listener).await;
}

#[tokio::test]
async fn should_reject_unbound_aws_role_without_authenticating_the_session() {
    // Arrange
    let fixture = Fixture::new();
    let (ingress, config, runtime) = ready_runtime(&fixture);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let listener =
        spawn_tcp_listener_with_bound_socket(socket, ingress.clone(), &config, runtime).unwrap();
    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut claims = fixture.claims();
    let other = "arn:aws:iam::123456789012:role/Other";
    claims["sub"] = json!(other);
    claims["https://sts.amazonaws.com/"]["principal_id"] = json!(other);

    // Act
    let frame = tcp_connect(&mut client, &fixture.sign(&claims)).await;

    // Assert
    assert_eq!(
        MessageHeader::decode_wire3(&frame).unwrap().message_type,
        control::CONNECT_REJECTED
    );
    for session in ingress.active_sessions() {
        assert!(!session.authenticated);
        assert!(ingress
            .get_session_actor(session.session_id)
            .is_none_or(|actor| !actor.is_authenticated()));
    }
    drop(client);
    stop(listener).await;
}

async fn tcp_connect(client: &mut TcpStream, jwt: &str) -> Vec<u8> {
    let connect = build_connect_frame("", jwt);
    client
        .write_u32(u32::try_from(connect.len()).unwrap())
        .await
        .unwrap();
    client.write_all(&connect).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let len = usize::try_from(client.read_u32().await.unwrap()).unwrap();
        let mut frame = vec![0; len];
        client.read_exact(&mut frame).await.unwrap();
        frame
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn should_deny_new_operations_after_the_established_session_token_expires() {
    // Arrange
    let fixture = Fixture::new();
    let WorkloadSession {
        mut claims,
        permissions,
        ..
    } = fixture.verify(&fixture.claims()).await.unwrap();
    let mut actor = crate::session::actor::SessionActor::new(
        crate::session::SessionId(1),
        SessionPermissions::empty(),
    );
    // Advance the effective expiry without sleeping or changing the system clock.
    claims.exp = 1;
    actor.authenticate(claims, permissions);

    // Act
    let allowed = actor.authorize_route("notice://prod/orders/new", Access::Read);

    // Assert
    assert!(!allowed);
}

fn ready_runtime(
    fixture: &Fixture,
) -> (
    Arc<crate::api::runtime_ingress::RuntimeIngress>,
    crate::api::ingress::IngressConfig,
    crate::boot::Runtime,
) {
    let (_, ingress, config, runtime) = crate::boot::runtime::init(&fixture.config()).unwrap();
    runtime.mark_auth_config_ready();
    runtime.mark_storage_ready();
    runtime.mark_domains_ready();
    runtime.mark_startup_complete();
    (ingress, config, runtime)
}

async fn stop(listener: ListenerHandle) {
    listener.shutdown.send(()).unwrap();
    listener.join.await.unwrap();
}

mod providers;

#[test]
fn should_limit_a_flat_aws_wildcard_to_roles_in_the_configured_account() {
    // Arrange
    let fixture = Fixture::new();
    let file = environment::read(|name| {
        Ok(match name {
            "FITZ_WORKLOAD_PROVIDER" => Some("aws"),
            "FITZ_WORKLOAD_AUDIENCE" => Some(AUDIENCE),
            "FITZ_WORKLOAD_PERMISSIONS" => Some("notice://prod/orders/**#read"),
            "FITZ_WORKLOAD_SUBJECT" => Some("*"),
            "FITZ_WORKLOAD_AWS_ACCOUNT_ID" => Some(ACCOUNT),
            "FITZ_WORKLOAD_AWS_ISSUER" => Some(fixture.issuer.as_str()),
            _ => None,
        }
        .map(str::to_owned))
    })
    .unwrap()
    .unwrap();
    let trust = WorkloadIdentityTrust::from_document(file, true).unwrap();
    let issuer = &trust.issuers[&fixture.issuer];
    let claims = fixture.claims();
    let now = claims["iat"].as_u64().unwrap();
    let token = serde_json::from_value(claims.clone()).unwrap();
    // Act
    let session = WorkloadIdentityTrust::bind(&token, &fixture.issuer, issuer, now).unwrap();
    // Assert
    assert_eq!(session.route_family, 1);
    assert_eq!(session.claims.sub, ROLE);
    for subject in [
        "arn:aws:iam::987654321098:role/Orders",
        "arn:aws:iam::123456789012:user/Orders",
    ] {
        let mut claims = claims.clone();
        claims["sub"] = json!(subject);
        claims["https://sts.amazonaws.com/"]["principal_id"] = json!(subject);
        let token = serde_json::from_value(claims).unwrap();
        assert!(WorkloadIdentityTrust::bind(&token, &fixture.issuer, issuer, now).is_err());
    }
}

#[test]
fn should_reject_a_flat_google_wildcard() {
    // Arrange
    let file = environment::read(|name| {
        Ok(match name {
            "FITZ_WORKLOAD_PROVIDER" => Some("google"),
            "FITZ_WORKLOAD_AUDIENCE" => Some(AUDIENCE),
            "FITZ_WORKLOAD_PERMISSIONS" => Some("notice://prod/orders/**#read"),
            "FITZ_WORKLOAD_SUBJECT" => Some("*"),
            _ => None,
        }
        .map(str::to_owned))
    })
    .unwrap()
    .unwrap();
    // Act
    let trust = WorkloadIdentityTrust::from_document(file, true);

    // Assert
    assert!(trust.is_err());
}
