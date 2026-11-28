use super::*;

const AZURE_AUDIENCE: &str = "c5ca00bc-26d3-4779-9284-528dedc8ad77";
const AZURE_OBJECT: &str = "4c6d885e-09af-4118-8f36-ed0477a45256";
const AZURE_CLIENT: &str = "07c6ab56-2dd1-47b8-b6d5-ab40e5ab5898";
const GOOGLE_SUBJECT: &str = "112010400000000710080";

fn azure_fixture(version: AzureVersion) -> (Fixture, Value) {
    let tenant = uuid::Uuid::new_v4().to_string();
    let issuer = version.issuer(&tenant);
    let (version_claim, actor_claim, audience) = match version {
        AzureVersion::V1 => ("1.0", "appid", "api://fitz-worker"),
        AzureVersion::V2 => ("2.0", "azp", AZURE_AUDIENCE),
    };
    let fixture = Fixture::from_document(
        issuer,
        &json!({
            "azure": [{
                "tenant_id": tenant, "token_version": version_claim, "audience": audience,
                "bindings": [{
                    "subject": AZURE_OBJECT, "client_id": AZURE_CLIENT, "route_family": 2,
                    "permissions": ["notice://prod/orders/**#read"]
                }]
            }]
        }),
    );
    let mut claims = fixture.workload_claims(audience, "opaque-entra-sub");
    claims["tid"] = json!(tenant);
    claims["oid"] = json!(AZURE_OBJECT);
    claims["idtyp"] = json!("app");
    claims["ver"] = json!(version_claim);
    claims[actor_claim] = json!(AZURE_CLIENT);
    (fixture, claims)
}

#[tokio::test]
async fn should_bind_the_azure_service_principal_for_each_supported_token_version() {
    // Arrange
    let (v1, token1) = azure_fixture(AzureVersion::V1);
    let (v2, token2) = azure_fixture(AzureVersion::V2);

    // Act
    let (first, second) = tokio::join!(v1.verify(&token1), v2.verify(&token2));

    // Assert
    for session in [first.unwrap(), second.unwrap()] {
        assert_eq!(session.claims.sub, AZURE_OBJECT);
        assert_eq!(session.route_family, 2);
        assert!(!session
            .permissions
            .allows_route("notice://prod/orders/new", Access::Write));
    }
}

#[tokio::test]
async fn should_reject_an_azure_token_without_the_exact_app_identity() {
    // Arrange
    let (fixture, claims) = azure_fixture(AzureVersion::V2);
    let other = uuid::Uuid::new_v4().to_string();
    let cases = [
        ("idtyp", json!("user")),
        ("idtyp", Value::Null),
        ("scp", json!("access_as_user")),
        ("tid", json!(other)),
        ("oid", json!(other)),
        ("azp", json!(other)),
        ("ver", json!("1.0")),
    ]
    .map(|(claim, value)| {
        let mut bad = claims.clone();
        bad[claim] = value;
        bad
    });

    // Act
    let outcomes =
        futures_util::future::join_all(cases.iter().map(|candidate| fixture.verify(candidate)))
            .await;

    // Assert
    assert!(outcomes.iter().all(Result::is_err));
}

fn google_fixture() -> &'static Fixture {
    static GOOGLE_FIXTURE: std::sync::OnceLock<Fixture> = std::sync::OnceLock::new();
    GOOGLE_FIXTURE.get_or_init(|| {
        Fixture::from_document(
            GOOGLE_ISSUER.to_owned(),
            &json!({
                "google": {
                    "audience": AUDIENCE, "bindings": [{
                        "subject": GOOGLE_SUBJECT, "route_family": 2,
                        "permissions": ["notice://prod/orders/**#read"]
                    }]
                }
            }),
        )
    })
}

#[tokio::test]
async fn should_bind_the_google_service_account_only_when_subject_and_azp_match() {
    // Arrange
    let fixture = google_fixture();
    let mut accepted = fixture.workload_claims(AUDIENCE, GOOGLE_SUBJECT);
    accepted["azp"] = json!(GOOGLE_SUBJECT);
    let mut denied = accepted.clone();
    denied["azp"] = json!("unrelated-consumer-client.apps.googleusercontent.com");

    // Act
    let (valid, invalid) = tokio::join!(fixture.verify(&accepted), fixture.verify(&denied));

    // Assert
    assert_eq!(valid.unwrap().claims.sub, GOOGLE_SUBJECT);
    assert!(invalid.is_err());
}

#[tokio::test]
async fn should_authenticate_azure_workloads_across_supported_transports() {
    // Arrange
    let (fixture, claims) = azure_fixture(AzureVersion::V2);

    // Act
    let (tcp, websocket) = connect_both(&fixture, &claims).await;

    // Assert
    assert_eq!((tcp, websocket), (control::HELLO, control::HELLO));
}

#[tokio::test]
async fn should_authenticate_google_workloads_across_supported_transports() {
    // Arrange
    let fixture = google_fixture();
    let mut claims = fixture.workload_claims(AUDIENCE, GOOGLE_SUBJECT);
    claims["azp"] = json!(GOOGLE_SUBJECT);

    // Act
    let (tcp, websocket) = connect_both(fixture, &claims).await;

    // Assert
    assert_eq!((tcp, websocket), (control::HELLO, control::HELLO));
}

async fn connect_both(fixture: &Fixture, claims: &Value) -> (u16, u16) {
    let (ingress, config, runtime) = ready_runtime(fixture);
    let tcp_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = tcp_socket.local_addr().unwrap();
    let ws_addr = ws_socket.local_addr().unwrap();
    let tcp_listener =
        spawn_tcp_listener_with_bound_socket(tcp_socket, ingress.clone(), &config, runtime.clone())
            .unwrap();
    let ws_listener =
        spawn_http_listener_with_bound_socket(ws_socket, ingress, &config, runtime, vec![])
            .unwrap();

    let token = fixture.sign(claims);
    let mut tcp = TcpStream::connect(tcp_addr).await.unwrap();
    let tcp_frame = tcp_connect(&mut tcp, &token).await;
    let stream = TcpStream::connect(ws_addr).await.unwrap();
    let (mut ws, _) = client_async(format!("ws://{ws_addr}/"), stream)
        .await
        .unwrap();
    ws.send(Message::Binary(build_connect_frame("", &token).into()))
        .await
        .unwrap();
    let ws_frame = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Message::Binary(bytes) = ws.next().await.unwrap().unwrap() {
                break bytes;
            }
        }
    })
    .await
    .unwrap();
    drop(tcp);
    ws.close(None).await.unwrap();
    stop(tcp_listener).await;
    stop(ws_listener).await;
    (
        MessageHeader::decode_wire3(&tcp_frame)
            .unwrap()
            .message_type,
        MessageHeader::decode_wire3(&ws_frame).unwrap().message_type,
    )
}

#[test]
fn should_reserve_a_workload_audience_even_for_a_mixed_audience_token() {
    // Arrange
    let (fixture, mut claims) = azure_fixture(AzureVersion::V2);
    claims["aud"] = json!([AZURE_AUDIENCE, "external-broker"]);
    claims["iss"] = json!("https://external.fitz.test");
    let compact = fixture.sign(&claims);
    let candidate = crate::auth::parse_jwt_noverify(&compact).unwrap();

    // Act
    let selects_workload = fixture.trust.selects(&candidate);

    // Assert
    assert!(selects_workload);
}

#[test]
fn should_reject_a_shared_workload_audience() {
    // Arrange
    let (fixture, _) = azure_fixture(AzureVersion::V2);
    let generic = AuthConfig::hmac("test-secret", AZURE_AUDIENCE);

    // Act
    let deploy = fixture
        .trust
        .validate_deployment(true, true, &generic, &[1, 2]);

    // Assert
    assert!(deploy.is_err());
}

#[tokio::test]
async fn should_verify_es384_given_an_explicit_aws_algorithm_allowlist() {
    // Arrange
    use aws_lc_rs::signature::{EcdsaKeyPair, ECDSA_P384_SHA384_FIXED_SIGNING};

    let issuer = format!(
        "https://fixture-{}{}",
        uuid::Uuid::new_v4().simple(),
        AWS_ISSUER_SUFFIX
    );
    let fixture = Fixture::from_document(
        issuer.clone(),
        &json!({
            "audience": AUDIENCE,
            "accounts": [{
                "account_id": ACCOUNT, "issuer": issuer, "algorithms": ["ES384"],
                "bindings": [{
                    "role_arn": ROLE, "route_family": 2,
                    "permissions": ["notice://prod/orders/**#read"]
                }]
            }]
        }),
    );
    let algorithm = &ECDSA_P384_SHA384_FIXED_SIGNING;
    let rng = SystemRandom::new();
    let seed = EcdsaKeyPair::generate_pkcs8(algorithm, &rng).unwrap();
    let signing_key = EcdsaKeyPair::from_pkcs8(algorithm, seed.as_ref()).unwrap();
    let point = signing_key.public_key().as_ref();
    assert_eq!(point.len(), 97);
    let keyset = json!({"keys": [{
        "kid": KID, "kty": "EC", "crv": "P-384", "alg": "ES384", "use": "sig",
        "x": URL_SAFE_NO_PAD.encode(&point[1..49]),
        "y": URL_SAFE_NO_PAD.encode(&point[49..])
    }]});
    crate::api::jwks::cache_jwks_from_json(
        &fixture.trust.issuers[&fixture.issuer].jwks_url,
        &keyset.to_string(),
    )
    .unwrap();
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(json!({"alg":"ES384","kid":KID}).to_string()),
        URL_SAFE_NO_PAD.encode(fixture.claims().to_string())
    );
    let signature = signing_key.sign(&rng, input.as_bytes()).unwrap();
    let token = format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature.as_ref()));

    // Act
    let verified = fixture.trust.verify(&token, &fixture.issuer).await;

    // Assert
    assert_eq!(verified.unwrap().claims.sub, ROLE);
}
