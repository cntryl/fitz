//! Real broker seed data for operator-console tests.
//!
//! This helper drives the public Fitz wire protocols against a `TestServer`.
//! Durable state is committed through the corresponding domains. Live state is
//! kept visible by retaining the owning `TestClient`s in `OperatorSeedReport`.

use crate::benchkit::transport as frames;
use crate::testkit::transport::{
    build_connect_frame, generate_test_jwt, TestClient, TestServer, TlvFrameBuilder,
};
use bytes::BufMut;

const DEFAULT_AREA: &str = "billing";
const KV_RESOURCE: &str = "accounts";
const STREAM_RESOURCE: &str = "events";
const QUEUE_RESOURCE: &str = "settlement";
const SCHEDULE_RESOURCE: &str = "reconcile";
const SCHEDULE_OPERATION: &str = "run";
const LEASE_RESOURCE: &str = "settlement";
const NOTICE_RESOURCE: &str = "events";
const RPC_RESOURCE: &str = "profile";
const RPC_OPERATION: &str = "sync";

#[derive(Debug, Clone)]
pub struct OperatorSeedFamily {
    pub route_family: u32,
    pub realm: String,
    pub area: String,
}

impl OperatorSeedFamily {
    pub fn new(route_family: u32, realm: impl Into<String>) -> Self {
        Self {
            route_family,
            realm: realm.into(),
            area: DEFAULT_AREA.to_string(),
        }
    }
}

pub struct OperatorSeedReport {
    pub families: Vec<OperatorSeededFamily>,
    pub live_clients: Vec<TestClient>,
}

impl OperatorSeedReport {
    pub async fn close(mut self) -> Result<(), String> {
        for client in self.live_clients.drain(..) {
            client.close().await.map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OperatorSeededFamily {
    pub route_family: u32,
    pub realm: String,
    pub area: String,
    pub kv_route: String,
    pub kv_key: String,
    pub kv_value: String,
    pub stream_route: String,
    pub queue_route: String,
    pub schedule_route: String,
    pub lease_route: String,
    pub notice_route: String,
    pub rpc_route: String,
}

impl OperatorSeededFamily {
    fn from_family(family: &OperatorSeedFamily) -> Self {
        let realm = family.realm.clone();
        let area = family.area.clone();
        Self {
            route_family: family.route_family,
            kv_route: format!("kv://{realm}/{area}/{KV_RESOURCE}"),
            kv_key: format!("account:{}", family.route_family),
            kv_value: format!("active-family-{}", family.route_family),
            stream_route: format!("stream://{realm}/{area}/{STREAM_RESOURCE}"),
            queue_route: format!("queue://{realm}/{area}/{QUEUE_RESOURCE}"),
            schedule_route: format!(
                "schedule://{realm}/{area}/{SCHEDULE_RESOURCE}/{SCHEDULE_OPERATION}"
            ),
            lease_route: format!("lease://{realm}/{area}/{LEASE_RESOURCE}"),
            notice_route: format!("notice://{realm}/{area}/{NOTICE_RESOURCE}"),
            rpc_route: format!("rpc://{realm}/{area}/{RPC_RESOURCE}/{RPC_OPERATION}"),
            realm,
            area,
        }
    }
}

pub async fn seed_operator_console(
    server: &TestServer,
    families: &[OperatorSeedFamily],
) -> Result<OperatorSeedReport, String> {
    let mut expected_authenticated_sessions = 0usize;
    let mut live_clients = Vec::new();
    let mut seeded_families = Vec::new();

    for family in families {
        let seeded = OperatorSeededFamily::from_family(family);
        let mut domain_client =
            authenticated_client(server, &family.realm, &mut expected_authenticated_sessions)
                .await?;

        seed_kv(&mut domain_client, &seeded).await?;
        seed_stream(&mut domain_client, &seeded).await?;
        seed_queue(&mut domain_client, &seeded).await?;
        seed_schedule(&mut domain_client, &seeded).await?;
        seed_lease_owner(&mut domain_client, &seeded).await?;

        let mut lease_waiter =
            authenticated_client(server, &family.realm, &mut expected_authenticated_sessions)
                .await?;
        seed_lease_waiter(&mut lease_waiter, &seeded).await?;

        let mut notice_subscriber =
            authenticated_client(server, &family.realm, &mut expected_authenticated_sessions)
                .await?;
        seed_notice_subscription(&mut notice_subscriber, &seeded).await?;

        let mut notice_publisher =
            authenticated_client(server, &family.realm, &mut expected_authenticated_sessions)
                .await?;
        seed_notice_publish(&mut notice_publisher, &seeded).await?;

        let mut rpc_worker =
            authenticated_client(server, &family.realm, &mut expected_authenticated_sessions)
                .await?;
        seed_rpc_worker(&mut rpc_worker, &seeded).await?;

        let mut rpc_caller =
            authenticated_client(server, &family.realm, &mut expected_authenticated_sessions)
                .await?;
        seed_rpc_pending_request(&mut rpc_caller, &seeded).await?;

        live_clients.push(domain_client);
        live_clients.push(lease_waiter);
        live_clients.push(notice_subscriber);
        live_clients.push(notice_publisher);
        live_clients.push(rpc_worker);
        live_clients.push(rpc_caller);
        seeded_families.push(seeded);
    }

    Ok(OperatorSeedReport {
        families: seeded_families,
        live_clients,
    })
}

async fn authenticated_client(
    server: &TestServer,
    realm: &str,
    expected_authenticated_sessions: &mut usize,
) -> Result<TestClient, String> {
    let mut client = server.connect().await.map_err(|error| error.to_string())?;
    let jwt = generate_test_jwt(realm);
    client
        .send_frame(&build_connect_frame(realm, &jwt))
        .await
        .map_err(|error| error.to_string())?;
    *expected_authenticated_sessions += 1;
    server
        .wait_for_authenticated_sessions(*expected_authenticated_sessions)
        .await
        .map_err(|error| error.to_string())?;
    Ok(client)
}

async fn seed_kv(client: &mut TestClient, seeded: &OperatorSeededFamily) -> Result<(), String> {
    let begin = request_frame(
        client,
        &frames::build_kv_begin(&seeded.kv_route, 1, 0),
        "KV BEGIN",
    )
    .await?;
    let (_msg_type, status, data) = frames::parse_kv_response(&begin);
    ensure_ok("KV BEGIN", status)?;
    let tx_id = frames::parse_kv_tx_id(&data)?;

    let put = request_frame(
        client,
        &frames::build_kv_put(
            tx_id,
            &seeded.kv_route,
            seeded.kv_key.as_bytes(),
            seeded.kv_value.as_bytes(),
        ),
        "KV PUT",
    )
    .await?;
    let (_msg_type, status, _data) = frames::parse_kv_response(&put);
    ensure_ok("KV PUT", status)?;

    let commit = request_frame(
        client,
        &frames::build_kv_commit(tx_id, &seeded.kv_route),
        "KV COMMIT",
    )
    .await?;
    let (_msg_type, status, _data) = frames::parse_kv_response(&commit);
    ensure_ok("KV COMMIT", status)
}

async fn seed_stream(client: &mut TestClient, seeded: &OperatorSeededFamily) -> Result<(), String> {
    let begin = request_frame(
        client,
        &frames::build_stream_begin(&seeded.stream_route),
        "STREAM BEGIN",
    )
    .await?;
    let (_msg_type, status, data) = frames::parse_stream_response(&begin);
    ensure_ok("STREAM BEGIN", status)?;
    let session_id = frames::parse_stream_session_id(&data)?;

    let event_body = format!("event-for-family-{}", seeded.route_family);
    let event_metadata = format!("family={};kind=operator-seed", seeded.route_family);
    let append = request_frame(
        client,
        &frames::build_stream_append_with_metadata(
            session_id,
            0,
            event_body.as_bytes(),
            Some(event_metadata.as_bytes()),
        ),
        "STREAM APPEND",
    )
    .await?;
    let (_msg_type, status, _data) = frames::parse_stream_response(&append);
    ensure_ok("STREAM APPEND", status)?;

    let commit = request_frame(
        client,
        &frames::build_stream_commit(session_id, 0),
        "STREAM COMMIT",
    )
    .await?;
    let (_msg_type, status, _data) = frames::parse_stream_response(&commit);
    ensure_ok("STREAM COMMIT", status)
}

async fn seed_queue(client: &mut TestClient, seeded: &OperatorSeededFamily) -> Result<(), String> {
    for suffix in ["ready", "inflight"] {
        let body = format!("{}-work-family-{}", suffix, seeded.route_family);
        let response = request_frame(
            client,
            &frames::build_queue_enqueue(&seeded.queue_route, body.as_bytes()),
            "QUEUE ENQUEUE",
        )
        .await?;
        let (_msg_type, status, _data) = frames::parse_queue_response(&response);
        ensure_ok("QUEUE ENQUEUE", status)?;
    }

    let response = request_frame(
        client,
        &frames::build_queue_dequeue(&seeded.queue_route),
        "QUEUE RESERVE",
    )
    .await?;
    let (_msg_type, status, _data) = frames::parse_queue_response(&response);
    ensure_ok("QUEUE RESERVE", status)
}

async fn seed_schedule(
    client: &mut TestClient,
    seeded: &OperatorSeededFamily,
) -> Result<(), String> {
    let response = request_frame(
        client,
        &frames::build_schedule_create(
            &seeded.schedule_route,
            "0 * * * *",
            b"{\"seed\":\"operator\"}",
        ),
        "SCHEDULE CREATE",
    )
    .await?;
    let (_msg_type, status, _data) = frames::parse_schedule_response(&response);
    ensure_ok("SCHEDULE CREATE", status)
}

async fn seed_lease_owner(
    client: &mut TestClient,
    seeded: &OperatorSeededFamily,
) -> Result<(), String> {
    let owner = format!("owner-family-{}", seeded.route_family);
    let response = request_frame(
        client,
        &frames::build_lease_acquire_immediate(&seeded.lease_route, &owner, 300),
        "LEASE ACQUIRE",
    )
    .await?;
    let (_msg_type, status, data) = frames::parse_lease_response(&response);
    ensure_ok("LEASE ACQUIRE", status)?;
    frames::parse_lease_token_response(&data)?;
    Ok(())
}

async fn seed_lease_waiter(
    client: &mut TestClient,
    seeded: &OperatorSeededFamily,
) -> Result<(), String> {
    let owner = format!("waiter-family-{}", seeded.route_family);
    let response = request_frame(
        client,
        &build_lease_acquire_with_wait(&seeded.lease_route, &owner, 300, 30),
        "LEASE WAIT",
    )
    .await?;
    let (_msg_type, status, data) = frames::parse_lease_response(&response);
    ensure_ok("LEASE WAIT", status)?;
    frames::parse_lease_token_response(&data)?;
    Ok(())
}

async fn seed_notice_subscription(
    client: &mut TestClient,
    seeded: &OperatorSeededFamily,
) -> Result<(), String> {
    let response = request_frame(
        client,
        &frames::build_notice_subscribe(&seeded.notice_route),
        "NOTICE SUBSCRIBE",
    )
    .await?;
    let (_msg_type, status, data) = frames::parse_notice_response(&response);
    ensure_ok("NOTICE SUBSCRIBE", status)?;
    frames::parse_notice_subscription_id(&data)?;
    Ok(())
}

async fn seed_notice_publish(
    client: &mut TestClient,
    seeded: &OperatorSeededFamily,
) -> Result<(), String> {
    let body = format!("notice-family-{}", seeded.route_family);
    client
        .send_frame(&frames::build_notice_publish(
            &seeded.notice_route,
            body.as_bytes(),
        ))
        .await
        .map_err(|error| format!("NOTICE PUBLISH: {error}"))
}

async fn seed_rpc_worker(
    client: &mut TestClient,
    seeded: &OperatorSeededFamily,
) -> Result<(), String> {
    let response = request_frame(
        client,
        &frames::build_rpc_subscribe(&seeded.rpc_route),
        "RPC SUBSCRIBE",
    )
    .await?;
    let (_msg_type, status, _data) = frames::parse_rpc_response(&response);
    ensure_ok("RPC SUBSCRIBE", status)
}

async fn seed_rpc_pending_request(
    client: &mut TestClient,
    seeded: &OperatorSeededFamily,
) -> Result<(), String> {
    let body = format!("rpc-family-{}", seeded.route_family);
    client
        .send_frame(&frames::build_rpc_request(
            &seeded.rpc_route,
            body.as_bytes(),
        ))
        .await
        .map_err(|error| format!("RPC REQUEST: {error}"))
}

async fn request_frame(
    client: &mut TestClient,
    frame: &[u8],
    operation: &str,
) -> Result<Vec<u8>, String> {
    client
        .request(frame, 2_000)
        .await
        .map_err(|error| format!("{operation}: {error}"))
}

fn build_lease_acquire_with_wait(
    route: &str,
    owner_id: &str,
    ttl_secs: i32,
    wait_seconds: u32,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(usize_to_u32_saturating(route.len()));
    buf.put_slice(route.as_bytes());
    buf.put_u32(usize_to_u32_saturating(owner_id.len()));
    buf.put_slice(owner_id.as_bytes());
    buf.put_u64(ttl_secs as u64);
    buf.put_u32(wait_seconds);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(400, &buf);
    builder.build()
}

fn ensure_ok(operation: &str, status: u8) -> Result<(), String> {
    if status == 0 {
        Ok(())
    } else {
        Err(format!("{operation} failed with status {status}"))
    }
}
#[inline]
fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
