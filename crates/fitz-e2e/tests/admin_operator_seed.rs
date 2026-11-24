use fitz::api::admin::{
    KvCommittedValueResponse, LeaseSearchResponse, NoticeDeliveryObservationList,
    QueueInflightList, RpcCallObservationList, ScheduleExecutionObservationList,
    ScheduleMissedObservationList, StreamRecordsResponse,
};
use fitz_e2e::testkit::{seed_operator_console, OperatorSeedFamily, TestServer};
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Deserialize};
use serial_test::serial;
use std::collections::BTreeSet;
use std::time::Duration;

const MAX_ADMIN_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
struct SearchResponse {
    results: Vec<SearchHit>,
}

#[derive(Deserialize)]
struct SearchHit {
    route_family: Option<String>,
}

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

#[tokio::test]
#[serial]
async fn should_expose_seeded_operator_console_state_through_real_admin_apis() {
    // Arrange
    let _admin_mode = EnvGuard::set("FITZ_ADMIN_AUTH_MODE", "open");
    let server = TestServer::start_with_auth_route_families(
        vec![1, 2],
        [("realm-alpha", 1), ("realm-beta", 2)],
    )
    .await
    .expect("start multi-family broker");
    let seed = seed_operator_console(
        &server,
        &[
            OperatorSeedFamily::new(1, "realm-alpha"),
            OperatorSeedFamily::new(2, "realm-beta"),
        ],
    )
    .await
    .expect("seed operator data");
    wait_for_seed_visibility(&server, seed.families.len()).await;
    let seeded_queue_scopes = seed
        .families
        .iter()
        .map(|family| {
            (
                family.route_family,
                family.realm.as_str(),
                family.area.as_str(),
            )
        })
        .collect::<Vec<_>>();
    wait_for_queue_projection(&server, &seeded_queue_scopes).await;
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .expect("create loopback admin client");
    let first = &seed.families[0];
    let second = &seed.families[1];

    // Act
    let search_first: SearchResponse = get_json(
        &client,
        &server,
        "/api/v1/search",
        &[
            ("domain", "queue".to_string()),
            ("q", "settlement".to_string()),
            ("route_family", first.route_family.to_string()),
        ],
    )
    .await;
    let search_second: SearchResponse = get_json(
        &client,
        &server,
        "/api/v1/search",
        &[
            ("domain", "queue".to_string()),
            ("q", "settlement".to_string()),
            ("route_family", second.route_family.to_string()),
        ],
    )
    .await;
    let search_all: SearchResponse = get_json(
        &client,
        &server,
        "/api/v1/search",
        &[
            ("domain", "queue".to_string()),
            ("q", "settlement".to_string()),
        ],
    )
    .await;
    let kv_value: KvCommittedValueResponse = get_json(
        &client,
        &server,
        &format!(
            "/api/v1/{}/kv/realms/{}/areas/{}/resources/accounts/value",
            first.route_family, first.realm, first.area
        ),
        &[
            ("route_family", first.route_family.to_string()),
            ("key", first.kv_key.clone()),
        ],
    )
    .await;
    let wrong_family_value: KvCommittedValueResponse = get_json(
        &client,
        &server,
        &format!(
            "/api/v1/{}/kv/realms/{}/areas/{}/resources/accounts/value",
            second.route_family, first.realm, first.area
        ),
        &[("key", first.kv_key.clone())],
    )
    .await;
    let stream_records: StreamRecordsResponse = get_json(
        &client,
        &server,
        &format!(
            "/api/v1/{}/stream/realms/{}/areas/{}/resources/events/records",
            first.route_family, first.realm, first.area
        ),
        &[
            ("route_family", first.route_family.to_string()),
            ("limit", "10".to_string()),
        ],
    )
    .await;
    let queue_inflight: QueueInflightList = get_json(
        &client,
        &server,
        &format!(
            "/api/v1/{}/queue/realms/{}/areas/{}/resources/settlement/inflight",
            first.route_family, first.realm, first.area
        ),
        &[("family", first.route_family.to_string())],
    )
    .await;
    let schedule_executions: ScheduleExecutionObservationList = get_json(
        &client,
        &server,
        &format!(
            "/api/v1/{}/schedule/realms/{}/areas/{}/resources/reconcile/executions",
            first.route_family, first.realm, first.area
        ),
        &[
            ("route_family", first.route_family.to_string()),
            ("limit", "10".to_string()),
        ],
    )
    .await;
    let schedule_missed: ScheduleMissedObservationList = get_json(
        &client,
        &server,
        &format!("/api/v1/{}/schedule/missed", first.route_family),
        &[
            ("route_family", first.route_family.to_string()),
            ("realm", first.realm.clone()),
        ],
    )
    .await;
    let lease_search: LeaseSearchResponse = get_json(
        &client,
        &server,
        &format!("/api/v1/{}/lease/search", first.route_family),
        &[
            ("route_family", first.route_family.to_string()),
            ("realm", first.realm.clone()),
            ("area", first.area.clone()),
            ("resource", "settlement".to_string()),
        ],
    )
    .await;
    let notice_deliveries: NoticeDeliveryObservationList = get_json(
        &client,
        &server,
        &format!("/api/v1/{}/notice/deliveries", first.route_family),
        &[
            ("route_family", first.route_family.to_string()),
            ("realm", first.realm.clone()),
            ("area", first.area.clone()),
            ("resource", "events".to_string()),
        ],
    )
    .await;
    let rpc_calls: RpcCallObservationList = get_json(
        &client,
        &server,
        &format!("/api/v1/{}/rpc/calls", first.route_family),
        &[
            ("route_family", first.route_family.to_string()),
            ("realm", first.realm.clone()),
            ("area", first.area.clone()),
            ("resource", "profile".to_string()),
            ("operation", "sync".to_string()),
        ],
    )
    .await;

    // Assert
    assert_search_route_family(&search_first, first.route_family);
    assert_search_route_family(&search_second, second.route_family);
    assert_search_contains_families(&search_all, &[first.route_family, second.route_family]);
    assert!(kv_value.found);
    assert_eq!(
        kv_value.value.and_then(|value| value.utf8).as_deref(),
        Some(first.kv_value.as_str())
    );
    assert!(!wrong_family_value.found);
    assert_family_rows(
        stream_records
            .records
            .iter()
            .map(|record| record.route_family),
        first.route_family,
    );
    assert_family_rows(
        queue_inflight.inflight.iter().map(|item| item.family),
        first.route_family,
    );
    assert_family_rows(
        schedule_executions
            .observations
            .iter()
            .map(|item| item.route_family),
        first.route_family,
    );
    assert_eq!(schedule_missed.route_family, u64::from(first.route_family));
    assert_lease_contention(&lease_search, first.route_family);
    assert_notice_delivery(&notice_deliveries, first.route_family);
    assert_rpc_calls(&rpc_calls, first.route_family);

    seed.close().await.expect("close seed clients");
    server
        .wait_for_session_count(0)
        .await
        .expect("seed sessions closed");
    server.shutdown().await.expect("shutdown broker");
}

async fn wait_for_seed_visibility(server: &TestServer, family_count: usize) {
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if server.runtime.queue_inflight_active() >= family_count
                && server.runtime.schedule_active() >= family_count
                && server.runtime.lease_active() >= family_count
                && server.runtime.lease_waiter_depth() >= family_count
                && server.runtime.notice_list_subscriptions(None, None).len() >= family_count
                && server.runtime.rpc_workers_registered() >= family_count
                && server.runtime.rpc_requests_pending() >= family_count
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "seed visibility: queue_inflight={} schedule_active={} lease_active={} lease_waiters={} notice_subscriptions={} rpc_workers={} rpc_pending={}",
        server.runtime.queue_inflight_active(),
        server.runtime.schedule_active(),
        server.runtime.lease_active(),
        server.runtime.lease_waiter_depth(),
        server.runtime.notice_list_subscriptions(None, None).len(),
        server.runtime.rpc_workers_registered(),
        server.runtime.rpc_requests_pending(),
    );
}

async fn wait_for_queue_projection(server: &TestServer, families: &[(u32, &str, &str)]) {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let rows = server.runtime.admin_read_model().queues(None);
            if families.iter().all(|(route_family, realm, area)| {
                rows.iter().any(|row| {
                    row.family == u64::from(*route_family)
                        && row.realm == *realm
                        && row.area == *area
                        && row.resource == "settlement"
                })
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "queue projection did not publish all seeded resources within 30 seconds: published={:?}",
        server.runtime.admin_read_model().queues(None),
    );
}

async fn get_json<T: DeserializeOwned>(
    client: &Client,
    server: &TestServer,
    path: &str,
    query: &[(&str, String)],
) -> T {
    let mut url = Url::parse(&format!("http://{}{}", server.ws_addr, path)).expect("admin url");
    url.query_pairs_mut().extend_pairs(query);
    let mut response = client
        .get(url)
        .send()
        .await
        .expect("admin request")
        .error_for_status()
        .expect("successful admin response");
    assert!(
        response.status().is_success(),
        "expected success from {path}"
    );

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.expect("admin response body") {
        assert!(
            chunk.len() <= MAX_ADMIN_RESPONSE_BYTES - body.len(),
            "admin response exceeds the test limit for {path}"
        );
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body)
        .unwrap_or_else(|error| panic!("invalid admin JSON for {path}: {error}"))
}

fn assert_search_route_family(payload: &SearchResponse, route_family: u32) {
    let expected = route_family.to_string();
    assert!(
        !payload.results.is_empty(),
        "expected search results for {expected}"
    );
    assert!(payload
        .results
        .iter()
        .all(|result| result.route_family.as_deref() == Some(expected.as_str())));
}

fn assert_search_contains_families(payload: &SearchResponse, route_families: &[u32]) {
    let actual = payload
        .results
        .iter()
        .filter_map(|result| result.route_family.as_deref())
        .collect::<BTreeSet<_>>();
    for route_family in route_families {
        let expected = route_family.to_string();
        assert!(
            actual.contains(expected.as_str()),
            "expected search to include route family {expected}"
        );
    }
}

fn assert_family_rows(families: impl Iterator<Item = u64>, route_family: u32) {
    let mut families = families.peekable();
    assert!(
        families.peek().is_some(),
        "expected at least one family row"
    );
    assert!(families.all(|family| family == u64::from(route_family)));
}

fn assert_lease_contention(payload: &LeaseSearchResponse, route_family: u32) {
    assert!(payload.items.iter().any(|item| {
        item.route_family == u64::from(route_family)
            && item.state == "owned_with_waiters"
            && item.pending_waiters > 0
    }));
}

fn assert_notice_delivery(payload: &NoticeDeliveryObservationList, route_family: u32) {
    assert!(payload.observations.iter().any(|observation| {
        observation.route_family == u64::from(route_family)
            && observation.status == "active_subscription"
    }));
}

fn assert_rpc_calls(payload: &RpcCallObservationList, route_family: u32) {
    assert_family_rows(
        payload
            .observations
            .iter()
            .map(|observation| observation.route_family),
        route_family,
    );
    assert!(payload
        .observations
        .iter()
        .any(|observation| observation.state == "worker_registered"));
    assert!(payload
        .observations
        .iter()
        .any(|observation| observation.state == "pending"));
}
