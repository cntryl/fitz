use fitz::testkit::{seed_operator_console, OperatorSeedFamily, TestServer};
use reqwest::Url;
use serde_json::Value;
use serial_test::serial;
use std::collections::BTreeSet;
use std::time::Duration;

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
    let first = &seed.families[0];
    let second = &seed.families[1];

    // Act
    let search_first = get_json(
        &server,
        "/api/v1/search",
        &[
            ("domain", "queue".to_string()),
            ("q", "settlement".to_string()),
            ("route_family", first.route_family.to_string()),
        ],
    )
    .await;
    let search_second = get_json(
        &server,
        "/api/v1/search",
        &[
            ("domain", "queue".to_string()),
            ("q", "settlement".to_string()),
            ("route_family", second.route_family.to_string()),
        ],
    )
    .await;
    let search_all = get_json(
        &server,
        "/api/v1/search",
        &[
            ("domain", "queue".to_string()),
            ("q", "settlement".to_string()),
        ],
    )
    .await;
    let kv_value = get_json(
        &server,
        &format!(
            "/api/v1/kv/realms/{}/areas/{}/resources/accounts/value",
            first.realm, first.area
        ),
        &[
            ("route_family", first.route_family.to_string()),
            ("key", first.kv_key.clone()),
        ],
    )
    .await;
    let wrong_family_value = get_json(
        &server,
        &format!(
            "/api/v1/kv/realms/{}/areas/{}/resources/accounts/value",
            first.realm, first.area
        ),
        &[
            ("route_family", second.route_family.to_string()),
            ("key", first.kv_key.clone()),
        ],
    )
    .await;
    let stream_records = get_json(
        &server,
        &format!(
            "/api/v1/stream/realms/{}/areas/{}/resources/events/records",
            first.realm, first.area
        ),
        &[
            ("route_family", first.route_family.to_string()),
            ("limit", "10".to_string()),
        ],
    )
    .await;
    let queue_inflight = get_json(
        &server,
        &format!(
            "/api/v1/queue/realms/{}/areas/{}/resources/settlement/inflight",
            first.realm, first.area
        ),
        &[("family", first.route_family.to_string())],
    )
    .await;
    let schedule_executions = get_json(
        &server,
        &format!(
            "/api/v1/schedule/realms/{}/areas/{}/resources/reconcile/executions",
            first.realm, first.area
        ),
        &[
            ("route_family", first.route_family.to_string()),
            ("limit", "10".to_string()),
        ],
    )
    .await;
    let schedule_missed = get_json(
        &server,
        "/api/v1/schedule/missed",
        &[
            ("route_family", first.route_family.to_string()),
            ("realm", first.realm.clone()),
        ],
    )
    .await;
    let lease_search = get_json(
        &server,
        "/api/v1/lease/search",
        &[
            ("route_family", first.route_family.to_string()),
            ("realm", first.realm.clone()),
            ("area", first.area.clone()),
            ("resource", "settlement".to_string()),
        ],
    )
    .await;
    let notice_deliveries = get_json(
        &server,
        "/api/v1/notice/deliveries",
        &[
            ("route_family", first.route_family.to_string()),
            ("realm", first.realm.clone()),
            ("area", first.area.clone()),
            ("resource", "events".to_string()),
        ],
    )
    .await;
    let rpc_calls = get_json(
        &server,
        "/api/v1/rpc/calls",
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
    assert_eq!(kv_value["found"], true);
    assert_eq!(kv_value["value"]["utf8"], first.kv_value);
    assert_eq!(wrong_family_value["found"], false);
    assert_nonempty_family_array(
        &stream_records,
        "records",
        "route_family",
        first.route_family,
    );
    assert_nonempty_family_array(&queue_inflight, "inflight", "family", first.route_family);
    assert_nonempty_family_array(
        &schedule_executions,
        "observations",
        "route_family",
        first.route_family,
    );
    assert_eq!(schedule_missed["route_family"], first.route_family);
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

async fn get_json(server: &TestServer, path: &str, query: &[(&str, String)]) -> Value {
    let mut url = Url::parse(&format!("http://{}{}", server.ws_addr, path)).expect("admin url");
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in query {
            pairs.append_pair(key, value);
        }
    }
    let response = reqwest::get(url).await.expect("admin request");
    let status = response.status();
    let body = response.text().await.expect("admin response body");
    assert!(status.is_success(), "admin request failed: {status} {body}");
    serde_json::from_str(&body).expect("admin json")
}

fn assert_search_route_family(payload: &Value, route_family: u32) {
    let expected = route_family.to_string();
    let results = payload["results"].as_array().expect("search results");
    assert!(
        !results.is_empty(),
        "expected search results for {expected}"
    );
    assert!(results.iter().all(|result| {
        result["route_family"]
            .as_str()
            .is_some_and(|value| value == expected)
    }));
}

fn assert_search_contains_families(payload: &Value, route_families: &[u32]) {
    let actual = payload["results"]
        .as_array()
        .expect("search results")
        .iter()
        .filter_map(|result| result["route_family"].as_str())
        .collect::<BTreeSet<_>>();
    for route_family in route_families {
        let expected = route_family.to_string();
        assert!(
            actual.contains(expected.as_str()),
            "expected search to include route family {expected}"
        );
    }
}

fn assert_nonempty_family_array(
    payload: &Value,
    array_key: &str,
    family_key: &str,
    route_family: u32,
) {
    let items = payload[array_key].as_array().expect("items array");
    assert!(!items.is_empty(), "expected nonempty {array_key}");
    assert!(items
        .iter()
        .all(|item| item[family_key].as_u64() == Some(u64::from(route_family))));
}

fn assert_lease_contention(payload: &Value, route_family: u32) {
    let items = payload["items"].as_array().expect("lease items");
    assert!(!items.is_empty(), "expected lease search items");
    assert!(items.iter().any(|item| {
        item["route_family"].as_u64() == Some(u64::from(route_family))
            && item["state"] == "owned_with_waiters"
            && item["pending_waiters"].as_u64().unwrap_or_default() > 0
    }));
}

fn assert_notice_delivery(payload: &Value, route_family: u32) {
    let observations = payload["observations"]
        .as_array()
        .expect("notice observations");
    assert!(!observations.is_empty(), "expected notice observations");
    assert!(observations.iter().any(|observation| {
        observation["route_family"].as_u64() == Some(u64::from(route_family))
            && observation["status"] == "active_subscription"
    }));
}

fn assert_rpc_calls(payload: &Value, route_family: u32) {
    let observations = payload["observations"]
        .as_array()
        .expect("rpc observations");
    assert!(!observations.is_empty(), "expected rpc observations");
    assert!(observations.iter().all(|observation| {
        observation["route_family"].as_u64() == Some(u64::from(route_family))
    }));
    assert!(observations
        .iter()
        .any(|observation| observation["state"] == "worker_registered"));
    assert!(observations
        .iter()
        .any(|observation| observation["state"] == "pending"));
}
