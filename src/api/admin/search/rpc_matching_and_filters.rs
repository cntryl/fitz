use super::{list, route_quad, AdminSearchResult, Candidate, Runtime, SearchOptions};
use std::collections::{BTreeMap, HashMap};

pub(crate) fn collect_rpc_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("rpc") {
        return;
    }

    collect_rpc_worker_candidates(runtime, options, candidates);
    collect_rpc_pending_candidates(runtime, options, candidates);
}

fn collect_rpc_worker_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    for worker in runtime.rpc_list_workers(options.realm.as_deref()) {
        let route_family = worker.route_family.to_string();
        let parsed = route_quad(&worker.route);
        if !options.matches_scope(
            Some(&route_family),
            Some(&worker.realm),
            parsed.map(|route| route.area),
            parsed.map(|route| route.resource),
            parsed.map(|route| route.operation),
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("session_id".to_string(), worker.session_id.clone());
        metadata.insert(
            "requests_handled".to_string(),
            worker.requests_handled.to_string(),
        );
        metadata.insert(
            "average_latency_ms".to_string(),
            worker.average_latency_ms.to_string(),
        );
        candidates.push(candidate(
            AdminSearchResult {
                id: format!("rpc:worker:{}:{}", worker.session_id, worker.route),
                domain: "rpc".to_string(),
                kind: "worker".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(worker.realm.clone()),
                area: parsed.map(|route| route.area.to_string()),
                resource: parsed.map(|route| route.resource.to_string()),
                operation: parsed.map(|route| route.operation.to_string()),
                title: worker.route.clone(),
                summary: format!(
                    "Session {}, average latency {}ms",
                    worker.session_id, worker.average_latency_ms
                ),
                health: Some(
                    if worker.average_latency_ms >= 100.0 {
                        "slow"
                    } else {
                        "live"
                    }
                    .to_string(),
                ),
                href: parsed.map_or_else(
                    || "/rpc".to_string(),
                    |route| resource_href("rpc", route.realm, route.area, route.resource),
                ),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", worker.realm),
                ("route", worker.route),
                ("session_id", worker.session_id),
            ],
        ));
    }
}

fn collect_rpc_pending_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    for request in runtime.rpc_list_pending(options.realm.as_deref()) {
        let route_family = request.route_family.to_string();
        let parsed = route_quad(&request.route);
        if !options.matches_scope(
            Some(&route_family),
            parsed.map(|route| route.realm),
            parsed.map(|route| route.area),
            parsed.map(|route| route.resource),
            parsed.map(|route| route.operation),
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("correlation_id".to_string(), request.correlation_id.clone());
        metadata.insert("age_seconds".to_string(), request.age_seconds.to_string());
        if let Some(worker_session_id) = &request.worker_session_id {
            metadata.insert("worker_session_id".to_string(), worker_session_id.clone());
        }
        candidates.push(candidate(
            AdminSearchResult {
                id: format!("rpc:pending:{}:{}", request.correlation_id, request.route),
                domain: "rpc".to_string(),
                kind: "pending_call".to_string(),
                route_family: Some(route_family.clone()),
                realm: parsed.map(|route| route.realm.to_string()),
                area: parsed.map(|route| route.area.to_string()),
                resource: parsed.map(|route| route.resource.to_string()),
                operation: parsed.map(|route| route.operation.to_string()),
                title: request.correlation_id.clone(),
                summary: format!("{} pending for {}s", request.route, request.age_seconds),
                health: Some(
                    if request.age_seconds >= 30 {
                        "stale"
                    } else {
                        "pending"
                    }
                    .to_string(),
                ),
                href: parsed.map_or_else(
                    || "/rpc".to_string(),
                    |route| resource_href("rpc", route.realm, route.area, route.resource),
                ),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("correlation_id", request.correlation_id),
                ("route", request.route),
                (
                    "worker_session_id",
                    request.worker_session_id.unwrap_or_default(),
                ),
            ],
        ));
    }
}

pub(crate) fn push_resource_candidate(
    domain: &str,
    kind: &str,
    resource: list::ResourceRef,
    route_family: Option<String>,
    candidates: &mut Vec<Candidate>,
) {
    candidates.push(candidate(
        AdminSearchResult {
            id: format!(
                "{}:{}:{}:{}:{}",
                domain, kind, resource.realm, resource.area, resource.resource
            ),
            domain: domain.to_string(),
            kind: kind.to_string(),
            route_family,
            realm: Some(resource.realm.clone()),
            area: Some(resource.area.clone()),
            resource: Some(resource.resource.clone()),
            operation: None,
            title: resource.resource.clone(),
            summary: format!("{}/{}/{}", resource.realm, resource.area, resource.resource),
            health: Some("observed".to_string()),
            href: resource_href(domain, &resource.realm, &resource.area, &resource.resource),
            matched_fields: Vec::new(),
            metadata: BTreeMap::new(),
        },
        vec![
            ("realm", resource.realm),
            ("area", resource.area),
            ("resource", resource.resource),
        ],
    ));
}

pub(crate) fn candidate(result: AdminSearchResult, fields: Vec<(&str, String)>) -> Candidate {
    Candidate {
        result,
        fields: fields
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect(),
    }
}

pub(crate) fn match_candidate(mut candidate: Candidate, query: &str) -> Option<AdminSearchResult> {
    let needle = query.trim().to_ascii_lowercase();
    if needle.is_empty() {
        candidate.result.matched_fields = Vec::new();
        return Some(candidate.result);
    }

    let mut matched_fields = candidate
        .fields
        .iter()
        .filter(|(_, value)| value.to_ascii_lowercase().contains(&needle))
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();

    if candidate
        .result
        .title
        .to_ascii_lowercase()
        .contains(&needle)
    {
        matched_fields.push("title".to_string());
    }
    if candidate
        .result
        .summary
        .to_ascii_lowercase()
        .contains(&needle)
    {
        matched_fields.push("summary".to_string());
    }

    if matched_fields.is_empty() {
        return None;
    }

    matched_fields.sort();
    matched_fields.dedup();
    candidate.result.matched_fields = matched_fields;
    Some(candidate.result)
}

pub(crate) fn parse_query_params(uri: &hyper::Uri) -> HashMap<String, String> {
    uri.query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn normalize_route_family_filter(value: &str) -> String {
    value
        .strip_prefix("family-")
        .unwrap_or(value)
        .trim()
        .to_string()
}

pub(crate) fn matches_optional_filter(filter: Option<&str>, value: Option<&str>) -> bool {
    filter.is_none_or(|filter| value.is_some_and(|value| value == filter))
}

pub(crate) fn scope_filter_matches(filter: Option<&str>, value: &str) -> bool {
    filter.is_none_or(|filter| filter == value)
}

pub(crate) fn resource_href(domain: &str, realm: &str, area: &str, resource: &str) -> String {
    format!(
        "/{}/{}/{}/{}",
        domain,
        encode_path_segment(realm),
        encode_path_segment(area),
        encode_path_segment(resource)
    )
}

pub(crate) fn domain_href(
    domain: &str,
    realm: &str,
    area: Option<&str>,
    resource: Option<&str>,
) -> String {
    match (area, resource) {
        (Some(area), Some(resource)) => resource_href(domain, realm, area, resource),
        (Some(area), None) => format!(
            "/{}?realm={}&area={}",
            domain,
            encode_query_value(realm),
            encode_query_value(area)
        ),
        _ => format!("/{}?realm={}", domain, encode_query_value(realm)),
    }
}

pub(crate) fn encode_path_segment(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

pub(crate) fn encode_query_value(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
