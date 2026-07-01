use super::{
    candidate, collect_lease_candidates, collect_notice_candidates, collect_queue_candidates,
    collect_rpc_candidates, collect_schedule_candidates, domain_href, list, match_candidate,
    matches_optional_filter, normalize_route_family_filter, parse_query_params,
    push_resource_candidate, resource_href, scope_filter_matches, Runtime,
};
use crate::api::admin::auth::{AdminPrincipal, AdminRouteFamilyAccess};
use crate::api::http::Response;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const DEFAULT_SEARCH_LIMIT: usize = 50;
const MAX_SEARCH_LIMIT: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminSearchResponse {
    pub query: String,
    pub route_family: Option<String>,
    pub domain: Option<String>,
    pub limit: usize,
    pub total: usize,
    pub truncated: bool,
    pub results: Vec<AdminSearchResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminSearchResult {
    pub id: String,
    pub domain: String,
    pub kind: String,
    pub route_family: Option<String>,
    pub realm: Option<String>,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub operation: Option<String>,
    pub title: String,
    pub summary: String,
    pub health: Option<String>,
    pub href: String,
    pub matched_fields: Vec<String>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub(crate) struct SearchOptions {
    pub(crate) query: String,
    pub(crate) route_family: Option<String>,
    pub(crate) domain: Option<String>,
    pub(crate) realm: Option<String>,
    pub(crate) area: Option<String>,
    pub(crate) resource: Option<String>,
    pub(crate) operation: Option<String>,
    pub(crate) limit: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub(crate) result: AdminSearchResult,
    pub(crate) fields: Vec<(String, String)>,
}

impl SearchOptions {
    fn from_uri(uri: &hyper::Uri) -> Result<Self, String> {
        let params = parse_query_params(uri);
        let limit = params
            .get("limit")
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|_| "Invalid limit query parameter".to_string())
                    .map(|limit| limit.clamp(1, MAX_SEARCH_LIMIT))
            })
            .transpose()?
            .unwrap_or(DEFAULT_SEARCH_LIMIT);

        Ok(Self {
            query: params.get("q").cloned().unwrap_or_default(),
            route_family: params
                .get("route_family")
                .or_else(|| params.get("routeFamily"))
                .map(|value| normalize_route_family_filter(value)),
            domain: params.get("domain").map(|value| value.to_ascii_lowercase()),
            realm: params.get("realm").cloned(),
            area: params.get("area").cloned(),
            resource: params.get("resource").cloned(),
            operation: params.get("operation").cloned(),
            limit,
        })
    }

    pub(crate) fn matches_domain(&self, domain: &str) -> bool {
        self.domain.as_deref().is_none_or(|filter| filter == domain)
    }

    pub(crate) fn matches_scope(
        &self,
        route_family: Option<&str>,
        realm: Option<&str>,
        area: Option<&str>,
        resource: Option<&str>,
        operation: Option<&str>,
    ) -> bool {
        if let Some(filter) = self.route_family.as_deref() {
            if route_family != Some(filter) {
                return false;
            }
        }
        if !matches_optional_filter(self.realm.as_deref(), realm) {
            return false;
        }
        if !matches_optional_filter(self.area.as_deref(), area) {
            return false;
        }
        if !matches_optional_filter(self.resource.as_deref(), resource) {
            return false;
        }
        if !matches_optional_filter(self.operation.as_deref(), operation) {
            return false;
        }
        true
    }
}

pub fn handle_search(uri: &hyper::Uri, runtime: &Runtime, principal: &AdminPrincipal) -> Response {
    let options = match SearchOptions::from_uri(uri) {
        Ok(options) => options,
        Err(message) => {
            return crate::api::admin::error_response(StatusCode::BAD_REQUEST, &message);
        }
    };

    if let Some(route_family) = options.route_family.as_deref() {
        if !principal.route_family_access.allows(route_family) {
            return crate::api::admin::error_response(
                StatusCode::FORBIDDEN,
                "Route family is not allowed for this admin session",
            );
        }
    }

    crate::api::admin::json_response(search_runtime(
        runtime,
        &options,
        &principal.route_family_access,
    ))
}

pub(crate) fn search_runtime(
    runtime: &Runtime,
    options: &SearchOptions,
    route_family_access: &AdminRouteFamilyAccess,
) -> AdminSearchResponse {
    let mut candidates = Vec::new();

    collect_session_candidates(runtime, options, &mut candidates);
    collect_kv_candidates(runtime, options, &mut candidates);
    collect_stream_candidates(runtime, options, &mut candidates);
    collect_queue_candidates(runtime, options, &mut candidates);
    collect_schedule_candidates(runtime, options, &mut candidates);
    collect_lease_candidates(runtime, options, &mut candidates);
    collect_notice_candidates(runtime, options, &mut candidates);
    collect_rpc_candidates(runtime, options, &mut candidates);

    let mut results = candidates
        .into_iter()
        .filter(|candidate| {
            route_family_is_visible(
                candidate.result.route_family.as_deref(),
                options,
                route_family_access,
            )
        })
        .filter_map(|candidate| match_candidate(candidate, &options.query))
        .collect::<Vec<_>>();
    results.sort_by(|left, right| {
        left.domain
            .cmp(&right.domain)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.title.cmp(&right.title))
    });

    let total = results.len();
    let truncated = total > options.limit;
    results.truncate(options.limit);

    AdminSearchResponse {
        query: options.query.clone(),
        route_family: options.route_family.clone(),
        domain: options.domain.clone(),
        limit: options.limit,
        total,
        truncated,
        results,
    }
}

pub(crate) fn route_family_is_visible(
    route_family: Option<&str>,
    options: &SearchOptions,
    route_family_access: &AdminRouteFamilyAccess,
) -> bool {
    if let Some(filter) = options.route_family.as_deref() {
        return route_family == Some(filter) && route_family_access.allows(filter);
    }

    if route_family_access.is_wildcard() {
        return true;
    }

    route_family.is_some_and(|route_family| route_family_access.allows(route_family))
}

pub(crate) fn collect_session_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("sessions") {
        return;
    }

    for session in runtime.list_sessions() {
        let route_family = session.route_family.to_string();
        if !options.matches_scope(Some(route_family.as_str()), None, None, None, None) {
            continue;
        }

        let title = if session.identity_value.is_empty() {
            format!("Session {}", session.session_id)
        } else {
            format!(
                "Session {} ({})",
                session.session_id, session.identity_value
            )
        };
        let summary = format!(
            "{} transport, idle {}s, {} received / {} sent",
            session.transport,
            session.idle_seconds,
            session.messages_received,
            session.messages_sent
        );
        let mut metadata = BTreeMap::new();
        metadata.insert("connected_at".to_string(), session.connected_at.clone());
        metadata.insert("remote_addr".to_string(), session.remote_addr.clone());

        candidates.push(candidate(
            AdminSearchResult {
                id: format!("session:{}:{}", route_family, session.session_id),
                domain: "sessions".to_string(),
                kind: "session".to_string(),
                route_family: Some(route_family.clone()),
                realm: None,
                area: None,
                resource: None,
                operation: None,
                title,
                summary,
                health: Some("live".to_string()),
                href: "/sessions".to_string(),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("session_id", session.session_id),
                ("route_family", route_family),
                ("subject", session.subject),
                ("identity_claim", session.identity_claim),
                ("identity_value", session.identity_value),
                ("transport", session.transport),
                ("remote_addr", session.remote_addr),
            ],
        ));
    }
}

pub(crate) fn collect_kv_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("kv") {
        return;
    }

    for resource in list::kv_resources(runtime, None) {
        if !options.matches_scope(
            None,
            Some(&resource.realm),
            Some(&resource.area),
            Some(&resource.resource),
            None,
        ) {
            continue;
        }
        push_resource_candidate("kv", "resource", resource, None, candidates);
    }

    for tx in runtime.kv_list_transactions(options.realm.as_deref()) {
        let route_family = tx.route_family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&tx.realm),
            Some(&tx.area),
            Some(&tx.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("tx_id".to_string(), tx.tx_id.to_string());
        metadata.insert("mode".to_string(), tx.mode.clone());
        metadata.insert(
            "operations_count".to_string(),
            tx.operations_count.to_string(),
        );
        metadata.insert("idle_seconds".to_string(), tx.idle_seconds.to_string());
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "kv:transaction:{}:{}:{}:{}:{}",
                    route_family, tx.realm, tx.area, tx.resource, tx.tx_id
                ),
                domain: "kv".to_string(),
                kind: "transaction".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(tx.realm.clone()),
                area: Some(tx.area.clone()),
                resource: Some(tx.resource.clone()),
                operation: None,
                title: format!("KV transaction {}", tx.tx_id),
                summary: format!("{} on {}/{}/{}", tx.mode, tx.realm, tx.area, tx.resource),
                health: Some(
                    if tx.idle_seconds > 300 {
                        "stale"
                    } else {
                        "live"
                    }
                    .to_string(),
                ),
                href: resource_href("kv", &tx.realm, &tx.area, &tx.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("tx_id", tx.tx_id.to_string()),
                ("realm", tx.realm),
                ("area", tx.area),
                ("resource", tx.resource),
                ("mode", tx.mode),
            ],
        ));
    }
}

pub(crate) fn collect_stream_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("stream") {
        return;
    }

    collect_stream_resource_candidates(runtime, options, candidates);
    collect_stream_area_watermark_candidates(runtime, options, candidates);
}

fn collect_stream_resource_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    for stream in runtime.stream_list_streams(options.realm.as_deref()) {
        let route_family = stream.route_family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&stream.realm),
            Some(&stream.area),
            Some(&stream.resource),
            None,
        ) {
            continue;
        }
        let lag = stream.offset.saturating_sub(stream.watermark);
        let mut metadata = BTreeMap::new();
        metadata.insert("offset".to_string(), stream.offset.to_string());
        metadata.insert("watermark".to_string(), stream.watermark.to_string());
        metadata.insert("lag".to_string(), lag.to_string());
        metadata.insert("size_bytes".to_string(), stream.size_bytes.to_string());
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "stream:resource:{}:{}:{}",
                    stream.realm, stream.area, stream.resource
                ),
                domain: "stream".to_string(),
                kind: "resource".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(stream.realm.clone()),
                area: Some(stream.area.clone()),
                resource: Some(stream.resource.clone()),
                operation: None,
                title: stream.resource.clone(),
                summary: format!(
                    "Offset {}, watermark {}, lag {}",
                    stream.offset, stream.watermark, lag
                ),
                health: Some(if lag > 0 { "lagging" } else { "caught_up" }.to_string()),
                href: resource_href("stream", &stream.realm, &stream.area, &stream.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", stream.realm),
                ("area", stream.area),
                ("resource", stream.resource),
                ("offset", stream.offset.to_string()),
                ("watermark", stream.watermark.to_string()),
            ],
        ));
    }
}

fn collect_stream_area_watermark_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    for detail in runtime.stream_list_area_watermark_details() {
        if !scope_filter_matches(options.realm.as_deref(), &detail.realm)
            || !scope_filter_matches(options.area.as_deref(), &detail.area)
        {
            continue;
        }
        for watermark in detail.family_watermarks {
            let route_family = watermark.family.to_string();
            if !options.matches_scope(
                Some(&route_family),
                Some(&detail.realm),
                Some(&detail.area),
                None,
                None,
            ) {
                continue;
            }
            let mut metadata = BTreeMap::new();
            metadata.insert("watermark".to_string(), watermark.watermark.to_string());
            metadata.insert(
                "resource_count".to_string(),
                detail.resource_count.to_string(),
            );
            candidates.push(candidate(
                AdminSearchResult {
                    id: format!(
                        "stream:area-watermark:{}:{}:{}",
                        route_family, detail.realm, detail.area
                    ),
                    domain: "stream".to_string(),
                    kind: "area_watermark".to_string(),
                    route_family: Some(route_family.clone()),
                    realm: Some(detail.realm.clone()),
                    area: Some(detail.area.clone()),
                    resource: None,
                    operation: None,
                    title: format!("{} area watermark", detail.area),
                    summary: format!("Family {} watermark {}", route_family, watermark.watermark),
                    health: Some("observed".to_string()),
                    href: domain_href("stream", &detail.realm, Some(&detail.area), None),
                    matched_fields: Vec::new(),
                    metadata,
                },
                vec![
                    ("route_family", route_family),
                    ("realm", detail.realm.clone()),
                    ("area", detail.area.clone()),
                    ("watermark", watermark.watermark.to_string()),
                ],
            ));
        }
    }
}
