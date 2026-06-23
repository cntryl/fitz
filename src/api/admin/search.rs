use crate::api::admin::auth::{AdminPrincipal, AdminRouteFamilyAccess};
use crate::api::admin::list;
use crate::api::http::Response;
use crate::boot::Runtime;
use crate::runtime::routing::{route_quad, route_triplet};
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::Arc;

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
struct SearchOptions {
    query: String,
    route_family: Option<String>,
    domain: Option<String>,
    realm: Option<String>,
    area: Option<String>,
    resource: Option<String>,
    operation: Option<String>,
    limit: usize,
}

#[derive(Debug, Clone)]
struct Candidate {
    result: AdminSearchResult,
    fields: Vec<(String, String)>,
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

    fn matches_domain(&self, domain: &str) -> bool {
        self.domain
            .as_deref()
            .map(|filter| filter == domain)
            .unwrap_or(true)
    }

    fn matches_scope(
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

pub async fn handle_search(
    uri: &hyper::Uri,
    runtime: Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Result<Response, Infallible> {
    let options = match SearchOptions::from_uri(uri) {
        Ok(options) => options,
        Err(message) => {
            return Ok(crate::api::admin::error_response(
                StatusCode::BAD_REQUEST,
                &message,
            ));
        }
    };

    if let Some(route_family) = options.route_family.as_deref() {
        if !principal.route_family_access.allows(route_family) {
            return Ok(crate::api::admin::error_response(
                StatusCode::FORBIDDEN,
                "Route family is not allowed for this admin session",
            ));
        }
    }

    crate::api::admin::json_response(search_runtime(
        runtime.as_ref(),
        &options,
        &principal.route_family_access,
    ))
}

fn search_runtime(
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

fn route_family_is_visible(
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

    route_family
        .map(|route_family| route_family_access.allows(route_family))
        .unwrap_or(false)
}

fn collect_session_candidates(
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

fn collect_kv_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("kv") {
        return;
    }

    for resource in list::kv_resources(runtime) {
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
        if !options.matches_scope(
            None,
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
                    "kv:transaction:{}:{}:{}:{}",
                    tx.realm, tx.area, tx.resource, tx.tx_id
                ),
                domain: "kv".to_string(),
                kind: "transaction".to_string(),
                route_family: None,
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
                ("tx_id", tx.tx_id.to_string()),
                ("realm", tx.realm),
                ("area", tx.area),
                ("resource", tx.resource),
                ("mode", tx.mode),
            ],
        ));
    }
}

fn collect_stream_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("stream") {
        return;
    }

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

fn collect_queue_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("queue") {
        return;
    }

    for queue in runtime.queue_list_queues(options.realm.as_deref()) {
        let route_family = queue.family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&queue.realm),
            Some(&queue.area),
            Some(&queue.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "messages_ready".to_string(),
            queue.messages_ready.to_string(),
        );
        metadata.insert(
            "messages_delayed".to_string(),
            queue.messages_delayed.to_string(),
        );
        metadata.insert(
            "messages_inflight".to_string(),
            queue.messages_inflight.to_string(),
        );
        metadata.insert(
            "messages_dead_lettered".to_string(),
            queue.messages_dead_lettered.to_string(),
        );
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "queue:resource:{}:{}:{}:{}",
                    route_family, queue.realm, queue.area, queue.resource
                ),
                domain: "queue".to_string(),
                kind: "resource".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(queue.realm.clone()),
                area: Some(queue.area.clone()),
                resource: Some(queue.resource.clone()),
                operation: None,
                title: queue.resource.clone(),
                summary: format!(
                    "{} ready, {} delayed, {} inflight, {} dead-lettered",
                    queue.messages_ready,
                    queue.messages_delayed,
                    queue.messages_inflight,
                    queue.messages_dead_lettered
                ),
                health: Some(
                    if queue.messages_dead_lettered > 0 {
                        "failing"
                    } else if queue.messages_ready + queue.messages_delayed > 0 {
                        "backlogged"
                    } else {
                        "healthy"
                    }
                    .to_string(),
                ),
                href: resource_href("queue", &queue.realm, &queue.area, &queue.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", queue.realm),
                ("area", queue.area),
                ("resource", queue.resource),
            ],
        ));
    }

    for inflight in runtime.queue_list_inflight(options.realm.as_deref()) {
        let route_family = inflight.family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&inflight.realm),
            Some(&inflight.area),
            Some(&inflight.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("message_id".to_string(), inflight.message_id.to_string());
        metadata.insert("session_id".to_string(), inflight.session_id.clone());
        metadata.insert("attempts".to_string(), inflight.attempts.to_string());
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "queue:inflight:{}:{}:{}:{}:{}",
                    route_family,
                    inflight.realm,
                    inflight.area,
                    inflight.resource,
                    inflight.message_id
                ),
                domain: "queue".to_string(),
                kind: "inflight_message".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(inflight.realm.clone()),
                area: Some(inflight.area.clone()),
                resource: Some(inflight.resource.clone()),
                operation: None,
                title: format!("Inflight message {}", inflight.message_id),
                summary: format!(
                    "Session {}, expires {}",
                    inflight.session_id, inflight.expires_at
                ),
                health: Some("inflight".to_string()),
                href: resource_href("queue", &inflight.realm, &inflight.area, &inflight.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("message_id", inflight.message_id.to_string()),
                ("session_id", inflight.session_id),
                ("realm", inflight.realm),
                ("area", inflight.area),
                ("resource", inflight.resource),
                ("inflight_token", inflight.inflight_token),
            ],
        ));
    }

    for message in runtime.queue_list_dead_letters(options.realm.as_deref()) {
        let route_family = message.family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&message.realm),
            Some(&message.area),
            Some(&message.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("message_id".to_string(), message.message_id.to_string());
        metadata.insert("attempts".to_string(), message.attempts.to_string());
        metadata.insert("reason".to_string(), message.reason.clone());
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "queue:dead-letter:{}:{}:{}:{}:{}",
                    route_family, message.realm, message.area, message.resource, message.message_id
                ),
                domain: "queue".to_string(),
                kind: "dead_letter".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(message.realm.clone()),
                area: Some(message.area.clone()),
                resource: Some(message.resource.clone()),
                operation: None,
                title: format!("Dead-letter message {}", message.message_id),
                summary: format!(
                    "{} attempt(s), reason: {}",
                    message.attempts, message.reason
                ),
                health: Some("failing".to_string()),
                href: resource_href("queue", &message.realm, &message.area, &message.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("message_id", message.message_id.to_string()),
                ("reason", message.reason),
                ("realm", message.realm),
                ("area", message.area),
                ("resource", message.resource),
            ],
        ));
    }
}

fn collect_schedule_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("schedule") {
        return;
    }

    for schedule in runtime.schedule_list_schedules(options.realm.as_deref()) {
        let route_family = schedule.route_family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&schedule.realm),
            Some(&schedule.area),
            Some(&schedule.resource),
            Some(&schedule.operation),
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("cron".to_string(), schedule.cron.clone());
        metadata.insert("next_run".to_string(), schedule.next_run.clone());
        metadata.insert(
            "executions_total".to_string(),
            schedule.executions_total.to_string(),
        );
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "schedule:{}:{}:{}:{}",
                    schedule.realm, schedule.area, schedule.resource, schedule.operation
                ),
                domain: "schedule".to_string(),
                kind: "schedule".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(schedule.realm.clone()),
                area: Some(schedule.area.clone()),
                resource: Some(schedule.resource.clone()),
                operation: Some(schedule.operation.clone()),
                title: format!("{}/{}", schedule.resource, schedule.operation),
                summary: format!("{}; next run {}", schedule.cron, schedule.next_run),
                health: Some(
                    if schedule.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    }
                    .to_string(),
                ),
                href: resource_href(
                    "schedule",
                    &schedule.realm,
                    &schedule.area,
                    &schedule.resource,
                ),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", schedule.realm),
                ("area", schedule.area),
                ("resource", schedule.resource),
                ("operation", schedule.operation),
                ("cron", schedule.cron),
                ("next_run", schedule.next_run),
            ],
        ));
    }
}

fn collect_lease_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("lease") {
        return;
    }

    for lease in runtime.lease_list_leases(options.realm.as_deref()) {
        let route_family = lease.route_family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&lease.realm),
            Some(&lease.area),
            Some(&lease.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "owner_session_id".to_string(),
            lease.owner_session_id.clone(),
        );
        metadata.insert("fencing_token".to_string(), lease.fencing_token.to_string());
        metadata.insert("renewals".to_string(), lease.renewals.to_string());
        candidates.push(candidate(
            AdminSearchResult {
                id: format!("lease:{}:{}:{}", lease.realm, lease.area, lease.resource),
                domain: "lease".to_string(),
                kind: "lease".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(lease.realm.clone()),
                area: Some(lease.area.clone()),
                resource: Some(lease.resource.clone()),
                operation: None,
                title: lease.resource.clone(),
                summary: format!(
                    "Owned by {} until {}",
                    lease.owner_session_id, lease.expires_at
                ),
                health: Some("owned".to_string()),
                href: resource_href("lease", &lease.realm, &lease.area, &lease.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", lease.realm),
                ("area", lease.area),
                ("resource", lease.resource),
                ("owner_session_id", lease.owner_session_id),
                ("fencing_token", lease.fencing_token.to_string()),
            ],
        ));
    }
}

fn collect_notice_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("notice") {
        return;
    }

    for subscription in runtime.notice_list_subscriptions(options.realm.as_deref(), None) {
        let route_family = subscription.route_family.to_string();
        let parsed = route_triplet(&subscription.pattern);
        if !options.matches_scope(
            Some(&route_family),
            Some(&subscription.realm),
            parsed.map(|route| route.area),
            parsed.map(|route| route.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "subscription_id".to_string(),
            subscription.subscription_id.to_string(),
        );
        metadata.insert("session_id".to_string(), subscription.session_id.clone());
        metadata.insert(
            "notifications_received".to_string(),
            subscription.notifications_received.to_string(),
        );
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "notice:subscription:{}:{}",
                    subscription.session_id, subscription.subscription_id
                ),
                domain: "notice".to_string(),
                kind: "subscription".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(subscription.realm.clone()),
                area: parsed.map(|route| route.area.to_string()),
                resource: parsed.map(|route| route.resource.to_string()),
                operation: None,
                title: subscription.pattern.clone(),
                summary: format!(
                    "Session {} active since {}",
                    subscription.session_id, subscription.created_at
                ),
                health: Some("live".to_string()),
                href: parsed
                    .map(|route| resource_href("notice", route.realm, route.area, route.resource))
                    .unwrap_or_else(|| "/notice".to_string()),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", subscription.realm),
                ("pattern", subscription.pattern),
                ("session_id", subscription.session_id),
                ("subscription_id", subscription.subscription_id.to_string()),
            ],
        ));
    }

    for route in runtime.notice_list_routes(options.realm.as_deref()) {
        let route_family = route.route_family.to_string();
        let parsed = route_triplet(&route.route);
        if !options.matches_scope(
            Some(&route_family),
            parsed.map(|value| value.realm),
            parsed.map(|value| value.area),
            parsed.map(|value| value.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("subscribers".to_string(), route.subscribers.to_string());
        metadata.insert(
            "publishes_total".to_string(),
            route.publishes_total.to_string(),
        );
        candidates.push(candidate(
            AdminSearchResult {
                id: format!("notice:route:{}", route.route),
                domain: "notice".to_string(),
                kind: "route".to_string(),
                route_family: Some(route_family.clone()),
                realm: parsed.map(|value| value.realm.to_string()),
                area: parsed.map(|value| value.area.to_string()),
                resource: parsed.map(|value| value.resource.to_string()),
                operation: None,
                title: route.route.clone(),
                summary: format!(
                    "{} subscriber(s), {} publishes",
                    route.subscribers, route.publishes_total
                ),
                health: Some(
                    if route.subscribers > 0 {
                        "observed"
                    } else {
                        "quiet"
                    }
                    .to_string(),
                ),
                href: parsed
                    .map(|value| resource_href("notice", value.realm, value.area, value.resource))
                    .unwrap_or_else(|| "/notice".to_string()),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("route", route.route),
                ("subscribers", route.subscribers.to_string()),
                ("publishes_total", route.publishes_total.to_string()),
            ],
        ));
    }
}

fn collect_rpc_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("rpc") {
        return;
    }

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
                href: parsed
                    .map(|route| resource_href("rpc", route.realm, route.area, route.resource))
                    .unwrap_or_else(|| "/rpc".to_string()),
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
                href: parsed
                    .map(|route| resource_href("rpc", route.realm, route.area, route.resource))
                    .unwrap_or_else(|| "/rpc".to_string()),
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

fn push_resource_candidate(
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

fn candidate(result: AdminSearchResult, fields: Vec<(&str, String)>) -> Candidate {
    Candidate {
        result,
        fields: fields
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect(),
    }
}

fn match_candidate(mut candidate: Candidate, query: &str) -> Option<AdminSearchResult> {
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

fn parse_query_params(uri: &hyper::Uri) -> HashMap<String, String> {
    uri.query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_route_family_filter(value: &str) -> String {
    value
        .strip_prefix("family-")
        .unwrap_or(value)
        .trim()
        .to_string()
}

fn matches_optional_filter(filter: Option<&str>, value: Option<&str>) -> bool {
    filter
        .map(|filter| value.map(|value| value == filter).unwrap_or(false))
        .unwrap_or(true)
}

fn scope_filter_matches(filter: Option<&str>, value: &str) -> bool {
    filter.map(|filter| filter == value).unwrap_or(true)
}

fn resource_href(domain: &str, realm: &str, area: &str, resource: &str) -> String {
    format!(
        "/{}/{}/{}/{}",
        domain,
        encode_path_segment(realm),
        encode_path_segment(area),
        encode_path_segment(resource)
    )
}

fn domain_href(domain: &str, realm: &str, area: Option<&str>, resource: Option<&str>) -> String {
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

fn encode_path_segment(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn encode_query_value(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::admin::read_model::AdminReadModel;
    use crate::api::admin::{
        QueueDeadLetter, QueueDeadLetterSnapshot, QueueInfo, QueueInfoSnapshot, RpcPendingRequest,
        ScheduleInfo,
    };
    use crate::boot::Runtime;
    use crate::runtime::Router;

    fn runtime_with_read_model(read_model: Arc<AdminReadModel>) -> Arc<Runtime> {
        Arc::new(Runtime::with_admin_read_model(
            Arc::new(Router::new()),
            read_model,
        ))
    }

    fn wildcard_access() -> AdminRouteFamilyAccess {
        AdminRouteFamilyAccess::wildcard()
    }

    fn explicit_access(route_families: &[&str]) -> AdminRouteFamilyAccess {
        AdminRouteFamilyAccess::Explicit(
            route_families
                .iter()
                .map(|route_family| route_family.to_string())
                .collect(),
        )
    }

    fn queue_snapshot(family: u64, realm: &str, resource: &str) -> QueueInfo {
        QueueInfo::snapshot(QueueInfoSnapshot {
            family,
            realm,
            area: "payments",
            resource,
            messages_ready: 4,
            messages_delayed: 0,
            messages_inflight: 0,
            messages_dead_lettered: 0,
            messages_total: 4,
            oldest_message_age_seconds: 5,
            oldest_backlog_age_seconds: 5,
            backlog_age_buckets: Default::default(),
            delay_age_buckets: Default::default(),
        })
    }

    #[test]
    fn should_search_queue_dead_letters_by_message_id() {
        // Arrange
        let read_model = AdminReadModel::new();
        read_model.replace_queue_dead_letters(vec![QueueDeadLetter::snapshot(
            QueueDeadLetterSnapshot {
                message_id: 42,
                family: 2,
                realm: "billing",
                area: "payments",
                resource: "settlement",
                dead_lettered_at: "2026-06-23T00:00:00Z",
                attempts: 3,
                reason: "timeout",
            },
        )]);
        let runtime = runtime_with_read_model(read_model);
        let options = SearchOptions {
            query: "42".to_string(),
            route_family: Some("2".to_string()),
            domain: Some("queue".to_string()),
            realm: None,
            area: None,
            resource: None,
            operation: None,
            limit: 50,
        };

        // Act
        let response = search_runtime(runtime.as_ref(), &options, &wildcard_access());

        // Assert
        assert_eq!(response.total, 1);
        assert_eq!(response.results[0].kind, "dead_letter");
        assert_eq!(response.results[0].route_family.as_deref(), Some("2"));
        assert!(response.results[0]
            .matched_fields
            .contains(&"message_id".to_string()));
    }

    #[test]
    fn should_filter_search_given_domain_realm_filters() {
        // Arrange
        let read_model = AdminReadModel::new();
        read_model.replace_queues(vec![queue_snapshot(1, "billing", "settlement")]);
        read_model.upsert_schedule(ScheduleInfo::enabled_snapshot(
            1,
            "ops".to_string(),
            "jobs".to_string(),
            "cleanup".to_string(),
            "run".to_string(),
            "0 * * * *".to_string(),
            "2026-06-23T00:00:00Z",
        ));
        let runtime = runtime_with_read_model(read_model);
        let options = SearchOptions {
            query: String::new(),
            route_family: None,
            domain: Some("queue".to_string()),
            realm: Some("billing".to_string()),
            area: None,
            resource: None,
            operation: None,
            limit: 50,
        };

        // Act
        let response = search_runtime(runtime.as_ref(), &options, &wildcard_access());

        // Assert
        assert_eq!(response.total, 1);
        assert_eq!(response.results[0].domain, "queue");
        assert_eq!(response.results[0].realm.as_deref(), Some("billing"));
    }

    #[test]
    fn should_search_rpc_pending_by_correlation_id() {
        // Arrange
        let read_model = AdminReadModel::new();
        read_model.replace_rpc_pending(vec![RpcPendingRequest::snapshot(
            1,
            "corr-123".to_string(),
            "rpc://billing/payments/settlement/run",
            "2026-06-23T00:00:00Z",
            7,
            None,
        )]);
        let runtime = runtime_with_read_model(read_model);
        let options = SearchOptions {
            query: "corr-123".to_string(),
            route_family: None,
            domain: Some("rpc".to_string()),
            realm: None,
            area: None,
            resource: None,
            operation: None,
            limit: 50,
        };

        // Act
        let response = search_runtime(runtime.as_ref(), &options, &wildcard_access());

        // Assert
        assert_eq!(response.total, 1);
        assert_eq!(response.results[0].operation.as_deref(), Some("run"));
    }

    #[test]
    fn should_limit_unfiltered_search_to_explicit_route_family_access() {
        // Arrange
        let read_model = AdminReadModel::new();
        read_model.replace_queues(vec![
            queue_snapshot(1, "billing", "settlement"),
            queue_snapshot(2, "billing", "settlement"),
        ]);
        let runtime = runtime_with_read_model(read_model);
        let options = SearchOptions {
            query: String::new(),
            route_family: None,
            domain: Some("queue".to_string()),
            realm: None,
            area: None,
            resource: None,
            operation: None,
            limit: 50,
        };

        // Act
        let response = search_runtime(runtime.as_ref(), &options, &explicit_access(&["1"]));

        // Assert
        assert_eq!(response.total, 1);
        assert_eq!(response.results[0].route_family.as_deref(), Some("1"));
    }

    #[test]
    fn should_hide_unknown_route_family_candidates_from_explicit_access() {
        // Arrange
        let read_model = AdminReadModel::new();
        read_model.replace_queues(vec![queue_snapshot(1, "billing", "settlement")]);
        read_model.upsert_schedule(ScheduleInfo::enabled_snapshot(
            2,
            "ops".to_string(),
            "jobs".to_string(),
            "cleanup".to_string(),
            "run".to_string(),
            "0 * * * *".to_string(),
            "2026-06-23T00:00:00Z",
        ));
        let runtime = runtime_with_read_model(read_model);
        let options = SearchOptions {
            query: String::new(),
            route_family: None,
            domain: None,
            realm: None,
            area: None,
            resource: None,
            operation: None,
            limit: 50,
        };

        // Act
        let response = search_runtime(runtime.as_ref(), &options, &explicit_access(&["1"]));

        // Assert
        assert_eq!(response.total, 1);
        assert!(response
            .results
            .iter()
            .all(|result| result.route_family.as_deref() == Some("1")));
    }

    #[test]
    fn should_hide_unknown_route_family_candidates_from_explicit_route_filter() {
        // Arrange
        let read_model = AdminReadModel::new();
        read_model.replace_queues(vec![queue_snapshot(1, "billing", "settlement")]);
        read_model.upsert_schedule(ScheduleInfo::enabled_snapshot(
            2,
            "billing".to_string(),
            "payments".to_string(),
            "settlement".to_string(),
            "run".to_string(),
            "0 * * * *".to_string(),
            "2026-06-23T00:00:00Z",
        ));
        let runtime = runtime_with_read_model(read_model);
        let options = SearchOptions {
            query: String::new(),
            route_family: Some("1".to_string()),
            domain: None,
            realm: None,
            area: None,
            resource: None,
            operation: None,
            limit: 50,
        };

        // Act
        let response = search_runtime(runtime.as_ref(), &options, &explicit_access(&["1"]));

        // Assert
        assert_eq!(response.total, 1);
        assert_eq!(response.results[0].domain, "queue");
        assert_eq!(response.results[0].route_family.as_deref(), Some("1"));
    }

    #[test]
    fn should_keep_unknown_route_family_candidates_visible_to_wildcard_access() {
        // Arrange
        let read_model = AdminReadModel::new();
        read_model.upsert_schedule(ScheduleInfo::enabled_snapshot(
            2,
            "ops".to_string(),
            "jobs".to_string(),
            "cleanup".to_string(),
            "run".to_string(),
            "0 * * * *".to_string(),
            "2026-06-23T00:00:00Z",
        ));
        let runtime = runtime_with_read_model(read_model);
        let options = SearchOptions {
            query: String::new(),
            route_family: None,
            domain: Some("schedule".to_string()),
            realm: None,
            area: None,
            resource: None,
            operation: None,
            limit: 50,
        };

        // Act
        let response = search_runtime(runtime.as_ref(), &options, &wildcard_access());

        // Assert
        assert_eq!(response.total, 1);
        assert_eq!(response.results[0].route_family.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn should_reject_disallowed_route_family_filter() {
        // Arrange
        let runtime = runtime_with_read_model(AdminReadModel::new());
        let uri = "/api/v1/search?route_family=2".parse().unwrap();
        let principal = AdminPrincipal {
            username: "admin".to_string(),
            route_family_access: explicit_access(&["1"]),
        };

        // Act
        let response = handle_search(&uri, runtime, &principal).await.unwrap();

        // Assert
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
