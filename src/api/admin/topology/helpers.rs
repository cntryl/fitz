use crate::api::admin::troubleshooting;
use crate::runtime::routing::{route_quad, route_triplet};
use std::cmp::Ordering;

use super::types::{
    TopologyConnection, TopologyConnectionBuilder, TopologyConnectionKind, TopologyCounter,
    TopologyLane, TopologyScope, TopologyScopedResource, TopologyState,
};

const TOP_RESOURCE_LIMIT: usize = 5;

pub(super) fn counter(key: &str, label: &str, value: impl Into<f64>) -> TopologyCounter {
    TopologyCounter {
        key: key.to_string(),
        label: label.to_string(),
        value: value.into(),
    }
}

fn resource_id(domain: &str, scope: &TopologyScope) -> String {
    [
        Some(domain.to_string()),
        scope.route_family.map(|family| family.to_string()),
        scope.realm.clone(),
        scope.area.clone(),
        scope.resource.clone(),
        scope.operation.clone(),
        scope.route.clone(),
        scope.pattern.clone(),
        scope.session_id.clone(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(":")
}

pub(super) fn scoped_resource(
    domain: &str,
    label: String,
    state: TopologyState,
    scope: TopologyScope,
    counters: Vec<TopologyCounter>,
) -> TopologyScopedResource {
    TopologyScopedResource {
        id: resource_id(domain, &scope),
        label,
        state,
        scope,
        counters,
    }
}

pub(super) fn scope_for_resource(
    realm: &str,
    area: &str,
    resource: &str,
    route_family: Option<u64>,
) -> TopologyScope {
    TopologyScope {
        realm: Some(realm.to_string()),
        route_family,
        area: Some(area.to_string()),
        resource: Some(resource.to_string()),
        ..TopologyScope::default()
    }
}

pub(super) fn scope_for_route(route: &str, session_id: Option<String>) -> TopologyScope {
    if let Some(parts) = route_quad(route) {
        return TopologyScope {
            realm: Some(parts.realm.to_string()),
            area: Some(parts.area.to_string()),
            resource: Some(parts.resource.to_string()),
            operation: Some(parts.operation.to_string()),
            route: Some(route.to_string()),
            session_id,
            ..TopologyScope::default()
        };
    }

    if let Some(parts) = route_triplet(route) {
        return TopologyScope {
            realm: Some(parts.realm.to_string()),
            area: Some(parts.area.to_string()),
            resource: Some(parts.resource.to_string()),
            route: Some(route.to_string()),
            session_id,
            ..TopologyScope::default()
        };
    }

    TopologyScope {
        route: Some(route.to_string()),
        session_id,
        ..TopologyScope::default()
    }
}

pub(super) fn scope_for_pattern(
    pattern: &str,
    realm: &str,
    session_id: Option<String>,
) -> TopologyScope {
    let mut scope = scope_for_route(pattern, session_id);
    scope.realm = Some(realm.to_string());
    scope.pattern = Some(pattern.to_string());
    scope
}

pub(super) fn session_node_id(session_id: &str) -> String {
    format!("session:{session_id}")
}

pub(super) fn domain_node_id(domain: &str) -> String {
    format!("domain:{domain}")
}

fn severity_state(severity: &troubleshooting::DiagnosticSeverity) -> Option<TopologyState> {
    match severity {
        troubleshooting::DiagnosticSeverity::High
        | troubleshooting::DiagnosticSeverity::Critical => Some(TopologyState::Blocked),
        troubleshooting::DiagnosticSeverity::Low | troubleshooting::DiagnosticSeverity::Medium => {
            Some(TopologyState::Pressure)
        }
        troubleshooting::DiagnosticSeverity::Informational => None,
    }
}

pub(super) fn topology_state(
    diagnostics: &troubleshooting::DomainDiagnostics,
    has_pressure: bool,
    has_activity: bool,
) -> TopologyState {
    let fallback = if has_pressure {
        TopologyState::Pressure
    } else if has_activity {
        TopologyState::Flowing
    } else {
        TopologyState::Quiet
    };

    severity_state(&diagnostics.snapshot.severity).unwrap_or(fallback)
}

pub(super) fn scoped_state(
    has_blocking_pressure: bool,
    has_pressure: bool,
    has_activity: bool,
) -> TopologyState {
    if has_blocking_pressure {
        TopologyState::Blocked
    } else if has_pressure {
        TopologyState::Pressure
    } else if has_activity {
        TopologyState::Flowing
    } else {
        TopologyState::Quiet
    }
}

fn state_rank(state: &TopologyState) -> u8 {
    match state {
        TopologyState::Quiet => 0,
        TopologyState::Flowing => 1,
        TopologyState::Pressure => 2,
        TopologyState::Blocked => 3,
    }
}

fn sort_resources(resources: &mut [TopologyScopedResource]) {
    resources.sort_by(|left, right| {
        state_rank(&right.state)
            .cmp(&state_rank(&left.state))
            .then_with(|| {
                score_counters(&right.counters)
                    .partial_cmp(&score_counters(&left.counters))
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.label.cmp(&right.label))
    });
}

fn score_counters(counters: &[TopologyCounter]) -> f64 {
    counters.iter().map(|counter| counter.value).sum()
}

pub(super) fn top_resources(
    mut resources: Vec<TopologyScopedResource>,
) -> Vec<TopologyScopedResource> {
    sort_resources(&mut resources);
    resources.truncate(TOP_RESOURCE_LIMIT);
    resources
}

pub(super) fn topology_lane(
    identity: (&str, &str),
    state: TopologyState,
    activity_per_second: f64,
    diagnostics: &troubleshooting::DomainDiagnostics,
    counters: Vec<TopologyCounter>,
    consumers_observers: (usize, usize),
    top_scoped_resources: Vec<TopologyScopedResource>,
) -> TopologyLane {
    let (id, title) = identity;
    let (consumers, observers) = consumers_observers;

    TopologyLane {
        id: id.to_string(),
        title: title.to_string(),
        state,
        activity_per_second,
        diagnostics: diagnostics.snapshot.clone(),
        counters,
        consumers,
        observers,
        top_scoped_resources,
    }
}

pub(super) fn topology_connection<I, S, T, L>(
    endpoints: (I, S, T),
    kind: TopologyConnectionKind,
    label: L,
    state: TopologyState,
    scope: TopologyScope,
    metrics: Vec<TopologyCounter>,
) -> TopologyConnection
where
    I: Into<String>,
    S: Into<String>,
    T: Into<String>,
    L: Into<String>,
{
    let (id, source, target) = endpoints;

    TopologyConnection {
        id: id.into(),
        kind,
        source: source.into(),
        target: target.into(),
        label: label.into(),
        state,
        scope,
        metrics,
    }
}

pub(super) fn scope_with_session(mut scope: TopologyScope, session_id: String) -> TopologyScope {
    scope.session_id = Some(session_id);
    scope
}

pub(super) fn add_broker_domain_flow(
    connections: &mut TopologyConnectionBuilder,
    domain: &str,
    state: &TopologyState,
    activity_per_second: f64,
    counters: Vec<TopologyCounter>,
) {
    if matches!(state, TopologyState::Quiet) && activity_per_second == 0.0 {
        return;
    }

    connections.push(topology_connection(
        (
            format!("broker-domain-flow:{domain}"),
            "broker",
            domain_node_id(domain),
        ),
        TopologyConnectionKind::BrokerDomainFlow,
        format!("{domain} lane"),
        state.clone(),
        TopologyScope::default(),
        counters,
    ));
}
