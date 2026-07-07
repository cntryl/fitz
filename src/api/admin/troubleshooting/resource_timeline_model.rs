use crate::api::admin::ResourcePath;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{rfc3339, DiagnosticSnapshot};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceTimelineKind {
    Observation,
    Transition,
    Failure,
    Retry,
    OwnershipChange,
    StateFlip,
    Registration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceTimelineEvent {
    pub domain: String,
    pub kind: ResourceTimelineKind,
    pub observed_at: String,
    pub summary: String,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: Option<String>,
    pub family: Option<u64>,
    pub age_seconds: Option<u64>,
    pub owner_session: Option<String>,
    pub worker_session: Option<String>,
    pub correlation_id: Option<String>,
    pub message_id: Option<u64>,
    pub attempts: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceTimeline {
    pub domain: String,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub family: Option<u64>,
    pub derived: bool,
    pub limit: usize,
    pub diagnostics: DiagnosticSnapshot,
    pub events: Vec<ResourceTimelineEvent>,
}

impl ResourceTimelineEvent {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        domain: &str,
        kind: ResourceTimelineKind,
        observed_at: DateTime<Utc>,
        summary: impl Into<String>,
        path: &ResourcePath<'_>,
        family: Option<u64>,
        operation: Option<String>,
        age_seconds: Option<u64>,
        owner_session: Option<String>,
        worker_session: Option<String>,
        correlation_id: Option<String>,
        message_id: Option<u64>,
        attempts: Option<usize>,
    ) -> Self {
        Self {
            domain: domain.to_string(),
            kind,
            observed_at: rfc3339(observed_at),
            summary: summary.into(),
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            operation,
            family,
            age_seconds,
            owner_session,
            worker_session,
            correlation_id,
            message_id,
            attempts,
        }
    }
}

impl ResourceTimeline {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        domain: &str,
        path: &ResourcePath<'_>,
        family: Option<u64>,
        diagnostics: DiagnosticSnapshot,
        limit: usize,
        events: Vec<ResourceTimelineEvent>,
    ) -> Self {
        Self {
            domain: domain.to_string(),
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            family,
            derived: true,
            limit,
            diagnostics,
            events,
        }
    }
}
