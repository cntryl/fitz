//! Bounded, process-local audit retention for MCP registry callers.

use super::McpCapabilityClass;
use crate::api::admin::auth::AdminPrincipal;
use crate::session::permissions::SessionPermissions;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

pub(super) const AUDIT_RECORD_LIMIT: usize = 1024;
const AUDIT_FIELD_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum McpAuditDecision {
    Allowed,
    Denied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpAuditRecord {
    pub correlation_id: Option<String>,
    pub principal: Option<String>,
    pub tool_name: String,
    pub capability: McpCapabilityClass,
    pub scope_route: Option<String>,
    pub argument_summary: String,
    pub decision: McpAuditDecision,
    pub result_summary: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct McpAuditBuffer(Arc<Mutex<AuditLog>>);

impl McpAuditBuffer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub(crate) fn records(&self) -> Vec<McpAuditRecord> {
        self.0.lock().records.iter().cloned().collect()
    }

    #[must_use]
    pub(crate) fn dropped(&self) -> u64 {
        self.0.lock().dropped
    }
}

#[derive(Debug, Clone)]
pub struct McpExecutionContext {
    pub principal: Option<AdminPrincipal>,
    pub permissions: SessionPermissions,
    audit_log: McpAuditBuffer,
    correlation_id: Option<String>,
    started_at: Option<Instant>,
}

impl McpExecutionContext {
    #[must_use]
    pub fn authenticated(principal: AdminPrincipal, permissions: SessionPermissions) -> Self {
        Self::authenticated_with_audit(principal, permissions, McpAuditBuffer::new())
    }

    pub(crate) fn authenticated_with_audit(
        principal: AdminPrincipal,
        permissions: SessionPermissions,
        audit_log: McpAuditBuffer,
    ) -> Self {
        Self {
            principal: Some(principal),
            permissions,
            audit_log,
            correlation_id: None,
            started_at: None,
        }
    }

    #[must_use]
    pub fn anonymous(permissions: SessionPermissions) -> Self {
        Self {
            principal: None,
            permissions,
            audit_log: McpAuditBuffer::new(),
            correlation_id: None,
            started_at: None,
        }
    }

    #[must_use]
    pub(crate) fn with_correlation_id(&self, correlation_id: String) -> Self {
        let mut context = self.clone();
        context.correlation_id = Some(correlation_id);
        context.started_at = Some(Instant::now());
        context
    }

    #[must_use]
    pub fn audit_records(&self) -> Vec<McpAuditRecord> {
        self.audit_log.records()
    }

    /// Number of old records evicted from this shared process-local context.
    #[must_use]
    pub fn dropped_audit_records(&self) -> u64 {
        self.audit_log.dropped()
    }

    pub(super) fn record_audit(&self, mut record: McpAuditRecord) {
        if record.correlation_id.is_none() {
            record.correlation_id.clone_from(&self.correlation_id);
        }
        if record.duration_ms == 0 {
            record.duration_ms = self.started_at.map_or(0, |started| {
                started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
            });
        }
        for value in [&mut record.principal, &mut record.scope_route]
            .into_iter()
            .flatten()
        {
            bound_field(value);
        }
        for value in [
            &mut record.tool_name,
            &mut record.argument_summary,
            &mut record.result_summary,
        ] {
            bound_field(value);
        }
        let mut audit = self.audit_log.0.lock();
        if audit.records.len() == AUDIT_RECORD_LIMIT {
            audit.records.pop_front();
            audit.dropped = audit.dropped.saturating_add(1);
            super::telemetry::record_audit_eviction();
        }
        if record.decision == McpAuditDecision::Denied {
            super::telemetry::record_denial();
        }
        audit.records.push_back(record);
    }

    pub(super) fn record_transport_denial(&self, operation: &str, reason: &str) {
        self.record_audit(McpAuditRecord {
            principal: self.principal_name(),
            correlation_id: None,
            tool_name: operation.to_string(),
            capability: McpCapabilityClass::Summary,
            scope_route: None,
            argument_summary: "redacted".to_string(),
            decision: McpAuditDecision::Denied,
            result_summary: reason.to_string(),
            duration_ms: 0,
        });
    }

    pub(super) fn record_action_audit(
        &self,
        action: &str,
        result: &str,
        decision: McpAuditDecision,
    ) {
        self.record_audit(McpAuditRecord {
            principal: self.principal_name(),
            correlation_id: None,
            tool_name: action.to_string(),
            capability: McpCapabilityClass::Mutate,
            scope_route: None,
            argument_summary: "redacted".to_string(),
            decision,
            result_summary: result.to_string(),
            duration_ms: 0,
        });
    }

    pub(super) fn principal_name(&self) -> Option<String> {
        self.principal
            .as_ref()
            .map(|principal| principal.username.clone())
    }
}

#[derive(Debug, Default)]
struct AuditLog {
    records: VecDeque<McpAuditRecord>,
    dropped: u64,
}

fn bound_field(value: &mut String) {
    let mut end = value.len().min(AUDIT_FIELD_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    // Identifiers are untrusted data; never preserve log-control characters.
    *value = value
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect();
}

#[cfg(test)]
mod tests;
