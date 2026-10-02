//! Bounded, process-local audit retention for MCP registry callers.

use super::McpCapabilityClass;
use crate::api::admin::auth::AdminPrincipal;
use crate::session::permissions::SessionPermissions;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;

pub(super) const AUDIT_RECORD_LIMIT: usize = 1024;
const AUDIT_FIELD_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum McpAuditDecision {
    Allowed,
    Denied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpAuditRecord {
    pub principal: Option<String>,
    pub tool_name: String,
    pub capability: McpCapabilityClass,
    pub scope_route: Option<String>,
    pub argument_summary: String,
    pub decision: McpAuditDecision,
    pub result_summary: String,
}

#[derive(Debug, Clone)]
pub struct McpExecutionContext {
    pub principal: Option<AdminPrincipal>,
    pub permissions: SessionPermissions,
    audit_log: Arc<Mutex<AuditLog>>,
}

impl McpExecutionContext {
    #[must_use]
    pub fn authenticated(principal: AdminPrincipal, permissions: SessionPermissions) -> Self {
        Self {
            principal: Some(principal),
            permissions,
            audit_log: Arc::new(Mutex::new(AuditLog::default())),
        }
    }

    #[must_use]
    pub fn anonymous(permissions: SessionPermissions) -> Self {
        Self {
            principal: None,
            permissions,
            audit_log: Arc::new(Mutex::new(AuditLog::default())),
        }
    }

    #[must_use]
    pub fn audit_records(&self) -> Vec<McpAuditRecord> {
        self.audit_log.lock().records.iter().cloned().collect()
    }

    /// Number of old records evicted from this shared process-local context.
    #[must_use]
    pub fn dropped_audit_records(&self) -> u64 {
        self.audit_log.lock().dropped
    }

    pub(super) fn record_audit(&self, mut record: McpAuditRecord) {
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
        let mut audit = self.audit_log.lock();
        if audit.records.len() == AUDIT_RECORD_LIMIT {
            audit.records.pop_front();
            audit.dropped = audit.dropped.saturating_add(1);
        }
        audit.records.push_back(record);
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
