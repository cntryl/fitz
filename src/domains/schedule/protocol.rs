//! Schedule request, response and compatibility data surfaces.

use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use bytes::Bytes;
use std::sync::Arc;

pub use super::cron::{CronField, CronSchedule};
pub use super::route_grammar::{
    parse_concrete_schedule_route, validate_concrete_schedule_route, ConcreteScheduleRoute,
};

// Preserve existing embedding paths; internal code imports the clock owner directly.
pub use crate::runtime::clock::{
    epoch_ms_to_instant_with_reference, instant_to_epoch_ms_with_reference, Clock, SystemClock,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleDeliveryMode {
    Broadcast = 0,
    Single = 1,
}

impl ScheduleDeliveryMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Broadcast => "broadcast",
            Self::Single => "single",
        }
    }
}

impl TryFrom<u8> for ScheduleDeliveryMode {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Broadcast),
            1 => Ok(Self::Single),
            _ => Err("invalid schedule delivery mode".to_string()),
        }
    }
}

/// Schedule operation messages
#[derive(Debug, Clone)]
pub enum ScheduleMessage {
    /// Create or update a schedule (route is identity, upsert)
    Create {
        route: String,
        cron: String,
        delivery_mode: ScheduleDeliveryMode,
        payload: Bytes,
    },
    /// Create or update multiple schedules atomically.
    CreateBatch { entries: Vec<ScheduleCreateEntry> },
    /// Cancel an existing schedule by route
    Cancel { route: String },
    /// Live route-ordered pagination. The cursor is an opaque continuation
    /// from a previous page; omitted cursors start at the first route.
    List { cursor: Option<String>, limit: u64 },
    /// Subscribe to live notifications for a schedule route pattern.
    Subscribe {
        family_id: RouteFamily,
        route: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Remove one live notification subscription for a schedule route pattern.
    Unsubscribe {
        family_id: RouteFamily,
        route: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Unsubscribe all schedule subscriptions for a session (called on disconnect)
    UnsubscribeAll {
        session_id: u64,
        subscriber: RouteAddress,
    },
}

/// Parsed client request delivered to the Schedule domain sink.
#[derive(Debug, Clone)]
pub struct ScheduleClientRequest {
    pub meta: ClientFrameMeta,
    pub message: Result<ScheduleMessage, ScheduleFailure>,
}

impl ScheduleClientRequest {
    pub fn new(meta: ClientFrameMeta, message: Result<ScheduleMessage, ScheduleFailure>) -> Self {
        Self { meta, message }
    }
}

/// Typed Schedule response to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct ScheduleClientResponse {
    pub meta: ClientFrameMeta,
    pub response: ScheduleResponse,
}

impl ScheduleClientResponse {
    #[must_use]
    pub fn new(meta: ClientFrameMeta, response: ScheduleResponse) -> Self {
        Self { meta, response }
    }
}

/// Typed live Schedule notification to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct ScheduleClientNotification {
    pub session_id: u64,
    pub route_family: RouteFamily,
    pub subscription_id: u64,
    pub route: String,
    pub payload: Bytes,
}

impl ScheduleClientNotification {
    pub fn new(
        session_id: u64,
        route_family: RouteFamily,
        subscription_id: u64,
        route: String,
        payload: Bytes,
    ) -> Self {
        Self {
            session_id,
            route_family,
            subscription_id,
            route,
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleCreateEntry {
    pub route: String,
    pub cron: String,
    pub delivery_mode: ScheduleDeliveryMode,
    pub payload: Bytes,
}

/// Response from schedule operations
#[derive(Debug, Clone)]
pub enum ScheduleResponse {
    /// Operation succeeded (no `schedule_id` returned; route is identity).
    Ok,
    /// SUBSCRIBE succeeded with the logical `subscription_id` used for NOTIFY fanout.
    SubscribeOk { subscription_id: u64 },
    /// One route-ordered LIST page.
    ListPage {
        entries: Arc<Vec<Arc<ScheduleListEntry>>>,
        has_more: bool,
        continuation: Option<String>,
    },
    /// Operation failed with a stable wire category and actionable message.
    Error(ScheduleFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleFailureCategory {
    NotFound,
    InvalidCron,
    Limit,
    Parse,
    InvalidTarget,
    InvalidSubscriptionPattern,
    SubscriptionLimit,
    InvalidDeliveryMode,
    Unauthorized,
}

impl ScheduleFailureCategory {
    #[must_use]
    pub const fn code(self) -> u16 {
        match self {
            Self::NotFound => 7050,
            Self::InvalidCron => 7051,
            Self::Limit => 7052,
            Self::Parse => 7008,
            Self::InvalidTarget => 7053,
            Self::InvalidSubscriptionPattern => 7003,
            Self::SubscriptionLimit => 7004,
            Self::InvalidDeliveryMode => 7054,
            Self::Unauthorized => 7001,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleFailure {
    pub category: ScheduleFailureCategory,
    pub message: String,
}

impl ScheduleFailure {
    #[must_use]
    pub fn new(category: ScheduleFailureCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn parse(message: impl Into<String>) -> Self {
        Self::new(ScheduleFailureCategory::Parse, message)
    }
}

impl std::fmt::Display for ScheduleFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl From<String> for ScheduleFailure {
    fn from(message: String) -> Self {
        Self::parse(message)
    }
}

impl From<&str> for ScheduleFailure {
    fn from(message: &str) -> Self {
        Self::parse(message)
    }
}

/// Single schedule entry in LIST response
#[derive(Debug, Clone)]
pub struct ScheduleListEntry {
    pub route: String,
    pub cron: String,
    pub delivery_mode: ScheduleDeliveryMode,
    pub payload: Bytes,
}
