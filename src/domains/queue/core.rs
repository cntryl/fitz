use crate::runtime::routing::{Route, RouteFamily, route_triplet};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Parsed queue identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueKey {
    pub family: RouteFamily,
    pub realm: String,
    pub area: String,
    pub resource: String,
}

impl QueueKey {
    /// Parse a route into queue key.
    pub fn from_route(family: RouteFamily, route: &Route) -> Option<Self> {
        let parts = route_triplet(route.as_str())?;

        if !parts.realm.is_empty() && !parts.area.is_empty() && !parts.resource.is_empty() {
            Some(QueueKey {
                family,
                realm: parts.realm.to_string(),
                area: parts.area.to_string(),
                resource: parts.resource.to_string(),
            })
        } else {
            None
        }
    }
}

/// Message identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(u64);

impl MessageId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Reserved message with body and inflight metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservedMessage {
    pub id: MessageId,
    pub body: Bytes,
    pub token: u64,
    pub inflight_seconds: u64,
    pub attempts: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_queue_route_with_scheme() {
        // Arrange
        let route = Route::new("queue://acme/tasks/work");
        let family = RouteFamily::new(1);

        // Act
        let key = QueueKey::from_route(family, &route);

        // Assert
        assert!(key.is_some());
        let key = key.unwrap();
        assert_eq!(key.realm, "acme");
        assert_eq!(key.area, "tasks");
        assert_eq!(key.resource, "work");
    }

    #[test]
    fn should_parse_queue_route_without_scheme() {
        // Arrange
        let route = Route::new("acme/tasks/work");
        let family = RouteFamily::new(2);

        // Act
        let key = QueueKey::from_route(family, &route);

        // Assert
        assert!(key.is_some());
        let key = key.unwrap();
        assert_eq!(key.realm, "acme");
        assert_eq!(key.area, "tasks");
        assert_eq!(key.resource, "work");
    }

    #[test]
    fn should_reject_queue_route_with_too_few_segments() {
        // Arrange
        let route = Route::new("queue://acme/tasks");
        let family = RouteFamily::new(1);

        // Act
        let key = QueueKey::from_route(family, &route);

        // Assert
        assert!(key.is_none());
    }
}
