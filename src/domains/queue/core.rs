use crate::runtime::routing::{route_exact_triplet, Route, RouteFamily};
use crate::utils::storage_key::{self, DomainKeyspace};
use bytes::Bytes;
use lexkey::LexKey;
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
    #[must_use]
    pub fn from_route(family: RouteFamily, route: &Route) -> Option<Self> {
        let parts = route_exact_triplet(route.as_str())?;

        if !parts.realm.is_empty()
            && !parts.area.is_empty()
            && !parts.resource.is_empty()
            && !parts.realm.contains('*')
            && !parts.area.contains('*')
            && !parts.resource.contains('*')
        {
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

    pub(crate) fn from_storage_key(family: RouteFamily, key: &[u8]) -> Option<Self> {
        let (realm, suffix) = storage_key::split_domain_key(key, DomainKeyspace::Queue)?;
        let area_end = suffix.iter().position(|byte| *byte == LexKey::SEPARATOR)?;
        let resource_start = area_end + 1;
        let resource_end = resource_start
            + suffix[resource_start..]
                .iter()
                .position(|byte| *byte == LexKey::SEPARATOR)?;
        suffix.get(resource_end + 1)?;
        let area = std::str::from_utf8(&suffix[..area_end]).ok()?;
        let resource = std::str::from_utf8(&suffix[resource_start..resource_end]).ok()?;
        if realm.is_empty()
            || area.is_empty()
            || resource.is_empty()
            || realm.contains('*')
            || area.contains('*')
            || resource.contains('*')
        {
            return None;
        }
        Some(Self {
            family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
        })
    }
}

/// Reserved message paired with its concrete queue route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutedReservedMessage {
    pub route: String,
    pub message: ReservedMessage,
}

/// Message identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(u64);

impl MessageId {
    #[must_use]
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    #[must_use]
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

    #[test]
    fn should_reject_queue_route_with_trailing_segments() {
        // Arrange
        let route = Route::new("queue://acme/tasks/work/extra");
        let family = RouteFamily::new(1);

        // Act
        let key = QueueKey::from_route(family, &route);

        // Assert
        assert!(key.is_none());
    }

    #[test]
    fn should_reject_queue_route_with_wildcard_segment() {
        // Arrange
        let route = Route::new("queue://acme/tasks/*");
        let family = RouteFamily::new(1);

        // Act
        let key = QueueKey::from_route(family, &route);

        // Assert
        assert!(key.is_none());
    }
}
