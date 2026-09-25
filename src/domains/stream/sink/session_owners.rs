//! Family-owned lookup of live Stream append-session owners.

use super::model::StreamResourceScope;
use crate::runtime::routing::RouteFamily;
use std::collections::HashMap;

#[derive(Clone)]
pub(super) struct StreamSessionOwner {
    pub(super) key: StreamResourceScope,
    pub(super) owner_session_id: u64,
}

#[derive(Default)]
pub(super) struct StreamSessionOwners {
    owners: HashMap<u64, StreamSessionOwner>,
}

impl StreamSessionOwners {
    pub(super) fn insert(&mut self, stream_session_id: u64, owner: StreamSessionOwner) {
        self.owners.insert(stream_session_id, owner);
    }

    pub(super) fn for_owner(
        &self,
        stream_session_id: u64,
        owner_session_id: u64,
        family: RouteFamily,
    ) -> Option<StreamSessionOwner> {
        self.owners
            .get(&stream_session_id)
            .filter(|owner| {
                owner.owner_session_id == owner_session_id && owner.key.family == family
            })
            .cloned()
    }

    pub(super) fn remove(&mut self, stream_session_id: u64) {
        self.owners.remove(&stream_session_id);
    }

    pub(super) fn len(&self) -> usize {
        self.owners.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::RouteFamily;

    #[test]
    fn should_match_stream_session_owner_only_with_the_same_connection_and_family() {
        // Arrange
        let family = RouteFamily::new(1);
        let key = super::super::model::StreamResourceScope {
            family,
            realm: "acme".to_string(),
            area: "app".to_string(),
            resource: "events".to_string(),
        };
        let mut owners = StreamSessionOwners::default();
        owners.insert(
            9,
            StreamSessionOwner {
                key: key.clone(),
                owner_session_id: 7,
            },
        );

        // Act
        let matching = owners.for_owner(9, 7, family);
        let other_connection = owners.for_owner(9, 8, family);
        let other_family = owners.for_owner(9, 7, RouteFamily::new(2));

        // Assert
        assert_eq!(matching.map(|owner| owner.key), Some(key));
        assert!(other_connection.is_none());
        assert!(other_family.is_none());
    }
}
