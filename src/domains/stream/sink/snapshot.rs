//! Family-serialized Stream snapshot capture.

use super::model::{StreamFamilyRuntime, StreamFamilyState, WatermarkCommit};
use crate::domains::stream::protocol::StreamReadItem;
use crate::domains::stream::store::ReadResourceParams;
use crate::snapshot::{
    SnapshotArtifact, SnapshotDomain, SnapshotSelector, SnapshotStreamRecord,
    SnapshotStreamResource,
};
use std::collections::BTreeMap;

impl StreamFamilyState {
    pub(super) fn capture_stream_snapshot(
        &self,
        selector: &SnapshotSelector,
    ) -> Result<SnapshotArtifact, String> {
        if selector.domain() != SnapshotDomain::Stream {
            return Err("Stream snapshot capture requires a Stream selector".to_string());
        }
        if selector.route_family() != self.family {
            return Err("Stream snapshot selector family does not match its actor".to_string());
        }
        let mut resources = BTreeMap::new();
        for resource in self
            .stream_store
            .list_resource_metadata(self.family.as_u64())?
        {
            let route = format!(
                "stream://{}/{}/{}",
                resource.realm, resource.area, resource.resource
            );
            if !selector.matches(SnapshotDomain::Stream, self.family, &route) {
                continue;
            }
            let mut records = Vec::new();
            let mut from_offset = 0_u64;
            while from_offset < resource.next_offset {
                let params = ReadResourceParams {
                    family: self.family.as_u64(),
                    realm: &resource.realm,
                    area: &resource.area,
                    resource: &resource.resource,
                    from_offset,
                    limit: resource.next_offset.saturating_sub(from_offset),
                    max_bytes: None,
                };
                let (items, cursor) = self.stream_store.read_resource(&params)?;
                for item in items {
                    let StreamReadItem::Event(record) = item else {
                        return Err(format!(
                            "Stream snapshot read returned a filtered marker for {route}"
                        ));
                    };
                    if record.resource_offset >= resource.next_offset {
                        continue;
                    }
                    records.push(SnapshotStreamRecord {
                        body: record.body.to_vec(),
                        metadata: record.metadata.map(|value| value.to_vec()),
                    });
                }
                let next = cursor
                    .last_resource_offset
                    .checked_add(1)
                    .ok_or_else(|| format!("Stream snapshot offset overflow for {route}"))?;
                if next <= from_offset {
                    return Err(format!("Stream snapshot read made no progress for {route}"));
                }
                from_offset = next;
            }
            resources.insert(
                route.clone(),
                SnapshotStreamResource {
                    route,
                    captured_watermark: resource.next_offset,
                    records,
                },
            );
        }
        if selector.is_exact_resource() && resources.is_empty() {
            resources.insert(
                selector.route_pattern().to_string(),
                SnapshotStreamResource {
                    route: selector.route_pattern().to_string(),
                    captured_watermark: 0,
                    records: Vec::new(),
                },
            );
        }
        SnapshotArtifact::from_stream_resources(selector, resources.into_values().collect())
    }
}

impl StreamFamilyRuntime {
    pub(super) fn restore_stream_snapshot(
        &mut self,
        artifact: &SnapshotArtifact,
    ) -> Result<Vec<String>, String> {
        if artifact.domain() != SnapshotDomain::Stream
            || artifact.route_family() != self.core.family.as_u64()
        {
            return Err("Stream snapshot family or domain does not match destination".to_string());
        }
        let selector = SnapshotSelector::new(
            SnapshotDomain::Stream,
            self.core.family,
            artifact.selector(),
        )?;
        self.validate_empty_stream_destination(artifact, &selector)?;
        let mut completed = Vec::new();
        for resource in artifact.stream_resources() {
            self.restore_stream_resource(resource, &completed)?;
            completed.push(resource.route.clone());
        }
        Ok(completed)
    }

    fn validate_empty_stream_destination(
        &self,
        artifact: &SnapshotArtifact,
        selector: &SnapshotSelector,
    ) -> Result<(), String> {
        let existing = self
            .core
            .stream_store
            .list_resource_metadata(self.core.family.as_u64())?;
        if existing.iter().any(|resource| {
            selector.matches(
                SnapshotDomain::Stream,
                self.core.family,
                &format!(
                    "stream://{}/{}/{}",
                    resource.realm, resource.area, resource.resource
                ),
            )
        }) {
            return Err("Stream snapshot destination contains matching history".to_string());
        }
        for resource in artifact.stream_resources() {
            let Some((realm, area, name)) = parse_stream_resource_route(&resource.route) else {
                return Err(format!(
                    "invalid Stream resource route in snapshot: {}",
                    resource.route
                ));
            };
            if self.core.stream_store.get_next_resource_offset(
                self.core.family.as_u64(),
                realm,
                area,
                name,
            )? != 0
            {
                return Err(format!(
                    "Stream snapshot destination contains matching history at {}",
                    resource.route
                ));
            }
        }
        Ok(())
    }

    fn restore_stream_resource(
        &mut self,
        resource: &SnapshotStreamResource,
        completed: &[String],
    ) -> Result<(), String> {
        let Some((realm, area, name)) = parse_stream_resource_route(&resource.route) else {
            return Err(format!(
                "invalid Stream resource route in snapshot: {}",
                resource.route
            ));
        };
        let mut expected_offset = 0_u64;
        for (index, record) in resource.records.iter().enumerate() {
            let event = crate::domains::stream::store::EventPayload {
                body: bytes::Bytes::copy_from_slice(&record.body),
                metadata: record
                    .metadata
                    .as_deref()
                    .map(bytes::Bytes::copy_from_slice),
                discriminator: None,
            };
            let commit = self.core.stream_store.commit_records(
                crate::domains::stream::store::CommitRecordsParams {
                    family: self.core.family.as_u64(),
                    realm,
                    area,
                    resource: name,
                    expected_resource_next_offset: expected_offset,
                    events: std::slice::from_ref(&event),
                    ingest_metadata: None,
                    mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
                },
            );
            let commit = match commit {
                Ok(commit) => commit,
                Err(error) => {
                    return Err(format!(
                        "Stream snapshot replay failed after resources {completed:?}; resource {} committed {} of {} records: {error}",
                        resource.route,
                        index,
                        resource.records.len()
                    ));
                }
            };
            expected_offset = commit.last_resource_offset.saturating_add(1);
            self.enqueue_watermark_commit(WatermarkCommit {
                family: self.core.family,
                realm: realm.to_string(),
                area: area.to_string(),
                batch: crate::domains::stream::protocol::BatchCommitted {
                    first_area_offset: commit.first_area_offset,
                    last_area_offset: commit.last_area_offset,
                    first_realm_offset: commit.first_realm_offset,
                    last_realm_offset: commit.last_realm_offset,
                    first_global_offset: commit.first_global_offset,
                    last_global_offset: commit.last_global_offset,
                },
            });
        }
        Ok(())
    }
}

fn parse_stream_resource_route(route: &str) -> Option<(&str, &str, &str)> {
    let rest = route.strip_prefix("stream://")?;
    let (realm, rest) = rest.split_once('/')?;
    let (area, resource) = rest.split_once('/')?;
    if realm.is_empty() || area.is_empty() || resource.is_empty() || resource.contains('/') {
        return None;
    }
    Some((realm, area, resource))
}
