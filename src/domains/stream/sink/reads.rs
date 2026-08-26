//! Resource/area/realm/global read execution, cursor integrity, and the
//! per-family actor lookup reads are executed against.

use super::model::{
    route_triplet, Arc, Mutex, Route, RouteFamily, StreamActor, StreamDomainCore,
    StreamReadExecution, StreamResourceScope, StreamStorageLayout,
};
use crate::domains::stream::protocol::ReadResponse;

mod read_finalization;
mod wire_encoding;

use read_finalization::apply_global_snapshot_boundary;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadScope {
    Resource,
    Area,
    Realm,
    Global,
}

impl StreamDomainCore {
    pub(in crate::domains::stream::sink) fn run_maintenance_slice(&self, family: u64) {
        if let Err(error) = self.stream_store.run_maintenance(family) {
            tracing::warn!(
                domain = "stream",
                family,
                error,
                "Stream maintenance slice failed; queued work will be retried"
            );
        }
    }

    fn cursor_integrity_token(
        &self,
        selector_fingerprint: u64,
        captured_watermark: u64,
        next_offset: u64,
    ) -> u64 {
        use hmac::{Hmac, KeyInit, Mac};

        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(self.cursor_integrity_key.as_ref())
            .expect("Stream cursor HMAC key has a valid fixed length");
        mac.update(&[1]);
        mac.update(&selector_fingerprint.to_le_bytes());
        mac.update(&captured_watermark.to_le_bytes());
        mac.update(&next_offset.to_le_bytes());
        let bytes = mac.finalize().into_bytes();
        u64::from_le_bytes(
            bytes[..8]
                .try_into()
                .expect("HMAC-SHA256 output is 32 bytes"),
        )
    }

    pub(in crate::domains::stream::sink) fn storage_layout(&self) -> StreamStorageLayout {
        self.stream_store.storage_layout()
    }

    pub(in crate::domains::stream::sink) fn actor_key_for_route(
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<StreamResourceScope, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.realm.is_empty()
            || parts.area.is_empty()
            || parts.resource.is_empty()
            || parts.realm.contains('*')
            || parts.area.contains('*')
            || parts.resource.contains('*')
        {
            return Err("stream append routes require concrete realm/area/resource".to_string());
        }
        if parts.area == crate::domains::stream::INTERNAL_REALM_SEGMENT {
            return Err(format!(
                "area '{}' is reserved for internal broker use",
                crate::domains::stream::INTERNAL_REALM_SEGMENT
            ));
        }
        if parts.resource == crate::domains::stream::INTERNAL_AREA_SEGMENT {
            return Err(format!(
                "resource '{}' is reserved for internal broker use",
                crate::domains::stream::INTERNAL_AREA_SEGMENT
            ));
        }
        Ok(StreamResourceScope {
            family: family_id,
            realm: parts.realm.to_string(),
            area: parts.area.to_string(),
            resource: parts.resource.to_string(),
        })
    }

    pub(in crate::domains::stream::sink) fn get_or_create_actor(
        &self,
        key: &StreamResourceScope,
    ) -> Result<Arc<Mutex<StreamActor>>, String> {
        use std::collections::hash_map::Entry;

        let mut actors = self.actors.lock();
        match actors.entry(key.clone()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let actor = Arc::new(Mutex::new(StreamActor::new(
                    key.family,
                    key.realm.clone(),
                    key.area.clone(),
                    key.resource.clone(),
                    self.stream_store.clone(),
                )?));
                entry.insert(actor.clone());
                Ok(actor)
            }
        }
    }

    fn empty_global_read_cursor(
        &self,
        request: &StreamReadExecution<'_>,
        selector_fingerprint: u64,
        captured_watermark: u64,
    ) -> crate::domains::stream::protocol::ReadCursor {
        crate::domains::stream::protocol::ReadCursor {
            last_resource_offset: 0,
            last_area_offset: None,
            last_realm_offset: None,
            last_global_offset: None,
            has_more: request.from_offset < captured_watermark,
            cursor_fingerprint: Some(self.cursor_integrity_token(
                selector_fingerprint,
                captured_watermark,
                request.from_offset,
            )),
            captured_watermark: Some(captured_watermark),
        }
    }

    fn execute_read_plan(
        &self,
        scope: ReadScope,
        route_filter_area: Option<&str>,
        route_filter_resource: Option<&str>,
        request: &StreamReadExecution<'_>,
    ) -> Result<ReadResponse, String> {
        let parts = route_triplet(request.route.as_str());
        let (items, cursor) = match scope {
            ReadScope::Realm => {
                let parts = parts.ok_or_else(|| "invalid stream route".to_string())?;
                if let Some(resource) = route_filter_resource {
                    self.stream_store.read_realm_resource_posting(
                        &crate::domains::stream::store::ReadRealmPostingParams {
                            family: request.family_id.as_u64(),
                            realm: parts.realm,
                            resource,
                            from_offset: request.from_offset,
                            limit: request.limit,
                            max_bytes: request.max_bytes,
                        },
                        request.filter,
                    )?
                } else {
                    self.stream_store.read_realm_with_filter(
                        request.family_id.as_u64(),
                        parts.realm,
                        request.from_offset,
                        request.limit,
                        request.max_bytes,
                        request.filter,
                    )?
                }
            }
            ReadScope::Area => {
                let parts = parts.ok_or_else(|| "invalid stream route".to_string())?;
                self.stream_store.read_area_with_filter(
                    &crate::domains::stream::store::ReadAreaParams {
                        family: request.family_id.as_u64(),
                        realm: parts.realm,
                        area: parts.area,
                        from_offset: request.from_offset,
                        limit: request.limit,
                        max_bytes: request.max_bytes,
                    },
                    request.filter,
                )?
            }
            ReadScope::Resource => {
                let key = Self::actor_key_for_route(request.family_id, request.route)?;
                let response = self.get_or_create_actor(&key)?.lock().read_with_filter(
                    request.from_offset,
                    request.limit,
                    request.max_bytes,
                    request.filter,
                )?;
                (response.items, response.cursor)
            }
            ReadScope::Global => self.stream_store.read_global_posting(
                &crate::domains::stream::store::ReadGlobalPostingParams {
                    family: request.family_id.as_u64(),
                    from_offset: request.from_offset,
                    limit: request.limit,
                    max_bytes: request.max_bytes,
                    area: route_filter_area,
                    resource: route_filter_resource,
                },
                request.filter,
            )?,
        };
        Ok(ReadResponse { items, cursor })
    }

    fn finalize_read_response(
        &self,
        request: &StreamReadExecution<'_>,
        selector_fingerprint: u64,
        captured_watermark: u64,
        mut response: ReadResponse,
    ) -> ReadResponse {
        apply_global_snapshot_boundary(request.from_offset, captured_watermark, &mut response);
        let next_offset = response
            .cursor
            .last_global_offset
            .map_or(request.from_offset, |offset| offset.saturating_add(1));
        response.cursor.cursor_fingerprint = Some(self.cursor_integrity_token(
            selector_fingerprint,
            captured_watermark,
            next_offset,
        ));
        response.cursor.captured_watermark = Some(captured_watermark);
        response
    }

    pub(in crate::domains::stream::sink) fn encode_read_response_data(
        &self,
        request: StreamReadExecution<'_>,
    ) -> Result<Vec<u8>, String> {
        use crate::domains::stream::route_grammar::StreamRouteShape;

        let shape = crate::domains::stream::route_grammar::classify_stream_route_shape(
            request.route.as_str(),
        )?;
        let (scope, area_filter, resource_filter) = match &shape {
            StreamRouteShape::Resource { .. } => (ReadScope::Resource, None, None),
            StreamRouteShape::Area { .. } => (ReadScope::Area, None, None),
            StreamRouteShape::Realm { .. } => (ReadScope::Realm, None, None),
            StreamRouteShape::RealmFilterResource { resource, .. } => {
                (ReadScope::Realm, None, Some(*resource))
            }
            StreamRouteShape::Global => (ReadScope::Global, None, None),
            StreamRouteShape::GlobalFilterArea { area } => (ReadScope::Global, Some(*area), None),
            StreamRouteShape::GlobalFilterResource { resource } => {
                (ReadScope::Global, None, Some(*resource))
            }
            StreamRouteShape::GlobalFilterAreaResource { area, resource } => {
                (ReadScope::Global, Some(*area), Some(*resource))
            }
        };
        if scope != ReadScope::Global {
            if request.cursor_fingerprint.is_some() || request.captured_watermark.is_some() {
                return Err(
                    "ERR_CURSOR_UNSUPPORTED: snapshot cursors require a global stream selector"
                        .to_string(),
                );
            }
            let response = self.execute_read_plan(scope, area_filter, resource_filter, &request)?;
            return Ok(Self::encode_stream_read_data(
                &response.items,
                &response.cursor,
                false,
            ));
        }

        let selector_fingerprint = crate::domains::stream::route_grammar::cursor_fingerprint(
            request.family_id,
            &shape,
            request.filter,
        );
        let current_frontier = self
            .stream_store
            .get_global_watermark(request.family_id.as_u64())?;
        let captured_watermark = request.captured_watermark.unwrap_or(current_frontier);
        if captured_watermark > current_frontier {
            return Err(
                "ERR_CURSOR_WATERMARK_INVALID: captured watermark is ahead of the visible frontier"
                    .to_string(),
            );
        }
        let is_continuation =
            request.cursor_fingerprint.is_some() || request.captured_watermark.is_some();
        let expected_token = self.cursor_integrity_token(
            selector_fingerprint,
            captured_watermark,
            request.from_offset,
        );
        if is_continuation && request.cursor_fingerprint != Some(expected_token) {
            return Err(
                "ERR_CURSOR_SELECTOR_MISMATCH: cursor was issued for a different stream route \
                 family, selector, filter, snapshot, or position"
                    .to_string(),
            );
        }
        if request.limit == 0 {
            let cursor =
                self.empty_global_read_cursor(&request, selector_fingerprint, captured_watermark);
            return Ok(Self::encode_stream_read_data(&[], &cursor, true));
        }
        let response = self.execute_read_plan(scope, area_filter, resource_filter, &request)?;
        let response = self.finalize_read_response(
            &request,
            selector_fingerprint,
            captured_watermark,
            response,
        );
        Ok(Self::encode_stream_read_data(
            &response.items,
            &response.cursor,
            true,
        ))
    }

    pub(in crate::domains::stream::sink) fn encode_last_response_data(
        &self,
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<Vec<u8>, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.area == "*" || parts.resource == "*" {
            return Ok(Vec::new());
        }
        let key = Self::actor_key_for_route(family_id, route)?;
        let actor = self.get_or_create_actor(&key)?;
        let data = actor
            .lock()
            .last()?
            .record
            .as_ref()
            .map(Self::encode_stream_last_data)
            .unwrap_or_default();
        Ok(data)
    }

    pub(in crate::domains::stream::sink) fn encode_metadata_response_data(
        &self,
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<Vec<u8>, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.area == "*" || parts.resource == "*" {
            return Ok(Vec::new());
        }
        let key = Self::actor_key_for_route(family_id, route)?;
        let actor = self.get_or_create_actor(&key)?;
        let metadata = actor.lock().metadata()?.metadata;
        Ok(Self::encode_stream_metadata_data(&metadata))
    }
}
