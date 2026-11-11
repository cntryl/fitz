//! Standalone deletion belongs to the family, outside live append sessions.

use super::model::{StreamFamilyRuntime, StreamFamilyState};
use crate::domains::stream::{StreamClientResponseBody, StreamDropRequest};
use crate::runtime::{ClientFrameMeta, Envelope};

impl StreamFamilyRuntime {
    pub(super) fn handle_drop_frame(
        &mut self,
        envelope: &Envelope,
        meta: ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        request: &StreamDropRequest,
    ) {
        let response = self.drop_stream_scope(envelope, meta, request);
        self.core
            .route_stream_response(envelope, meta, &response, request_started);
    }

    fn drop_stream_scope(
        &mut self,
        envelope: &Envelope,
        meta: ClientFrameMeta,
        request: &StreamDropRequest,
    ) -> StreamClientResponseBody {
        let scope = &request.scope;
        if scope.route_family != meta.route_family {
            return StreamFamilyState::stream_error_response("route family mismatch");
        }
        if let Err(error) = scope.validate() {
            return StreamFamilyState::stream_error_response(
                crate::domains::stream::StreamClientError::new(
                    crate::domains::stream::StreamClientErrorKind::InvalidRoute,
                    error,
                ),
            );
        }
        if envelope.is_expired() {
            return StreamFamilyState::stream_error_response(
                crate::domains::stream::StreamClientError::new(
                    crate::domains::stream::StreamClientErrorKind::BadRequest,
                    "Stream DROP deadline expired before admission",
                ),
            );
        }
        if self.core.actors.iter().any(|(key, actor)| {
            actor.has_active_session()
                && scope.matches_parts(key.family, &key.realm, &key.area, &key.resource)
        }) {
            return StreamFamilyState::stream_error_response(
                crate::domains::stream::StreamClientError::new(
                    crate::domains::stream::StreamClientErrorKind::ConcurrencyConflict,
                    "DROP scope contains an active append session",
                ),
            );
        }
        match self.core.stream_store.drop_scope_with_deadline(
            scope,
            request.mode,
            envelope.deadline(),
        ) {
            Ok(()) => {
                self.forget_dropped_runtime_state(scope);
                self.core.observability.forget_drop_watermarks(scope);
                self.core.mark_committed_admin_snapshot_dirty();
                StreamClientResponseBody::Ok {
                    session_id: None,
                    data: vec![],
                }
            }
            Err(error) => StreamFamilyState::stream_error_response(error),
        }
    }

    fn forget_dropped_runtime_state(&mut self, scope: &crate::domains::stream::StreamDropScope) {
        self.core.actors.retain(|key, _| {
            !scope.matches_parts(key.family, &key.realm, &key.area, &key.resource)
        });
        self.watermark_coordinators
            .area
            .retain(|key, _| !scope.resets_area(&key.realm, &key.area));
        self.watermark_coordinators
            .realm
            .retain(|key, _| !scope.resets_realm(&key.realm));
        for commit in &mut self.pending_watermark_commits {
            commit.targets.area &= !scope.resets_area(&commit.realm, &commit.area);
            commit.targets.realm &= !scope.resets_realm(&commit.realm);
        }
        self.pending_watermark_commits
            .retain(|commit| !commit.targets.is_empty());
    }
}
