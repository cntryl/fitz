use super::{info, obs, RuntimeIngress, SessionEvent};
use crate::session::{SessionInfo, SessionPermissions};

pub(super) struct SessionRegistry<'a> {
    ingress: &'a RuntimeIngress,
}

impl RuntimeIngress {
    pub(super) fn session_registry(&self) -> SessionRegistry<'_> {
        SessionRegistry { ingress: self }
    }
}

impl SessionRegistry<'_> {
    pub(super) fn open_session(&self, session: SessionInfo) -> u64 {
        let session_id = session.session_id;

        if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
            collector.counter_inc(obs::METRIC_SESSIONS_CREATED);
        }

        info!(
            session_id = session_id,
            transport = %session.transport_kind,
            peer_addr = ?session.peer_addr,
            authenticated = session.authenticated,
            "Ingress: session opened"
        );

        self.ingress.sessions.insert(session_id, session.clone());
        self.ingress.session_inbox_routes.insert(
            session_id,
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        );
        if let Some(admin_read_model) = &self.ingress.admin_read_model {
            admin_read_model.record_session_open(&session);
        }

        let permissions = if self.ingress.auth_required {
            session.permissions_snapshot.clone()
        } else {
            SessionPermissions::all()
        };

        self.ingress.session_actors.insert(
            session_id,
            crate::session::actor::SessionActor::new(
                crate::session::session::SessionId(session_id),
                permissions,
            ),
        );

        if let Some(handler) = &self.ingress.event_handler {
            handler(SessionEvent::Open(session_id, session));
        }

        session_id
    }

    pub(super) fn session_actor(
        &self,
        session_id: u64,
    ) -> Option<crate::session::actor::SessionActor> {
        self.ingress
            .session_actors
            .get(&session_id)
            .map(|entry| entry.value().clone())
    }

    pub(super) fn session(&self, session_id: u64) -> Option<SessionInfo> {
        self.ingress
            .sessions
            .get(&session_id)
            .map(|entry| entry.value().clone())
    }

    pub(super) fn active_sessions(&self) -> Vec<SessionInfo> {
        self.ingress
            .sessions
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub(super) fn session_count(&self) -> usize {
        self.ingress.sessions.len()
    }

    pub(super) fn route_family(
        &self,
        session_id: u64,
    ) -> Option<crate::runtime::routing::RouteFamily> {
        self.ingress
            .sessions
            .get(&session_id)
            .map(|session| session.route_family)
    }

    pub(super) fn record_frame_received(&self, session_id: u64) {
        if let Some(session) = self.ingress.sessions.get(&session_id) {
            session.record_frame_received();
        }
    }

    pub(super) fn record_frame_sent(&self, session_id: u64) {
        if let Some(session) = self.ingress.sessions.get(&session_id) {
            session.record_frame_sent();
        }
    }

    pub(super) fn cached_inbox_route(&self, session_id: u64) -> crate::runtime::routing::Route {
        self.ingress
            .session_inbox_routes
            .get(&session_id)
            .map_or_else(
                || crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
                |entry| entry.value().clone(),
            )
    }

    pub(super) fn finalize_close(&self, session_id: u64) {
        self.ingress.sessions.remove(&session_id);
        self.ingress.session_actors.remove(&session_id);
        self.ingress.session_inbox_routes.remove(&session_id);
        if let Some(admin_read_model) = &self.ingress.admin_read_model {
            admin_read_model.record_session_close(session_id);
        }
    }
}
