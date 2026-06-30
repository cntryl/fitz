use super::{debug, warn, Bytes, ChannelId, IngressDecision, RuntimeIngress, SessionFrame};
use crate::session::{SessionInfo, SessionPermissions};
use std::sync::Arc;
use tracing::error;

pub(super) struct SessionAuthenticator<'a> {
    ingress: &'a RuntimeIngress,
}

impl RuntimeIngress {
    pub(super) fn session_authenticator(&self) -> SessionAuthenticator<'_> {
        SessionAuthenticator { ingress: self }
    }
}

impl SessionAuthenticator<'_> {
    pub(super) async fn authenticate_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &Bytes,
        should_notify_handler: bool,
    ) -> Result<(crate::runtime::routing::RouteFamily, Option<SessionFrame>), IngressDecision> {
        let needs_authentication = self.needs_authentication(session_id)?;
        let verified_auth = if needs_authentication && self.ingress.auth_required {
            Some(
                self.verify_connect_frame(session_id, channel_id, msg_type, payload)
                    .await?,
            )
        } else {
            None
        };

        let Some(mut entry) = self.ingress.sessions.get_mut(&session_id) else {
            warn!(
                session_id = session_id,
                "Ingress: frame for unknown session"
            );
            return Err(IngressDecision::Close(format!(
                "unknown session: {session_id}"
            )));
        };

        let mut notify_frame = None;
        if !entry.authenticated {
            if self.ingress.auth_required {
                let Some((snapshot, claims, route_family)) = verified_auth else {
                    return Err(IngressDecision::Close(
                        "connect failed: session authentication state changed".to_string(),
                    ));
                };
                self.apply_authenticated_session(
                    session_id,
                    &mut entry,
                    claims,
                    snapshot,
                    route_family,
                );
                if should_notify_handler {
                    notify_frame =
                        Some(Self::session_frame(session_id, channel_id, payload.clone()));
                }
            } else {
                self.apply_anonymous_session(session_id, &mut entry);
                if should_notify_handler {
                    notify_frame =
                        Some(Self::session_frame(session_id, channel_id, payload.clone()));
                }
            }
        }

        Ok((entry.route_family, notify_frame))
    }

    fn needs_authentication(&self, session_id: u64) -> Result<bool, IngressDecision> {
        if let Some(entry) = self.ingress.sessions.get(&session_id) {
            Ok(!entry.authenticated)
        } else {
            warn!(
                session_id = session_id,
                "Ingress: frame for unknown session"
            );
            Err(IngressDecision::Close(format!(
                "unknown session: {session_id}"
            )))
        }
    }

    async fn verify_connect_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &Bytes,
    ) -> Result<
        (
            SessionPermissions,
            crate::auth::Claims,
            crate::runtime::routing::RouteFamily,
        ),
        IngressDecision,
    > {
        if channel_id != ChannelId::Control
            || msg_type != crate::protocol::tlv::MessageType::CONNECT
        {
            warn!(session_id = session_id, channel = ?channel_id, msg_type = msg_type.as_u16(), "Ingress: unauthenticated, CONNECT required");
            return Err(IngressDecision::Close(
                "unauthenticated: connect required".to_string(),
            ));
        }

        let compact = std::str::from_utf8(payload.as_ref())
            .unwrap_or("")
            .to_string();
        debug!(
            session_id = session_id,
            jwt_len = compact.len(),
            "Ingress: verifying CONNECT JWT"
        );

        let auth_config = self
            .ingress
            .auth_config
            .clone()
            .unwrap_or_else(|| crate::auth::AuthConfig::from_env(true));

        match crate::auth::verified_jwt_with_claims_config(
            &compact,
            &auth_config,
            &self.ingress.auth_claims_config,
        )
        .await
        {
            Ok(verified) => {
                let route_family =
                    match self.resolve_authenticated_route_family(&verified.raw_claims) {
                        Ok(route_family) => route_family,
                        Err(error) => {
                            error!(
                                session_id = session_id,
                                error = %error,
                                "Ingress: CONNECT failed (route family resolution)"
                            );
                            return Err(IngressDecision::Close(format!("connect failed: {error}")));
                        }
                    };
                Ok((verified.permissions, verified.claims, route_family))
            }
            Err(error) => {
                error!(
                    session_id = session_id,
                    error = %error,
                    "Ingress: CONNECT failed (verification)"
                );
                Err(IngressDecision::Close(format!("connect failed: {error}")))
            }
        }
    }

    pub(super) fn resolve_authenticated_route_family(
        &self,
        raw_claims: &crate::auth::RawClaims,
    ) -> Result<crate::runtime::routing::RouteFamily, String> {
        let route_family = self.ingress.route_family_resolver.resolve(raw_claims)?;
        if !self.ingress.route_families.contains(&route_family) {
            return Err(format!(
                "resolved route family {route_family} is not provisioned"
            ));
        }
        Ok(crate::runtime::routing::RouteFamily::new(
            route_family.into(),
        ))
    }

    pub(super) fn apply_authenticated_session(
        &self,
        session_id: u64,
        entry: &mut SessionInfo,
        claims: crate::auth::Claims,
        snapshot: SessionPermissions,
        route_family: crate::runtime::routing::RouteFamily,
    ) {
        entry.permissions_snapshot = snapshot.clone();
        entry.authenticated = true;
        entry.claims = Some(Arc::new(claims.clone()));
        entry.route_family = route_family;

        let mut actor = crate::session::actor::SessionActor::new(
            crate::session::session::SessionId(session_id),
            snapshot.clone(),
        );
        actor.authenticate(claims, snapshot);
        self.ingress.session_actors.insert(session_id, actor);

        if let Some(admin_read_model) = &self.ingress.admin_read_model {
            admin_read_model.record_session_update(entry);
        }
    }

    fn apply_anonymous_session(&self, session_id: u64, entry: &mut SessionInfo) {
        let snapshot = crate::auth::default_anonymous_permissions();
        entry.permissions_snapshot = snapshot.clone();
        entry.authenticated = true;
        entry.route_family = crate::runtime::routing::RouteFamily::new(1);

        self.ingress.session_actors.insert(
            session_id,
            crate::session::actor::SessionActor::new(
                crate::session::session::SessionId(session_id),
                snapshot,
            ),
        );
    }

    fn session_frame(session_id: u64, channel_id: ChannelId, payload: Bytes) -> SessionFrame {
        SessionFrame {
            session_id,
            channel_id,
            payload,
        }
    }
}
