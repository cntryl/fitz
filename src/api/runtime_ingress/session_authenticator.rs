use super::{debug, warn, Bytes, ChannelId, IngressDecision, RuntimeIngress, SessionFrame};
use crate::session::{SessionInfo, SessionPermissions};
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

/// Outcome of running a frame through session authentication.
pub(super) struct AuthenticatedFrame {
    pub(super) route_family: crate::runtime::routing::RouteFamily,
    pub(super) notify_frame: Option<SessionFrame>,
    /// True only on the frame that established the session. Distinct from
    /// `notify_frame`, which is populated only when an event handler is
    /// registered - the HELLO answer must not depend on that.
    pub(super) session_established: bool,
    /// The service name the client reported in CONNECT, when non-empty.
    pub(super) service_name: Option<String>,
}

/// Why a CONNECT was refused, as the client will see it.
pub(super) struct ConnectRefusal {
    pub(super) code: u16,
    pub(super) message: String,
}

pub(super) struct SessionAuthenticator {
    pub(super) registry: super::session_registry::SessionRegistry,
    pub(super) route_families: Arc<std::collections::HashSet<u32>>,
    pub(super) auth_required: bool,
    pub(super) auth_config: Option<crate::auth::AuthConfig>,
    pub(super) auth_claims_config: crate::auth::AuthClaimsConfig,
    pub(super) route_family_resolver: crate::auth::RouteFamilyResolverConfig,
    pub(super) connect_diagnostics_budget: Arc<ConnectDiagnosticsBudget>,
    /// Routes `CONNECT_REJECTED` to the session inbox before the close.
    pub(super) router: Option<Arc<crate::runtime::Router>>,
}

impl RuntimeIngress {
    pub(super) fn session_authenticator(&self) -> &SessionAuthenticator {
        &self.authenticator
    }
}

/// How long one CONNECT-failure diagnostics budget window lasts.
const CONNECT_DIAGNOSTICS_WINDOW: Duration = Duration::from_secs(1);
/// Full diagnostics emitted per window before the rest are summarized.
const MAX_FULL_CONNECT_DIAGNOSTICS_PER_WINDOW: u32 = 5;

/// Whether one CONNECT failure may log full diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectDiagnosticsGrant {
    /// Emit the full diagnostic record.
    Full,
    /// Emit only a terse line; `suppressed_in_window` failures have been
    /// summarized this window so far, including this one.
    Suppressed { suppressed_in_window: u64 },
}

/// Rate limiter for CONNECT-failure diagnostics.
///
/// The full record costs a SHA-256, a base64 decode, and a `serde_json` parse
/// of an attacker-controlled payload, and expands to several kilobytes of
/// ERROR-level output - all for a peer that has not authenticated. Without a
/// bound, a peer looping CONNECT with a JWT-shaped payload of long claim
/// values can flood the log pipeline. Windowed rather than per-session, since
/// the attacker chooses the session count.
#[derive(Debug, Default)]
struct ConnectDiagnosticsWindow {
    started_at_millis: u64,
    opened: bool,
    emitted: u32,
    suppressed: u64,
}

#[derive(Debug)]
pub(crate) struct ConnectDiagnosticsBudget {
    baseline: std::time::Instant,
    // The window marker and its counters must move together: publishing a new
    // window before resetting its counters lets a concurrent caller increment
    // the outgoing counter and then have that increment erased, granting more
    // full diagnostics than the bound allows. One lock keeps the whole
    // decision atomic, and this is a failure path that is already about to
    // log, so the contention cost is irrelevant.
    window: std::sync::Mutex<ConnectDiagnosticsWindow>,
}

impl Default for ConnectDiagnosticsBudget {
    fn default() -> Self {
        Self {
            baseline: std::time::Instant::now(),
            window: std::sync::Mutex::new(ConnectDiagnosticsWindow::default()),
        }
    }
}

impl ConnectDiagnosticsBudget {
    /// Take one grant for a failure observed now.
    pub(crate) fn acquire_now(&self) -> ConnectDiagnosticsGrant {
        self.acquire(u64::try_from(self.baseline.elapsed().as_millis()).unwrap_or(u64::MAX))
    }

    /// Take one grant for a failure observed at `now_millis` (monotonic).
    pub(crate) fn acquire(&self, now_millis: u64) -> ConnectDiagnosticsGrant {
        let window_millis = u64::try_from(CONNECT_DIAGNOSTICS_WINDOW.as_millis()).unwrap_or(1_000);
        let mut window = self
            .window
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if !window.opened
            || now_millis < window.started_at_millis
            || now_millis.saturating_sub(window.started_at_millis) >= window_millis
        {
            *window = ConnectDiagnosticsWindow {
                started_at_millis: now_millis,
                opened: true,
                emitted: 1,
                suppressed: 0,
            };
            return ConnectDiagnosticsGrant::Full;
        }

        if window.emitted < MAX_FULL_CONNECT_DIAGNOSTICS_PER_WINDOW {
            window.emitted = window.emitted.saturating_add(1);
            return ConnectDiagnosticsGrant::Full;
        }
        window.suppressed = window.suppressed.saturating_add(1);
        ConnectDiagnosticsGrant::Suppressed {
            suppressed_in_window: window.suppressed,
        }
    }
}

impl SessionAuthenticator {
    fn log_connect_failure(&self, session_id: u64, compact: &str, stage: &str, error: &str) {
        const MAX_LOGGED_ERROR_CHARS: usize = 512;

        if let ConnectDiagnosticsGrant::Suppressed {
            suppressed_in_window,
        } = self.connect_diagnostics_budget.acquire_now()
        {
            // Terse and cheap: no token hashing, decoding, or claim parsing.
            warn!(
                session_id,
                stage,
                suppressed_in_window,
                "Ingress: CONNECT authentication failed (diagnostics suppressed)"
            );
            return;
        }

        let diagnostics = crate::auth::jwt_failure_diagnostics(compact, &self.auth_claims_config);
        let mut error_characters = error.chars();
        let mut bounded_error = error_characters
            .by_ref()
            .take(MAX_LOGGED_ERROR_CHARS)
            .collect::<String>();
        if error_characters.next().is_some() {
            bounded_error.push_str("...");
        }

        error!(
            session_id,
            stage,
            error = ?bounded_error,
            jwt_fingerprint = %diagnostics.token_fingerprint,
            jwt_algorithm = ?diagnostics.algorithm,
            jwt_key_id = ?diagnostics.key_id,
            jwt_payload_status = %diagnostics.payload_status,
            jwt_issuer = ?diagnostics.issuer,
            jwt_audience = ?diagnostics.audience,
            jwt_exp = ?diagnostics.expires_at,
            jwt_nbf = ?diagnostics.not_before,
            jwt_expected_permission_sources = ?diagnostics.expected_permission_sources,
            jwt_presented_permission_sources = ?diagnostics.presented_permission_sources,
            "Ingress: CONNECT authentication failed"
        );
    }

    pub(super) async fn authenticate_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &Bytes,
        should_notify_handler: bool,
    ) -> Result<AuthenticatedFrame, IngressDecision> {
        // Anonymous sessions do not need the preliminary read used to decide
        // whether JWT verification must run. The mutable lookup below still
        // validates that the session exists and initializes it atomically.
        let needs_authentication = self.auth_required && self.needs_authentication(session_id)?;
        let connect = if msg_type == crate::protocol::tlv::MessageType::CONNECT {
            Some(self.decode_connect(session_id, channel_id, payload)?)
        } else {
            None
        };
        let verified_auth = if needs_authentication && self.auth_required {
            let Some(connect) = connect.as_ref() else {
                warn!(session_id = session_id, channel = ?channel_id, msg_type = msg_type.as_u16(), "Ingress: unauthenticated, CONNECT required");
                return Err(IngressDecision::Close(
                    "unauthenticated: connect required".to_string(),
                ));
            };
            Some(self.verify_connect(session_id, connect).await?)
        } else {
            None
        };
        let service_name = connect
            .as_ref()
            .map(|connect| connect.service_name.clone())
            .filter(|name| !name.is_empty());

        let Some(mut entry) = self.registry.sessions.get_mut(&session_id) else {
            warn!(
                session_id = session_id,
                "Ingress: frame for unknown session"
            );
            return Err(IngressDecision::Close(format!(
                "unknown session: {session_id}"
            )));
        };

        let mut notify_frame = None;
        let session_established = !entry.authenticated;
        if !entry.authenticated {
            if self.auth_required {
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

        Ok(AuthenticatedFrame {
            route_family: entry.route_family,
            notify_frame,
            session_established,
            service_name,
        })
    }

    /// Parse a CONNECT body and settle the wire version. Any failure is
    /// answered with `CONNECT_REJECTED` before the session closes.
    fn decode_connect(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        payload: &Bytes,
    ) -> Result<crate::protocol::wire2::handshake::Connect, IngressDecision> {
        use crate::protocol::wire2::catalog::block;
        use crate::protocol::wire2::envelope::ErrorClass;

        if channel_id != ChannelId::Control {
            return Err(self.reject_connect(
                session_id,
                &ConnectRefusal {
                    code: crate::protocol::wire2::catalog::shared(
                        block::PROTOCOL,
                        ErrorClass::BadRequest,
                    )
                    .unwrap_or_default(),
                    message: "CONNECT must use the control channel".to_string(),
                },
            ));
        }
        let connect = match crate::protocol::wire2::handshake::Connect::decode(payload) {
            Ok(connect) => connect,
            Err(error) => {
                return Err(self.reject_connect(
                    session_id,
                    &ConnectRefusal {
                        code: crate::protocol::wire2::catalog::shared(
                            block::PROTOCOL,
                            ErrorClass::BadRequest,
                        )
                        .unwrap_or_default(),
                        message: format!("malformed CONNECT: {error}"),
                    },
                ));
            }
        };
        let version = crate::protocol::wire2::handshake::negotiate_version(
            connect.min_version,
            connect.max_version,
            crate::protocol::wire2::WIRE_VERSION,
            crate::protocol::wire2::WIRE_VERSION,
        );
        if version.is_none() {
            return Err(self.reject_connect(
                session_id,
                &ConnectRefusal {
                    code: crate::protocol::wire2::catalog::VERSION_MISMATCH,
                    message: format!(
                        "client speaks wire {}..{}, broker speaks {}",
                        connect.min_version,
                        connect.max_version,
                        crate::protocol::wire2::WIRE_VERSION
                    ),
                },
            ));
        }
        Ok(connect)
    }

    /// Send `CONNECT_REJECTED` to the session's inbox and produce the close
    /// decision that follows it.
    fn reject_connect(&self, session_id: u64, refusal: &ConnectRefusal) -> IngressDecision {
        if let (Some(router), Some(entry)) = (&self.router, self.registry.sessions.get(&session_id))
        {
            super::domain_frame_dispatcher::announce_connect_rejected(
                router,
                session_id,
                entry.route_family,
                refusal.code,
                &refusal.message,
            );
        }
        IngressDecision::Close(format!("connect rejected: {}", refusal.message))
    }

    fn needs_authentication(&self, session_id: u64) -> Result<bool, IngressDecision> {
        if let Some(entry) = self.registry.sessions.get(&session_id) {
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

    async fn verify_connect(
        &self,
        session_id: u64,
        connect: &crate::protocol::wire2::handshake::Connect,
    ) -> Result<
        (
            SessionPermissions,
            crate::auth::Claims,
            crate::runtime::routing::RouteFamily,
        ),
        IngressDecision,
    > {
        use crate::protocol::wire2::catalog::block;
        use crate::protocol::wire2::envelope::ErrorClass;

        let compact = connect.jwt.as_str();
        debug!(
            session_id = session_id,
            jwt_len = compact.len(),
            "Ingress: verifying CONNECT JWT"
        );

        let auth_config = self
            .auth_config
            .clone()
            .unwrap_or_else(|| crate::auth::AuthConfig::from_env(true));

        match crate::api::authentication::verified_jwt_with_claims_config(
            compact,
            &auth_config,
            &self.auth_claims_config,
        )
        .await
        {
            Ok(verified) => {
                let route_family =
                    match self.resolve_authenticated_route_family(&verified.raw_claims) {
                        Ok(route_family) => route_family,
                        Err(error) => {
                            self.log_connect_failure(
                                session_id,
                                compact,
                                "route_family_resolution",
                                &error,
                            );
                            return Err(self.reject_connect(
                                session_id,
                                &ConnectRefusal {
                                    code: crate::protocol::wire2::catalog::FAMILY_NOT_PROVISIONED,
                                    message: format!("connect failed: {error}"),
                                },
                            ));
                        }
                    };
                Ok((verified.permissions, verified.claims, route_family))
            }
            Err(error) => {
                self.log_connect_failure(session_id, compact, "jwt_verification", &error);
                Err(self.reject_connect(
                    session_id,
                    &ConnectRefusal {
                        code: crate::protocol::wire2::catalog::shared(
                            block::PROTOCOL,
                            ErrorClass::Unauthorized,
                        )
                        .unwrap_or_default(),
                        message: format!("connect failed: {error}"),
                    },
                ))
            }
        }
    }

    pub(super) fn resolve_authenticated_route_family(
        &self,
        raw_claims: &crate::auth::RawClaims,
    ) -> Result<crate::runtime::routing::RouteFamily, String> {
        let route_family = self.route_family_resolver.resolve(raw_claims)?;
        if !self.route_families.contains(&route_family) {
            return Err(format!(
                "resolved route family {route_family} is not provisioned"
            ));
        }
        Ok(crate::runtime::routing::RouteFamily::new(route_family))
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
        self.registry.session_actors.insert(session_id, actor);

        if let Some(admin_read_model) = &self.registry.admin_read_model {
            admin_read_model.record_session_update(entry);
        }
    }

    fn apply_anonymous_session(&self, session_id: u64, entry: &mut SessionInfo) {
        let snapshot = crate::auth::default_anonymous_permissions();
        entry.permissions_snapshot = snapshot.clone();
        entry.authenticated = true;
        entry.route_family = crate::runtime::routing::RouteFamily::new(1);

        self.registry.session_actors.insert(
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
