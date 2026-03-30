use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

struct SinkLeaseState {
    owner_id: String,
    fencing_token: u64,
    expiry: std::time::Instant,
}

pub struct LeaseDomainSink {
    leases: Mutex<HashMap<crate::domains::lease::protocol::LeaseKey, SinkLeaseState>>,
    next_token: AtomicU64,
    router: Arc<Router>,
    active: AtomicBool,
    families: Mutex<HashMap<u64, LeaseFamilyState>>,
    next_sub_id: AtomicU64,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
}

#[derive(Debug, Clone)]
struct LeaseSubscription {
    pattern_str: String,
    session_id: u64,
    route_address: crate::runtime::routing::RouteAddress,
    subscription_id: u64,
}

#[derive(Default)]
struct LeaseFamilyState {
    subscriptions: HashMap<u64, LeaseSubscription>,
    index: crate::runtime::SubscriptionIndex,
}

impl LeaseDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(1),
            router,
            active: AtomicBool::new(true),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            admin_read_model,
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn sync_admin_snapshot(&self) {
        let now = std::time::Instant::now();
        let leases = self
            .leases
            .lock()
            .iter()
            .map(|(key, state)| crate::api::admin::LeaseInfo {
                realm: key.realm.clone(),
                area: key.area.clone(),
                resource: key.resource.clone(),
                owner_session_id: state.owner_id.clone(),
                acquired_at: Utc::now().to_rfc3339(),
                expires_at: Utc::now()
                    .checked_add_signed(chrono::TimeDelta::seconds(
                        state.expiry.saturating_duration_since(now).as_secs() as i64,
                    ))
                    .unwrap_or_else(Utc::now)
                    .to_rfc3339(),
                renewals: 0,
                fencing_token: state.fencing_token,
            })
            .collect();
        self.admin_read_model.replace_leases(leases);
    }

    pub fn cleanup_session(&self, session_id: u64) {
        let owner_prefix = format!("session:{}", session_id);
        let mut leases = self.leases.lock();
        let count_before = leases.len();
        leases.retain(|_key, state| !state.owner_id.starts_with(&owner_prefix));
        let count_removed = count_before - leases.len();

        tracing::debug!(
            domain = "lease",
            session = session_id,
            count_removed = count_removed,
            "Lease: released all leases for disconnected session"
        );

        self.unsubscribe_all(session_id);
        drop(leases);
        self.sync_admin_snapshot();
    }

    pub fn lease_count(&self) -> usize {
        self.leases.lock().len()
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(|state| state.subscriptions.len())
            .sum()
    }

    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.families.lock();

        if let Some(family_state) = families.get(&family_id) {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let matches = family_state.index.match_all_with_capacity(
                event.family_id,
                &event.route,
                family_state.subscriptions.len(),
            );
            for sub_id in matches {
                if let Some(sub) = family_state.subscriptions.get(&sub_id.0) {
                    let notify_payload = crate::protocol::lease_codec::encode_notify_into(
                        &mut payload_encoder,
                        sub.subscription_id,
                        event.route.as_str(),
                        &event.payload,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub,
                        crate::protocol::tlv::MessageType::new(409),
                        bytes::Bytes::from(notify_payload),
                        event.family_id,
                    );

                    let notify_envelope = Envelope::new(sub.route_address.clone(), notify_ctx);
                    let _ = self.router.route(notify_envelope);
                }
            }
        }
        Ok(())
    }

    fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            let removed_ids: Vec<u64> = state
                .subscriptions
                .iter()
                .filter_map(|(sub_id, sub)| (sub.session_id == session_id).then_some(*sub_id))
                .collect();
            for sub_id in removed_ids {
                if let Some(sub) = state.subscriptions.remove(&sub_id) {
                    let pattern = crate::runtime::routing::Route::new(sub.pattern_str.clone());
                    state.index.remove(
                        crate::runtime::routing::RouteFamily::new(*family_id),
                        &pattern,
                        crate::runtime::SubscriptionId(sub_id),
                    );
                }
            }
        }
    }

    fn next_fencing_token(&self) -> u64 {
        self.next_token.fetch_add(1, Ordering::Relaxed)
    }

    fn handle_acquire(
        &self,
        key: crate::domains::lease::protocol::LeaseKey,
        owner_id: String,
        ttl_secs: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::{Duration, Instant};

        let now = Instant::now();
        let ttl = Duration::from_secs(ttl_secs);
        let mut leases = self.leases.lock();

        match leases.get(&key) {
            None => {
                let token = self.next_fencing_token();
                leases.insert(
                    key,
                    SinkLeaseState {
                        owner_id,
                        fencing_token: token,
                        expiry: now + ttl,
                    },
                );
                LeaseResponse::Acquired {
                    fencing_token: token,
                }
            }
            Some(state) => {
                if state.expiry <= now {
                    let token = self.next_fencing_token();
                    leases.insert(
                        key,
                        SinkLeaseState {
                            owner_id,
                            fencing_token: token,
                            expiry: now + ttl,
                        },
                    );
                    LeaseResponse::Acquired {
                        fencing_token: token,
                    }
                } else if state.owner_id == owner_id {
                    LeaseResponse::AlreadyHeld {
                        fencing_token: state.fencing_token,
                    }
                } else {
                    LeaseResponse::HeldByOther {
                        current_owner: state.owner_id.clone(),
                    }
                }
            }
        }
    }

    fn handle_extend(
        &self,
        key: crate::domains::lease::protocol::LeaseKey,
        owner_id: String,
        fencing_token: u64,
        ttl_secs: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::{Duration, Instant};

        let now = Instant::now();
        let ttl = Duration::from_secs(ttl_secs);
        let mut leases = self.leases.lock();

        match leases.get_mut(&key) {
            None => LeaseResponse::NotHeld,
            Some(state) => {
                if state.expiry <= now {
                    LeaseResponse::Expired
                } else if state.owner_id != owner_id {
                    LeaseResponse::NotHeld
                } else if state.fencing_token != fencing_token {
                    LeaseResponse::Fenced {
                        current_token: state.fencing_token,
                    }
                } else {
                    let new_token = self.next_fencing_token();
                    state.expiry = now + ttl;
                    state.fencing_token = new_token;
                    LeaseResponse::Extended {
                        fencing_token: new_token,
                    }
                }
            }
        }
    }

    fn handle_release(
        &self,
        key: crate::domains::lease::protocol::LeaseKey,
        owner_id: String,
        fencing_token: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::Instant;

        let now = Instant::now();
        let mut leases = self.leases.lock();

        match leases.get(&key) {
            None => LeaseResponse::NotHeld,
            Some(state) => {
                if state.expiry <= now {
                    LeaseResponse::Expired
                } else if state.owner_id != owner_id {
                    LeaseResponse::NotHeld
                } else if state.fencing_token != fencing_token {
                    LeaseResponse::Fenced {
                        current_token: state.fencing_token,
                    }
                } else {
                    leases.remove(&key);
                    LeaseResponse::Released
                }
            }
        }
    }

    fn handle_query(
        &self,
        key: crate::domains::lease::protocol::LeaseKey,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;
        use std::time::Instant;

        let now = Instant::now();
        let leases = self.leases.lock();

        match leases.get(&key) {
            None => LeaseResponse::NotFound,
            Some(state) => {
                if state.expiry <= now {
                    LeaseResponse::Expired
                } else {
                    let expires_in = state.expiry.duration_since(now);
                    LeaseResponse::Status {
                        owner_id: state.owner_id.clone(),
                        fencing_token: state.fencing_token,
                        expires_in_secs: expires_in.as_secs(),
                        pending_waiters: 0,
                    }
                }
            }
        }
    }
}

impl MailboxSink for LeaseDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        tracing::debug!(
            domain = "lease",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Lease domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "lease", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let lease_msg = match crate::protocol::lease_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
        ) {
            Ok(msg) => {
                tracing::debug!(
                    domain = "lease",
                    session = frame_ctx.session_id,
                    msg_type = frame_ctx.msg_type.as_u16(),
                    "Lease: parsed message successfully"
                );
                msg
            }
            Err(e) => {
                tracing::warn!(domain = "lease", error = %e, "Failed to parse lease message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::lease::protocol::{LeaseKey, LeaseMessage, LeaseResponse};

        let session_prefix = frame_ctx.session_id.to_string();
        let effective_owner = |owner_id: String| {
            if owner_id.is_empty() {
                let mut scoped = String::with_capacity("session:".len() + session_prefix.len());
                scoped.push_str("session:");
                scoped.push_str(&session_prefix);
                scoped
            } else {
                let mut scoped = String::with_capacity(
                    "session::".len() + session_prefix.len() + owner_id.len(),
                );
                scoped.push_str("session:");
                scoped.push_str(&session_prefix);
                scoped.push(':');
                scoped.push_str(&owner_id);
                scoped
            }
        };

        let domain_response = match lease_msg {
            LeaseMessage::Acquire {
                family_id,
                route,
                owner_id,
                ttl_secs,
                wait_seconds: _,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => self.handle_acquire(key, effective_owner(owner_id), ttl_secs),
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Extend {
                family_id,
                route,
                owner_id,
                fencing_token,
                ttl_secs,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => {
                    self.handle_extend(key, effective_owner(owner_id), fencing_token, ttl_secs)
                }
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Release {
                family_id,
                route,
                owner_id,
                fencing_token,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => {
                    let resp =
                        self.handle_release(key.clone(), effective_owner(owner_id), fencing_token);
                    if let crate::domains::lease::protocol::LeaseResponse::Released = resp {
                        let route = key.to_route();
                        let event = crate::runtime::DomainPublishEvent::new(
                            key.family,
                            route,
                            bytes::Bytes::new(),
                        );
                        let _ = self.handle_domain_publish(&event);
                    }
                    resp
                }
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Query { family_id, route } => {
                match LeaseKey::from_route(family_id, &route) {
                    Some(key) => self.handle_query(key),
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Tick => {
                let now = std::time::Instant::now();
                let mut leases = self.leases.lock();
                let expired_keys: Vec<crate::domains::lease::protocol::LeaseKey> = leases
                    .iter()
                    .filter(|(_, state)| state.expiry <= now)
                    .map(|(k, _)| k.clone())
                    .collect();

                for key in &expired_keys {
                    leases.remove(key);
                    let route = key.to_route();
                    let event = crate::runtime::DomainPublishEvent::new(
                        key.family,
                        route,
                        bytes::Bytes::new(),
                    );
                    let _ = self.handle_domain_publish(&event);
                }
                return Ok(());
            }
            LeaseMessage::Subscribe { family_id, pattern } => {
                let route_address = match envelope.source() {
                    Some(src) => src,
                    None => {
                        let error_bytes = vec![1u8];
                        let response_ctx = FrameContext::new(
                            frame_ctx.session_id,
                            frame_ctx.channel_id,
                            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                            bytes::Bytes::from(error_bytes),
                            frame_ctx.route_family,
                        );
                        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                            let _ = self.router.route(response_envelope);
                        }
                        return Ok(());
                    }
                };

                let subscription_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                let family_id_u64 = family_id.as_u64();

                let mut families = self.families.lock();
                let family_state = families.entry(family_id_u64).or_default();

                if let Some(existing) = family_state
                    .subscriptions
                    .values()
                    .find(|s| s.session_id == frame_ctx.session_id && s.pattern_str == pattern)
                {
                    let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                        &mut payload_encoder,
                        &LeaseResponse::SubscribeOk {
                            subscription_id: existing.subscription_id,
                        },
                    );
                    let response_ctx = FrameContext::new(
                        frame_ctx.session_id,
                        frame_ctx.channel_id,
                        crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                        bytes::Bytes::from(response_bytes),
                        frame_ctx.route_family,
                    );
                    if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                        let _ = self.router.route(response_envelope);
                    }
                    return Ok(());
                }

                let pattern_route = crate::runtime::routing::Route::new(pattern.as_str());
                family_state.index.insert(
                    family_id,
                    &pattern_route,
                    crate::runtime::SubscriptionId(subscription_id),
                );
                family_state.subscriptions.insert(
                    subscription_id,
                    LeaseSubscription {
                        pattern_str: pattern,
                        session_id: frame_ctx.session_id,
                        route_address: route_address.clone(),
                        subscription_id,
                    },
                );

                let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                    &mut payload_encoder,
                    &LeaseResponse::SubscribeOk { subscription_id },
                );
                let response_ctx = FrameContext::new(
                    frame_ctx.session_id,
                    frame_ctx.channel_id,
                    crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                    bytes::Bytes::from(response_bytes),
                    frame_ctx.route_family,
                );
                if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                    let _ = self.router.route(response_envelope);
                }
                return Ok(());
            }
            LeaseMessage::Unsubscribe { family_id, pattern } => {
                let family_id_u64 = family_id.as_u64();
                let mut families = self.families.lock();

                if let Some(family_state) = families.get_mut(&family_id_u64) {
                    let pattern_route = crate::runtime::routing::Route::new(pattern.as_str());
                    let removed_ids: Vec<u64> = family_state
                        .subscriptions
                        .iter()
                        .filter_map(|(sub_id, sub)| {
                            (sub.session_id == frame_ctx.session_id && sub.pattern_str == pattern)
                                .then_some(*sub_id)
                        })
                        .collect();
                    for sub_id in removed_ids {
                        if family_state.subscriptions.remove(&sub_id).is_some() {
                            family_state.index.remove(
                                family_id,
                                &pattern_route,
                                crate::runtime::SubscriptionId(sub_id),
                            );
                        }
                    }
                }

                let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                    &mut payload_encoder,
                    &LeaseResponse::UnsubscribeOk,
                );
                let response_ctx = FrameContext::new(
                    frame_ctx.session_id,
                    frame_ctx.channel_id,
                    crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                    bytes::Bytes::from(response_bytes),
                    frame_ctx.route_family,
                );
                if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                    let _ = self.router.route(response_envelope);
                }
                return Ok(());
            }
            LeaseMessage::UnsubscribeAll => {
                let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
                    &mut payload_encoder,
                    &LeaseResponse::UnsubscribeOk,
                );
                let response_ctx = FrameContext::new(
                    frame_ctx.session_id,
                    frame_ctx.channel_id,
                    crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                    bytes::Bytes::from(response_bytes),
                    frame_ctx.route_family,
                );
                if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                    let _ = self.router.route(response_envelope);
                }
                return Ok(());
            }
        };

        let response_bytes = crate::protocol::lease_codec::encode_domain_response_into(
            &mut payload_encoder,
            &domain_response,
        );
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
            frame_ctx.route_family,
        );
        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let _ = self.router.route(response_envelope);
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn should_create_lease_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = LeaseDomainSink::new(router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }
}
