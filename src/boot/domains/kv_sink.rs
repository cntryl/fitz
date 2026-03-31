use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct KvResourceLockKey {
    family_id: u64,
    realm: String,
    area: String,
    resource: String,
}

impl KvResourceLockKey {
    fn new(family_id: u64, realm: &str, area: &str, resource: &str) -> Self {
        Self {
            family_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
        }
    }
}

pub struct KvDomainSink {
    store: Arc<cntryl_midge::Engine>,
    actors: Arc<Mutex<HashMap<u64, crate::domains::kv::KvActor>>>,
    resource_locks: Mutex<HashMap<KvResourceLockKey, u64>>,
    tx_to_resource: Mutex<HashMap<(u64, u64), KvResourceLockKey>>,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
}

impl KvDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            store,
            actors: Arc::new(Mutex::new(HashMap::new())),
            resource_locks: Mutex::new(HashMap::new()),
            tx_to_resource: Mutex::new(HashMap::new()),
            router,
            admin_read_model,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn sync_admin_snapshot(&self) {
        let transactions = self
            .tx_to_resource
            .lock()
            .iter()
            .map(
                |((session_id, tx_id), resource_key)| crate::api::admin::KvTransaction {
                    tx_id: *tx_id,
                    realm: resource_key.realm.clone(),
                    area: resource_key.area.clone(),
                    resource: resource_key.resource.clone(),
                    mode: format!("session:{session_id}:readwrite"),
                    started_at: Utc::now().to_rfc3339(),
                    operations_count: 0,
                    idle_seconds: 0,
                },
            )
            .collect();
        self.admin_read_model.replace_kv_transactions(transactions);
    }

    pub fn cleanup_session(&self, session_id: u64) {
        self.actors.lock().remove(&session_id);

        {
            let mut locks = self.resource_locks.lock();
            locks.retain(|_key, holder_id| *holder_id != session_id);
        }

        {
            let mut tx_map = self.tx_to_resource.lock();
            tx_map.retain(|(sid, _tx_id), _key| *sid != session_id);
        }

        tracing::debug!(
            domain = "kv",
            session = session_id,
            "All KV transactions and resource locks released for session (disconnect cleanup)"
        );
        self.sync_admin_snapshot();
    }

    pub fn active_transaction_count(&self) -> usize {
        self.tx_to_resource.lock().len()
    }
}

impl MailboxSink for KvDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }

        tracing::debug!(
            domain = "kv",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "KV domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => match envelope.payload::<bytes::Bytes>() {
                Some(_bytes) => {
                    tracing::warn!(
                        domain = "kv",
                        destination = ?envelope.destination(),
                        "Envelope payload was Bytes, expected FrameContext - raw TLV not supported yet"
                    );
                    return Err(DeliveryError::ActorStopped);
                }
                None => {
                    tracing::warn!(
                        domain = "kv",
                        destination = ?envelope.destination(),
                        "Envelope payload was neither FrameContext nor Bytes"
                    );
                    return Err(DeliveryError::ActorStopped);
                }
            },
        };

        let kv_message = match crate::protocol::kv::parse_request(
            frame_ctx.msg_type.as_u16(),
            frame_ctx.route_family,
            &frame_ctx.payload,
        ) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    msg_type = frame_ctx.msg_type.as_u16(),
                    error = %e,
                    "Failed to parse KV message"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        tracing::debug!(
            domain = "kv",
            session = frame_ctx.session_id,
            channel = ?frame_ctx.channel_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "Parsed KV message successfully"
        );

        use crate::domains::kv::{KvError, KvMessage, KvResponse, TxMode};
        let session_id = frame_ctx.session_id;

        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "KV deliver: getting or creating actor for session"
        );

        let (response, should_sync_admin_snapshot) = match &kv_message {
            KvMessage::Begin {
                route_family,
                realm,
                area,
                resource,
                mode,
                ..
            } if *mode == TxMode::ReadWrite => {
                let lock_key = KvResourceLockKey::new(route_family.as_u64(), realm, area, resource);
                {
                    let locks = self.resource_locks.lock();
                    if let Some(&holder) = locks.get(&lock_key) {
                        if holder != session_id {
                            drop(locks);
                            (
                                KvResponse::Error {
                                    error: KvError::Conflict(
                                        "resource locked by another session".to_string(),
                                    ),
                                },
                                false,
                            )
                        } else {
                            drop(locks);
                            let mut actors = self.actors.lock();
                            let actor = actors.entry(session_id).or_insert_with(|| {
                                tracing::trace!(
                                    domain = "kv",
                                    session_id = session_id,
                                    "Creating new KvActor instance"
                                );
                                crate::domains::kv::KvActor::new(self.store.clone())
                            });
                            tracing::trace!(
                                domain = "kv",
                                session_id = session_id,
                                "Calling actor.handle() for BEGIN (ReadWrite)"
                            );
                            let resp = actor.handle(kv_message.clone());
                            if let KvResponse::BeginOk { tx_id } = resp {
                                tracing::trace!(
                                    domain = "kv",
                                    session_id = session_id,
                                    tx_id = tx_id,
                                    "BEGIN succeeded, storing resource lock"
                                );
                                self.resource_locks
                                    .lock()
                                    .insert(lock_key.clone(), session_id);
                                self.tx_to_resource
                                    .lock()
                                    .insert((session_id, tx_id), lock_key);
                                (resp, true)
                            } else {
                                (resp, false)
                            }
                        }
                    } else {
                        drop(locks);
                        let mut actors = self.actors.lock();
                        let actor = actors.entry(session_id).or_insert_with(|| {
                            tracing::trace!(
                                domain = "kv",
                                session_id = session_id,
                                "Creating new KvActor instance"
                            );
                            crate::domains::kv::KvActor::new(self.store.clone())
                        });
                        tracing::trace!(
                            domain = "kv",
                            session_id = session_id,
                            "Calling actor.handle() for BEGIN (ReadWrite, acquiring lock)"
                        );
                        let resp = actor.handle(kv_message.clone());
                        if let KvResponse::BeginOk { tx_id } = resp {
                            tracing::trace!(
                                domain = "kv",
                                session_id = session_id,
                                tx_id = tx_id,
                                "BEGIN succeeded, acquiring resource lock"
                            );
                            self.resource_locks
                                .lock()
                                .insert(lock_key.clone(), session_id);
                            self.tx_to_resource
                                .lock()
                                .insert((session_id, tx_id), lock_key);
                            (resp, true)
                        } else {
                            (resp, false)
                        }
                    }
                }
            }
            KvMessage::Commit { tx_id } => {
                let mut actors = self.actors.lock();
                let actor = actors.entry(session_id).or_insert_with(|| {
                    tracing::trace!(
                        domain = "kv",
                        session_id = session_id,
                        "Creating new KvActor instance (COMMIT)"
                    );
                    crate::domains::kv::KvActor::new(self.store.clone())
                });
                tracing::trace!(
                    domain = "kv",
                    session_id = session_id,
                    tx_id = tx_id,
                    "Calling actor.handle() for COMMIT"
                );
                let resp = actor.handle(kv_message.clone());
                if let KvResponse::CommitOk = resp {
                    let lock_key = self.tx_to_resource.lock().remove(&(session_id, *tx_id));
                    if let Some(k) = lock_key {
                        self.resource_locks.lock().remove(&k);
                    }
                    (resp, true)
                } else {
                    (resp, false)
                }
            }
            KvMessage::Rollback { tx_id } => {
                let mut actors = self.actors.lock();
                let actor = actors.entry(session_id).or_insert_with(|| {
                    tracing::trace!(
                        domain = "kv",
                        session_id = session_id,
                        "Creating new KvActor instance (ROLLBACK)"
                    );
                    crate::domains::kv::KvActor::new(self.store.clone())
                });
                tracing::trace!(
                    domain = "kv",
                    session_id = session_id,
                    tx_id = tx_id,
                    "Calling actor.handle() for ROLLBACK"
                );
                let resp = actor.handle(kv_message.clone());
                if let KvResponse::RollbackOk = resp {
                    let lock_key = self.tx_to_resource.lock().remove(&(session_id, *tx_id));
                    if let Some(k) = lock_key {
                        self.resource_locks.lock().remove(&k);
                    }
                    (resp, true)
                } else {
                    (resp, false)
                }
            }
            _ => {
                let mut actors = self.actors.lock();
                let actor = actors.entry(session_id).or_insert_with(|| {
                    tracing::trace!(
                        domain = "kv",
                        session_id = session_id,
                        "Creating new KvActor instance (other operation)"
                    );
                    crate::domains::kv::KvActor::new(self.store.clone())
                });
                tracing::trace!(
                    domain = "kv",
                    session_id = session_id,
                    msg_type = frame_ctx.msg_type.as_u16(),
                    "Calling actor.handle() for operation"
                );
                (actor.handle(kv_message), false)
            }
        };
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }

        tracing::debug!(
            domain = "kv",
            session = frame_ctx.session_id,
            response = ?std::mem::discriminant(&response),
            "KV actor returned response"
        );

        let response_bytes = crate::protocol::kv::encode_response(&response);
        tracing::trace!(
            domain = "kv",
            session = frame_ctx.session_id,
            response_len = response_bytes.len(),
            "KV response encoded"
        );

        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
            frame_ctx.route_family,
        );
        let response_envelope = match envelope.try_reply_to(response_ctx) {
            Some(env) => env,
            None => {
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    "Cannot route response: envelope has no source address"
                );
                return Ok(());
            }
        };

        match self.router.route(response_envelope) {
            Ok(_) => {
                tracing::debug!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    "KV message handled and response routed"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    error = ?e,
                    "Failed to route response"
                );
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::ChannelId;
    use crate::protocol::tlv::MessageType;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::runtime::Mailbox;
    use bytes::{BufMut, Bytes};
    use std::sync::Arc;

    fn encode_kv_begin(route: &str, mode: u8, durability: u8) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u8(mode);
        payload.put_u8(durability);
        Bytes::from(payload)
    }

    fn decode_kv_begin_tx_id(payload: &[u8]) -> u64 {
        let tx_id_bytes: [u8; 8] = payload[1..9]
            .try_into()
            .expect("begin response tx_id bytes");
        u64::from_be_bytes(tx_id_bytes)
    }

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
    }

    #[test]
    fn should_create_kv_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = KvDomainSink::new(store, router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_release_resource_lock_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let first_session_id = 7;
        let second_session_id = 8;
        let kv_route = "kv://acme/app/users";
        let kv_address = RouteAddress::new(family, Route::new(kv_route));
        let first_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let second_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let first_mailbox = Arc::new(Mailbox::new(8));
        let second_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(first_address.clone(), first_mailbox.clone());
        router.register(second_address.clone(), second_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = KvDomainSink::new(store, router, admin_read_model);

        sink.deliver(Envelope::from_route(
            first_address,
            kv_address.clone(),
            FrameContext::new(
                first_session_id,
                ChannelId::Sub,
                MessageType::new(100),
                encode_kv_begin(kv_route, 1, 0),
                family,
            ),
        ))
        .expect("begin first KV transaction");
        let first_begin_envelope = first_mailbox
            .receiver()
            .try_recv()
            .expect("first begin ack envelope");
        let first_begin_frame = first_begin_envelope
            .into_payload::<FrameContext>()
            .expect("first begin ack frame");
        let first_tx_id = decode_kv_begin_tx_id(&first_begin_frame.payload);
        assert_eq!(first_begin_frame.payload[0], 0);
        assert!(first_tx_id > 0);
        assert_eq!(sink.active_transaction_count(), 1);
        assert_eq!(sink.resource_locks.lock().len(), 1);
        drain_mailbox(&first_mailbox);

        // Act
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("kv://cleanup")),
            crate::runtime::SessionCleanup {
                session_id: first_session_id,
            },
        ))
        .expect("cleanup first KV session");
        assert_eq!(sink.active_transaction_count(), 0);
        assert!(sink.resource_locks.lock().is_empty());

        sink.deliver(Envelope::from_route(
            second_address,
            kv_address,
            FrameContext::new(
                second_session_id,
                ChannelId::Sub,
                MessageType::new(100),
                encode_kv_begin(kv_route, 1, 0),
                family,
            ),
        ))
        .expect("begin second KV transaction");

        // Assert
        let second_begin_envelope = second_mailbox
            .receiver()
            .try_recv()
            .expect("second begin ack envelope");
        let second_begin_frame = second_begin_envelope
            .into_payload::<FrameContext>()
            .expect("second begin ack frame");
        let second_tx_id = decode_kv_begin_tx_id(&second_begin_frame.payload);
        assert_eq!(second_begin_frame.payload[0], 0);
        assert!(second_tx_id > 0);
        assert_eq!(sink.active_transaction_count(), 1);
        assert_eq!(sink.resource_locks.lock().len(), 1);
        assert!(first_mailbox.receiver().try_recv().is_err());
    }
}
