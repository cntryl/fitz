use super::model::*;

impl MailboxSink for KvDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        tracing::debug!(
            domain = "kv",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "KV domain sink: received envelope"
        );

        let request = match Self::request_from_envelope(&envelope) {
            Some(request) => request,
            None => {
                tracing::warn!(
                    domain = "kv",
                    destination = ?envelope.destination(),
                    "Envelope payload was not KvClientRequest"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        let meta = request.meta;
        let operation_started = std::time::Instant::now();
        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());

        let parsed_frame = match request.frame {
            Ok(msg) => msg,
            Err(e) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(
                    domain = "kv",
                    session = meta.session_id,
                    msg_type = meta.message_type,
                    error = %e,
                    "Failed to parse KV message"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        tracing::debug!(
            domain = "kv",
            session = meta.session_id,
            channel = ?meta.channel,
            msg_type = meta.message_type,
            "Parsed KV message successfully"
        );

        if let KvClientFrame::Sub(sub_msg) = parsed_frame {
            let response = match sub_msg {
                crate::domains::kv::KvSubscriptionMessage::Subscribe {
                    family_id,
                    pattern,
                    session_id,
                    subscriber,
                } => {
                    let pattern_str = pattern.as_str().to_string();
                    let subscription_id = {
                        let mut families = self.families.lock();
                        let state = families
                            .entry(family_id.as_u64())
                            .or_insert_with(RoutedSubscriptionSet::new);

                        if let Some(existing_id) =
                            state.find_existing_id(session_id, pattern_str.as_str())
                        {
                            existing_id
                        } else {
                            let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                            state.insert(
                                family_id,
                                KvSubscription {
                                    pattern: crate::runtime::matcher::Pattern::new(
                                        pattern_str.as_str(),
                                    ),
                                    session_id,
                                    subscription_id: new_id,
                                    subscriber,
                                },
                            );
                            new_id
                        }
                    };
                    crate::domains::kv::KvResponse::SubscribeOk { subscription_id }
                }
                crate::domains::kv::KvSubscriptionMessage::Unsubscribe {
                    family_id,
                    pattern,
                    session_id,
                    ..
                } => {
                    let mut families = self.families.lock();
                    let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                        state.remove_session_pattern(family_id, session_id, pattern.as_str());
                        state.is_empty()
                    } else {
                        false
                    };
                    if remove_family {
                        families.remove(&family_id.as_u64());
                    }
                    crate::domains::kv::KvResponse::UnsubscribeOk
                }
            };

            self.refresh_metrics_gauges();
            return self.route_kv_response(&envelope, meta, &response, request_started);
        }

        use crate::domains::kv::{KvError, KvMessage, KvResponse, TxMode};
        let kv_message = match parsed_frame {
            KvClientFrame::Op(msg) => msg,
            KvClientFrame::Sub(_) => unreachable!(),
        };
        let kv_message = self.apply_sync_write_options(kv_message);
        let session_id = meta.session_id;

        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            msg_type = meta.message_type,
            "KV deliver: getting or creating actor for session"
        );

        let (response, should_sync_admin_snapshot, commit_notification) = match &kv_message {
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
                                None,
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
                                (resp, true, None)
                            } else {
                                (resp, false, None)
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
                            (resp, true, None)
                        } else {
                            (resp, false, None)
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
                let mutation_count = actor.mutation_count_for_tx(*tx_id).unwrap_or(0);
                let resp = actor.handle(kv_message.clone());
                if let KvResponse::CommitOk = resp {
                    let lock_key = self.tx_to_resource.lock().remove(&(session_id, *tx_id));
                    if let Some(k) = lock_key {
                        self.resource_locks.lock().remove(&k);
                        let notify = (mutation_count > 0).then_some((k, mutation_count));
                        (resp, true, notify)
                    } else {
                        (resp, true, None)
                    }
                } else {
                    crate::observability::counter_inc("fitz_kv_commits_failed_total");
                    (resp, false, None)
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
                    crate::observability::counter_inc("fitz_kv_rollbacks_total");
                    (resp, true, None)
                } else {
                    (resp, false, None)
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
                    msg_type = meta.message_type,
                    "Calling actor.handle() for operation"
                );
                (actor.handle(kv_message.clone()), false, None)
            }
        };
        if matches!(
            &response,
            crate::domains::kv::KvResponse::Error {
                error: crate::domains::kv::KvError::InvalidTxId,
                ..
            }
        ) {
            crate::observability::counter_inc("fitz_kv_invalid_transaction_rejects_total");
        }
        match (&kv_message, &response) {
            (KvMessage::Get { tx_id, .. }, crate::domains::kv::KvResponse::GetResult { .. })
            | (KvMessage::Scan { tx_id, .. }, crate::domains::kv::KvResponse::ScanResult { .. }) => {
                if let Some(resource_key) = self.resource_key_for_tx(session_id, *tx_id) {
                    self.record_read_latency(&resource_key, operation_started);
                }
            }
            (KvMessage::Commit { .. }, crate::domains::kv::KvResponse::CommitOk) => {
                if let Some((resource_key, _)) = commit_notification.as_ref() {
                    self.record_write_latency(resource_key, operation_started);
                }
            }
            _ => {}
        }
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }
        if let Some((resource_key, mutation_count)) = commit_notification {
            self.route_kv_notification(&resource_key, mutation_count);
        }

        tracing::debug!(
            domain = "kv",
            session = meta.session_id,
            response = ?std::mem::discriminant(&response),
            "KV actor returned response"
        );

        self.route_kv_response(&envelope, meta, &response, request_started)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl KvDomainSink {
    fn request_from_envelope(envelope: &Envelope) -> Option<KvClientRequest> {
        if let Some(request) = envelope.payload::<KvClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                Self::session_inbox_address(frame_ctx.route_family, frame_ctx.session_id)
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::protocol::kv::parse_frame(
                &frame_ctx,
                &frame_ctx.payload,
                frame_ctx.route_family,
                frame_ctx.session_id,
                subscriber,
            )
            .map(|frame| match frame {
                crate::protocol::kv::ParsedKvFrame::Op(message) => KvClientFrame::Op(message),
                crate::protocol::kv::ParsedKvFrame::Sub(message) => KvClientFrame::Sub(message),
            });
            Some(KvClientRequest::new(meta, parsed))
        }

        #[cfg(not(test))]
        {
            None
        }
    }
}

#[cfg(test)]
fn test_client_channel_from_protocol(
    channel: crate::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::protocol::frame::ChannelId::Control => crate::runtime::ClientChannel::Control,
        crate::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::protocol::frame::ChannelId::Internal => crate::runtime::ClientChannel::Internal,
    }
}
