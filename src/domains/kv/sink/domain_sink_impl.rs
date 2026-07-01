use super::model::{
    AdminKvPrefixScanResult, AdminKvRowsRequest, AdminKvRowsResult, Arc, AtomicBool, AtomicU64,
    Bytes, DeliveryError, Envelope, HashMap, KvDomainSink, KvResourceLockKey, Mutex, Ordering,
    RoutedSubscriptionSet, Router, Utc, ADMIN_INVENTORY_REFRESH_LIMIT,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;

impl KvDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            store,
            actors: Arc::new(Mutex::new(HashMap::new())),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
            projection: crate::domains::kv::projection::KvAdminProjection::new(admin_read_model),
            metrics: None,
            sync_write_options: cntryl_midge::WriteOptions::sync(),
            active: AtomicBool::new(true),
        }
    }

    #[must_use]
    pub fn with_sync_write_options(mut self, write_options: cntryl_midge::WriteOptions) -> Self {
        self.sync_write_options = write_options;
        self
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.metrics = Some(crate::domains::kv::KvMetrics::new(collector));
        self.refresh_metrics_gauges();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    /// Build an admin inventory snapshot for the requested route family scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying storage inventory scan fails.
    pub fn admin_inventory(
        &self,
        family: Option<crate::runtime::routing::RouteFamily>,
    ) -> Result<Vec<crate::control::admin::KvResourceInventoryEntry>, String> {
        let families = if let Some(family) = family {
            vec![family.id()]
        } else {
            self.store
                .list_column_families()
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|handle| handle.id())
                .filter(|family_id| *family_id != 0)
                .collect::<Vec<_>>()
        };

        let mut entries = Vec::new();
        for family_id in families {
            entries.extend(self.admin_inventory_for_family(u64::from(family_id))?);
        }
        entries.sort_by(|left, right| {
            (
                left.route_family,
                left.realm.as_str(),
                left.area.as_str(),
                left.resource.as_str(),
            )
                .cmp(&(
                    right.route_family,
                    right.realm.as_str(),
                    right.area.as_str(),
                    right.resource.as_str(),
                ))
        });
        Ok(entries)
    }

    /// Read one admin inventory entry for a specific KV resource.
    ///
    /// # Errors
    ///
    /// Returns an error when storage reads, estimate refreshes, or estimate
    /// decoding fails.
    pub fn admin_inventory_resource(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<crate::control::admin::KvResourceInventoryEntry>, String> {
        let family_id = route_family.as_u64();
        let column_family =
            crate::domains::kv::KvActor::resolve_column_family(route_family, resource)?;
        let key = crate::domains::kv::KvActor::inventory_metadata_key(realm, area, resource);
        let tx = self
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;

        let estimate = if let Some(value) = tx.get(&key).map_err(|error| error.to_string())? {
            crate::domains::kv::KvActor::decode_inventory_estimate(&value)?
        } else {
            let refreshed =
                self.refresh_inventory_estimate(family_id, realm, area, resource, false)?;
            if refreshed.estimated_record_count == 0 && refreshed.estimate_complete {
                return Ok(None);
            }
            refreshed
        };
        let estimate = if estimate.estimate_complete {
            estimate
        } else {
            self.refresh_inventory_estimate(family_id, realm, area, resource, true)?
        };

        Ok(Some(self.inventory_entry_from_estimate(
            family_id, realm, area, resource, estimate,
        )))
    }

    /// Read one committed KV value directly from storage for admin inspection.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage transaction or read fails.
    pub fn admin_get_committed_value(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        let started_at = std::time::Instant::now();
        let column_family =
            crate::domains::kv::KvActor::resolve_column_family(route_family, resource)?;
        let tx = self
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let prefix = crate::domains::kv::KvActor::realm_resource_prefix(realm, area, resource);
        let scoped_key = crate::domains::kv::KvActor::encode_scoped_key(&prefix, key);
        let value = tx
            .get(&scoped_key)
            .map(|value| value.map(|value| value.as_ref().to_vec()))
            .map_err(|error| error.to_string())?;
        self.record_read_latency(
            &KvResourceLockKey::new(route_family.as_u64(), realm, area, resource),
            started_at,
        );
        Ok(value)
    }

    /// Scan a committed KV prefix directly from storage for admin inspection.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage transaction or scan fails.
    pub fn admin_scan_committed_prefix(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<AdminKvPrefixScanResult, String> {
        let started_at = std::time::Instant::now();
        let column_family =
            crate::domains::kv::KvActor::resolve_column_family(route_family, resource)?;
        let tx = self
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let resource_prefix =
            crate::domains::kv::KvActor::realm_resource_prefix(realm, area, resource);
        let scoped_prefix =
            crate::domains::kv::KvActor::encode_scoped_key(&resource_prefix, key_prefix);
        let query_limit = limit.saturating_add(1);
        let midge_query = cntryl_midge::Query::new()
            .prefix(Bytes::from(scoped_prefix.clone()))
            .start_key(Bytes::from(scoped_prefix.clone()))
            .end_key(Bytes::from(crate::domains::kv::KvActor::prefix_range_end(
                &scoped_prefix,
            )))
            .limit(query_limit);
        let mut iterator = tx.scan(&midge_query).map_err(|error| error.to_string())?;
        let mut rows = Vec::new();

        while let Some((scoped_key, value)) = iterator.next() {
            let Some(user_key) =
                crate::domains::kv::KvActor::strip_scoped_prefix(&resource_prefix, &scoped_key)
            else {
                continue;
            };
            rows.push((user_key, value.clone()));
        }

        let has_more = rows.len() > limit;
        rows.truncate(limit);
        self.record_read_latency(
            &KvResourceLockKey::new(route_family.as_u64(), realm, area, resource),
            started_at,
        );
        Ok((rows, has_more))
    }

    /// Scan committed KV rows with an optional pagination cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when cursor validation fails or the storage scan fails.
    pub fn admin_scan_committed_rows(
        &self,
        request: &AdminKvRowsRequest<'_>,
    ) -> Result<AdminKvRowsResult, String> {
        let started_at = std::time::Instant::now();
        if let Some(cursor) = request.cursor {
            if !cursor.starts_with(request.starts_with) {
                return Err("cursor must start with starts_with prefix".to_string());
            }
        }

        let column_family = crate::domains::kv::KvActor::resolve_column_family(
            request.route_family,
            request.resource,
        )?;
        let tx = self
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let resource_prefix = crate::domains::kv::KvActor::realm_resource_prefix(
            request.realm,
            request.area,
            request.resource,
        );
        let scoped_prefix =
            crate::domains::kv::KvActor::encode_scoped_key(&resource_prefix, request.starts_with);
        let scoped_start = request.cursor.map_or_else(
            || scoped_prefix.clone(),
            |cursor| crate::domains::kv::KvActor::encode_scoped_key(&resource_prefix, cursor),
        );
        let query_limit = request.limit.saturating_add(1);
        let midge_query = cntryl_midge::Query::new()
            .prefix(Bytes::from(scoped_prefix.clone()))
            .start_key(Bytes::from(scoped_start))
            .end_key(Bytes::from(crate::domains::kv::KvActor::prefix_range_end(
                &scoped_prefix,
            )))
            .limit(query_limit);
        let mut iterator = tx.scan(&midge_query).map_err(|error| error.to_string())?;
        let mut rows = Vec::new();

        while let Some((scoped_key, value)) = iterator.next() {
            let Some(user_key) =
                crate::domains::kv::KvActor::strip_scoped_prefix(&resource_prefix, &scoped_key)
            else {
                continue;
            };
            if request
                .cursor
                .is_some_and(|cursor| user_key.as_slice() <= cursor)
            {
                continue;
            }
            rows.push((user_key, value.clone()));
        }

        let has_more = rows.len() > request.limit;
        rows.truncate(request.limit);
        let next_cursor = if has_more {
            rows.last().map(|(key, _)| key.clone())
        } else {
            None
        };
        self.record_read_latency(
            &KvResourceLockKey::new(
                request.route_family.as_u64(),
                request.realm,
                request.area,
                request.resource,
            ),
            started_at,
        );
        Ok((rows, next_cursor, has_more))
    }

    pub(super) fn admin_inventory_for_family(
        &self,
        family_id: u64,
    ) -> Result<Vec<crate::control::admin::KvResourceInventoryEntry>, String> {
        let route_family = crate::runtime::routing::RouteFamily::new(family_id);
        let column_family = crate::domains::kv::KvActor::resolve_column_family(route_family, "")?;
        let tx = self
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let mut iterator = tx
            .scan(&cntryl_midge::Query::new())
            .map_err(|error| error.to_string())?;
        let mut discovered = Vec::new();

        while let Some((key, value)) = iterator.next() {
            let Some((realm, area, resource)) =
                crate::domains::kv::KvActor::parse_inventory_metadata_key(&key)
            else {
                continue;
            };
            let estimate = crate::domains::kv::KvActor::decode_inventory_estimate(&value)?;
            discovered.push((realm, area, resource, estimate));
        }

        drop(iterator);
        drop(tx);

        discovered
            .into_iter()
            .map(|(realm, area, resource, estimate)| {
                let estimate = if estimate.estimate_complete {
                    estimate
                } else {
                    self.refresh_inventory_estimate(family_id, &realm, &area, &resource, true)?
                };
                Ok(self
                    .inventory_entry_from_estimate(family_id, &realm, &area, &resource, estimate))
            })
            .collect()
    }

    pub(super) fn refresh_inventory_estimate(
        &self,
        family_id: u64,
        realm: &str,
        area: &str,
        resource: &str,
        persist_empty: bool,
    ) -> Result<crate::domains::kv::actor::KvInventoryEstimate, String> {
        let route_family = crate::runtime::routing::RouteFamily::new(family_id);
        let column_family =
            crate::domains::kv::KvActor::resolve_column_family(route_family, resource)?;
        let read_tx = self
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let resource_prefix =
            crate::domains::kv::KvActor::realm_resource_prefix(realm, area, resource);
        let query_limit = ADMIN_INVENTORY_REFRESH_LIMIT.saturating_add(1);
        let query = cntryl_midge::Query::new()
            .prefix(Bytes::from(resource_prefix.clone()))
            .start_key(Bytes::from(resource_prefix.clone()))
            .end_key(Bytes::from(crate::domains::kv::KvActor::prefix_range_end(
                &resource_prefix,
            )))
            .limit(query_limit);
        let mut iterator = read_tx.scan(&query).map_err(|error| error.to_string())?;
        let mut count = 0u64;
        let mut storage_bytes = 0u64;
        let mut has_more = false;

        while let Some((scoped_key, value)) = iterator.next() {
            if count >= u64::try_from(ADMIN_INVENTORY_REFRESH_LIMIT).unwrap_or(u64::MAX) {
                has_more = true;
                break;
            }
            let Some(user_key) =
                crate::domains::kv::KvActor::strip_scoped_prefix(&resource_prefix, &scoped_key)
            else {
                continue;
            };
            count = count.saturating_add(1);
            storage_bytes = storage_bytes.saturating_add(user_key.len() as u64);
            storage_bytes = storage_bytes.saturating_add(value.len() as u64);
        }

        let estimate_complete = !has_more;
        let estimate = crate::domains::kv::actor::KvInventoryEstimate {
            estimated_record_count: count,
            estimated_storage_bytes: storage_bytes,
            estimate_complete,
        };

        drop(iterator);
        drop(read_tx);

        if persist_empty || estimate.estimated_record_count > 0 || !estimate.estimate_complete {
            let mut write_tx = self
                .store
                .begin_tx(column_family, cntryl_midge::TransactionMode::ReadWrite)
                .map_err(|error| error.to_string())?;
            write_tx
                .put(
                    crate::domains::kv::KvActor::inventory_metadata_key(realm, area, resource),
                    crate::domains::kv::KvActor::encode_inventory_estimate(estimate),
                    None,
                )
                .map_err(|error| error.to_string())?;
            write_tx
                .commit(self.sync_write_options)
                .map_err(|error| error.to_string())?;
        }

        Ok(estimate)
    }

    pub(super) fn inventory_entry_from_estimate(
        &self,
        family_id: u64,
        realm: &str,
        area: &str,
        resource: &str,
        estimate: crate::domains::kv::actor::KvInventoryEstimate,
    ) -> crate::control::admin::KvResourceInventoryEntry {
        let resource_key = KvResourceLockKey::new(family_id, realm, area, resource);
        let (read_latency, write_latency) = self.latency_snapshots(&resource_key);
        crate::control::admin::KvResourceInventoryEntry {
            route_family: family_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            estimated_record_count: estimate.estimated_record_count,
            estimated_storage_bytes: estimate.estimated_storage_bytes,
            estimate_complete: estimate.estimate_complete,
            read_latency_avg_ms: read_latency.avg_ms,
            read_latency_p95_ms: read_latency.p95_ms,
            write_latency_avg_ms: write_latency.avg_ms,
            write_latency_p95_ms: write_latency.p95_ms,
            transactions_active: self.active_transactions_for_resource(&resource_key),
        }
    }

    pub(super) fn sync_admin_snapshot(&self) {
        let started_at = Utc::now().to_rfc3339();
        let transactions = self
            .actors
            .lock()
            .iter()
            .flat_map(|(session_id, actor)| {
                actor.active_transaction_scopes().into_iter().map(
                    |(tx_id, family_id, realm, area, resource)| {
                        crate::control::admin::KvTransaction::snapshot(
                            family_id,
                            tx_id,
                            *session_id,
                            &realm,
                            &area,
                            &resource,
                            &started_at,
                        )
                    },
                )
            })
            .collect();
        self.projection.mark_dirty();
        self.projection.refresh_if_dirty(|| transactions);
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_active_transactions(self.active_transaction_count());
            metrics.set_subscription_count(self.subscription_count());
        }
    }

    pub(super) fn subscription_count(&self) -> usize {
        self.families
            .lock()
            .values()
            .map(RoutedSubscriptionSet::subscription_count)
            .sum()
    }

    pub(super) fn active_transactions_for_resource(
        &self,
        resource_key: &KvResourceLockKey,
    ) -> usize {
        self.actors
            .lock()
            .values()
            .flat_map(crate::domains::kv::KvActor::active_transaction_scopes)
            .filter(|(_tx_id, family_id, realm, area, resource)| {
                *family_id == resource_key.family_id
                    && realm == &resource_key.realm
                    && area == &resource_key.area
                    && resource == &resource_key.resource
            })
            .count()
    }

    pub(super) fn conflicting_session_for_resource(
        &self,
        session_id: u64,
        resource_key: &KvResourceLockKey,
    ) -> Option<u64> {
        self.actors
            .lock()
            .iter()
            .find_map(|(active_session_id, actor)| {
                if *active_session_id == session_id {
                    return None;
                }

                actor.active_transaction_scopes().into_iter().find_map(
                    |(_tx_id, family_id, realm, area, resource)| {
                        (family_id == resource_key.family_id
                            && realm == resource_key.realm
                            && area == resource_key.area
                            && resource == resource_key.resource)
                            .then_some(*active_session_id)
                    },
                )
            })
    }

    pub(super) fn latency_snapshots(
        &self,
        resource_key: &KvResourceLockKey,
    ) -> (
        crate::control::admin::KvLatencySnapshot,
        crate::control::admin::KvLatencySnapshot,
    ) {
        self.projection.latency_snapshots(resource_key)
    }

    pub(super) fn record_read_latency(
        &self,
        resource_key: &KvResourceLockKey,
        started_at: std::time::Instant,
    ) {
        self.projection
            .record_read_latency(resource_key, started_at.elapsed().as_secs_f64() * 1000.0);
    }

    pub(super) fn record_write_latency(
        &self,
        resource_key: &KvResourceLockKey,
        started_at: std::time::Instant,
    ) {
        self.projection
            .record_write_latency(resource_key, started_at.elapsed().as_secs_f64() * 1000.0);
    }

    pub(super) fn resource_key_for_tx(
        &self,
        session_id: u64,
        tx_id: u64,
    ) -> Option<KvResourceLockKey> {
        self.actors
            .lock()
            .get(&session_id)
            .and_then(|actor| actor.resource_scope_for_tx(tx_id))
            .map(|(family_id, realm, area, resource)| {
                KvResourceLockKey::new(family_id, &realm, &area, &resource)
            })
    }

    #[cfg(test)]
    pub(super) fn session_inbox_address(
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
    ) -> crate::runtime::routing::RouteAddress {
        crate::runtime::routing::RouteAddress::new(
            family_id,
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        )
    }

    pub(super) fn kv_route_for_lock(
        resource_key: &KvResourceLockKey,
    ) -> crate::runtime::routing::Route {
        crate::runtime::routing::Route::new(format!(
            "kv://{}/{}/{}",
            resource_key.realm, resource_key.area, resource_key.resource
        ))
    }

    pub(super) fn route_kv_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &crate::runtime::routing::Route,
        mutation_count: u64,
    ) {
        #[cfg(test)]
        {
            let payload = crate::protocol::kv::encode_notify(
                subscription_id,
                route,
                crate::domains::kv::KvNotification { mutation_count },
            );
            let notify_ctx = FrameContext::new(
                session_id,
                crate::protocol::frame::ChannelId::Sub,
                crate::protocol::tlv::MessageType::new(crate::protocol::kv::msg_type::NOTIFY),
                bytes::Bytes::from(payload),
                *subscriber.family(),
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);
            if self.router.route(notify_envelope).is_err() {
                crate::observability::counter_inc("fitz_kv_notify_drops_total");
            }
        }

        #[cfg(not(test))]
        {
            let notification = crate::domains::kv::KvClientNotification::new(
                session_id,
                *subscriber.family(),
                subscription_id,
                route.clone(),
                crate::domains::kv::KvNotification { mutation_count },
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notification);
            if self.router.route(notify_envelope).is_err() {
                crate::observability::counter_inc("fitz_kv_notify_drops_total");
            }
        }
    }

    pub(super) fn route_kv_notification(
        &self,
        resource_key: &KvResourceLockKey,
        mutation_count: u64,
    ) {
        let family_id = crate::runtime::routing::RouteFamily::new(resource_key.family_id);
        let route = Self::kv_route_for_lock(resource_key);
        let families = self.families.lock();
        if let Some(state) = families.get(&resource_key.family_id) {
            state.for_each_matching_route(family_id, route.as_str(), |subscription| {
                self.route_kv_notify_to_subscription(
                    subscription.session_id,
                    subscription.subscription_id,
                    &subscription.subscriber,
                    &route,
                    mutation_count,
                );
            });
        }
    }

    pub(super) fn route_kv_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::kv::KvResponse,
        request_started: Option<std::time::Instant>,
    ) -> Result<(), DeliveryError> {
        #[cfg(test)]
        let response_ctx = {
            let response_bytes = crate::protocol::kv::encode_response(response);
            tracing::trace!(
                domain = "kv",
                session = meta.session_id,
                response_len = response_bytes.len(),
                "KV response encoded"
            );

            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx = crate::domains::kv::KvClientResponse::new(meta, response.clone());

        let Some(response_envelope) = envelope.try_reply_to(response_ctx) else {
            if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
                if matches!(response, crate::domains::kv::KvResponse::Error { .. }) {
                    metrics.record_failure(started_at);
                } else {
                    metrics.record_success(started_at);
                }
            }
            tracing::warn!(
                domain = "kv",
                session = meta.session_id,
                "Cannot route response: envelope has no source address"
            );
            return Ok(());
        };

        match self.router.route(response_envelope) {
            Ok(()) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    if matches!(response, crate::domains::kv::KvResponse::Error { .. }) {
                        metrics.record_failure(started_at);
                    } else {
                        metrics.record_success(started_at);
                    }
                }
                tracing::debug!(
                    domain = "kv",
                    session = meta.session_id,
                    "KV message handled and response routed"
                );
                Ok(())
            }
            Err(error) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(
                    domain = "kv",
                    session = meta.session_id,
                    error = ?error,
                    "Failed to route response"
                );
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    /// Remove all live KV transaction state owned by a disconnected session.
    ///
    /// This is the authoritative boundary for session-scoped cleanup: open
    /// transactions are dropped, resource locks are released, and the admin read
    /// model is refreshed so no durable recovery is implied.
    pub fn cleanup_session(&self, session_id: u64) {
        self.actors.lock().remove(&session_id);

        {
            let mut families = self.families.lock();
            for (family_id, state) in families.iter_mut() {
                state.remove_session(
                    crate::runtime::routing::RouteFamily::new(*family_id),
                    session_id,
                );
            }
            families.retain(|_, state| !state.is_empty());
        }

        tracing::debug!(
            domain = "kv",
            session = session_id,
            "All KV transactions and resource locks released for session (disconnect cleanup)"
        );
        self.sync_admin_snapshot();
    }

    pub fn active_transaction_count(&self) -> usize {
        self.actors
            .lock()
            .values()
            .map(crate::domains::kv::KvActor::transaction_count)
            .sum()
    }

    pub(super) fn apply_sync_write_options(
        &self,
        message: crate::domains::kv::KvMessage,
    ) -> crate::domains::kv::KvMessage {
        match message {
            crate::domains::kv::KvMessage::Begin {
                route_family,
                realm,
                area,
                resource,
                mode,
                write_options,
            } if write_options.is_sync() => crate::domains::kv::KvMessage::Begin {
                route_family,
                realm,
                area,
                resource,
                mode,
                write_options: self.sync_write_options,
            },
            message => message,
        }
    }
}

#[cfg(test)]
fn test_protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => crate::protocol::frame::ChannelId::Control,
        crate::runtime::ClientChannel::Pub => crate::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => crate::protocol::frame::ChannelId::Internal,
    }
}
