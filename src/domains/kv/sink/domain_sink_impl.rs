use super::model::{
    AdminKvPrefixScanResult, AdminKvRowsRequest, AdminKvRowsResult, Arc, AtomicBool, Bytes,
    HashMap, KvDomainActor, KvDomainCommand, KvDomainCore, KvDomainRuntime, KvDomainSink,
    KvDomainState, KvResourceLockKey, Mutex, Ordering, Router, Utc, ADMIN_INVENTORY_REFRESH_LIMIT,
};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

mod routing;
#[cfg(test)]
mod test_channels;

impl KvDomainState {
    fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            core: KvDomainCore {
                store,
                actors: Arc::new(Mutex::new(HashMap::new())),
                watch_actors: Mutex::new(HashMap::new()),
                router,
                projection: crate::domains::kv::projection::KvAdminProjection::new(
                    admin_read_model,
                ),
                metrics: None,
                sync_write_options: cntryl_midge::WriteOptions::sync(),
                buffered_write_options: cntryl_midge::WriteOptions::buffered(),
            },
            active: AtomicBool::new(true),
        }
    }

    pub(super) fn runtime(&self) -> KvDomainRuntime<'_> {
        KvDomainRuntime {
            core: &self.core,
            active: &self.active,
        }
    }
}

impl KvDomainActor {
    pub(super) fn new(state: Arc<KvDomainState>) -> Self {
        Self { state }
    }

    pub(super) fn route_address() -> RouteAddress {
        RouteAddress::new(RouteFamily::new(0), Route::new("internal://domain/kv"))
    }
}

impl KvDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        let state = Arc::new(KvDomainState::new(store, router, admin_read_model));
        let actor = Self::spawn_actor(state.clone());
        Self { state, actor }
    }

    fn spawn_actor(state: Arc<KvDomainState>) -> crate::runtime::ManagedActor<KvDomainCommand> {
        let router = state.core.router.clone();
        crate::runtime::ManagedActor::spawn_fail_closed(
            router,
            KvDomainActor::route_address(),
            move || KvDomainActor::new(state.clone()),
            crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
        )
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.actor = Self::spawn_actor(self.state.clone());
    }

    fn state_for_builder(&mut self) -> &mut KvDomainState {
        Arc::get_mut(&mut self.state).expect("KV sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_sync_write_options(self, write_options: cntryl_midge::WriteOptions) -> Self {
        let buffered_write_options =
            if write_options.is_cloud_async() || write_options.is_cloud_strict() {
                cntryl_midge::WriteOptions::cloud_async()
            } else {
                cntryl_midge::WriteOptions::buffered()
            };
        self.with_write_options(write_options, buffered_write_options)
    }

    #[must_use]
    pub fn with_write_options(
        mut self,
        sync_write_options: cntryl_midge::WriteOptions,
        buffered_write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        self.actor.stop();
        let core = &mut self.state_for_builder().core;
        core.sync_write_options = sync_write_options;
        core.buffered_write_options = buffered_write_options;
        self.rebuild_actor();
        self
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.actor.stop();
        let state = self.state_for_builder();
        state.core.metrics = Some(crate::domains::kv::KvMetrics::new(collector));
        state.runtime().refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.state.active.store(false, Ordering::Relaxed);
        self.actor.stop();
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.actor.health_snapshot()
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
    }

    #[cfg(test)]
    pub(crate) fn mark_actor_permanently_failed_for_tests(&self) {
        self.actor.mark_permanently_failed_for_tests();
    }

    #[cfg(test)]
    pub(crate) fn panic_actor_for_tests(&self) {
        let _ = self
            .actor
            .try_send_high_priority(KvDomainCommand::PanicForTests);
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
        self.state.runtime().admin_inventory(family)
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
        self.state
            .runtime()
            .admin_inventory_resource(route_family, realm, area, resource)
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
        self.state
            .runtime()
            .admin_get_committed_value(route_family, realm, area, resource, key)
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
        self.state.runtime().admin_scan_committed_prefix(
            route_family,
            realm,
            area,
            resource,
            key_prefix,
            limit,
        )
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
        self.state.runtime().admin_scan_committed_rows(request)
    }
}

impl KvDomainRuntime<'_> {
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
            self.core
                .store
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
            .core
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
            .core
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
            .core
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
        let iterator = tx.scan(&midge_query).map_err(|error| error.to_string())?;
        let mut rows = Vec::new();

        for entry in iterator {
            let (scoped_key, value) = entry.map_err(|error| error.to_string())?;
            let Some(user_key) =
                crate::domains::kv::KvActor::strip_scoped_prefix(&resource_prefix, &scoped_key)
            else {
                continue;
            };
            rows.push((user_key, value.to_vec()));
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
            .core
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
        let iterator = tx.scan(&midge_query).map_err(|error| error.to_string())?;
        let mut rows = Vec::new();

        for entry in iterator {
            let (scoped_key, value) = entry.map_err(|error| error.to_string())?;
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
            rows.push((user_key, value.to_vec()));
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
        let route_family = crate::runtime::routing::RouteFamily::try_from(family_id)
            .expect("KV family IDs originate from RouteFamily");
        let column_family = crate::domains::kv::KvActor::resolve_column_family(route_family, "")?;
        let tx = self
            .core
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let mut iterator = tx
            .scan(&cntryl_midge::Query::new())
            .map_err(|error| error.to_string())?;
        let mut discovered = Vec::new();

        for entry in iterator.by_ref() {
            let (key, value) = entry.map_err(|error| error.to_string())?;
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
        let route_family = crate::runtime::routing::RouteFamily::try_from(family_id)
            .expect("KV family IDs originate from RouteFamily");
        let column_family =
            crate::domains::kv::KvActor::resolve_column_family(route_family, resource)?;
        let read_tx = self
            .core
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

        for entry in iterator.by_ref() {
            let (scoped_key, value) = entry.map_err(|error| error.to_string())?;
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
                .core
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
                .commit(self.core.sync_write_options)
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
            .core
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
        self.core.projection.mark_dirty();
        self.core.projection.refresh_if_dirty(|| transactions);
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.core.metrics {
            metrics.set_active_transactions(self.active_transaction_count());
            metrics.set_subscription_count(self.subscription_count());
        }
    }

    pub(super) fn subscription_count(&self) -> usize {
        self.core
            .watch_actors
            .lock()
            .values()
            .map(crate::domains::kv::watch::KvWatchActor::subscription_count)
            .sum()
    }

    pub(super) fn active_transactions_for_resource(
        &self,
        resource_key: &KvResourceLockKey,
    ) -> usize {
        self.core
            .actors
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
        self.core
            .actors
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
        self.core.projection.latency_snapshots(resource_key)
    }

    pub(super) fn record_read_latency(
        &self,
        resource_key: &KvResourceLockKey,
        started_at: std::time::Instant,
    ) {
        self.core
            .projection
            .record_read_latency(resource_key, started_at.elapsed().as_secs_f64() * 1000.0);
    }

    pub(super) fn record_write_latency(
        &self,
        resource_key: &KvResourceLockKey,
        started_at: std::time::Instant,
    ) {
        self.core
            .projection
            .record_write_latency(resource_key, started_at.elapsed().as_secs_f64() * 1000.0);
    }

    pub(super) fn resource_key_for_tx(
        &self,
        session_id: u64,
        tx_id: u64,
    ) -> Option<KvResourceLockKey> {
        self.core
            .actors
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
}
