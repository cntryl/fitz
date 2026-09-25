use super::{
    KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueDeadLetter, QueueInflight,
    QueueInfo, RpcPendingRequest, RpcWorker, ScheduleInfo, SessionInfo, StreamAreaWatermarkDetail,
    StreamInfo, StreamRealmWatermarkDetail,
};
use crate::runtime::matcher::{parse_pattern_segments, PatternSegment};
use crate::runtime::routing::route_triplet;
use crate::session::session::SessionInfo as RuntimeSessionInfo;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

type ScheduleIdentity = (u64, String, String, String, String);
type LeaseIdentity = (u64, String, String, String);
type StreamRealmIdentity = String;
type StreamAreaIdentity = (String, String);
fn schedule_identity_key(
    route_family: u64,
    realm: &str,
    area: &str,
    resource: &str,
    operation: &str,
) -> ScheduleIdentity {
    (
        route_family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        operation.to_string(),
    )
}

fn lease_identity_key(route_family: u64, realm: &str, area: &str, resource: &str) -> LeaseIdentity {
    (
        route_family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
    )
}

fn schedule_identity_for(info: &ScheduleInfo) -> ScheduleIdentity {
    schedule_identity_key(
        info.route_family,
        &info.realm,
        &info.area,
        &info.resource,
        &info.operation,
    )
}

fn lease_identity_for(info: &LeaseInfo) -> LeaseIdentity {
    lease_identity_key(info.route_family, &info.realm, &info.area, &info.resource)
}

fn kv_transaction_session_mode(session_id: u64) -> String {
    format!("session:{session_id}:readwrite")
}

fn snapshot_session_info(session: &RuntimeSessionInfo, connected_at: String) -> SessionInfo {
    let claims = session.claims.as_ref();
    SessionInfo {
        session_id: session.session_id.to_string(),
        route_family: session.route_family.as_u64(),
        subject: claims.map(|claims| claims.sub.clone()).unwrap_or_default(),
        identity_claim: claims
            .and_then(|claims| claims.identity_claim.clone())
            .unwrap_or_default(),
        identity_value: claims
            .and_then(|claims| claims.identity_value.clone())
            .unwrap_or_default(),
        connected_at,
        idle_seconds: session.idle_seconds(),
        messages_received: session.messages_received(),
        messages_sent: session.messages_sent(),
        transport: session.transport_kind.to_string(),
        remote_addr: session
            .peer_addr
            .map(|addr| addr.to_string())
            .unwrap_or_default(),
    }
}

fn matches_realm(realm: Option<&str>, value: &str) -> bool {
    realm.is_none_or(|needle| value == needle)
}

fn matches_substring(filter: Option<&str>, value: &str) -> bool {
    filter.is_none_or(|needle| value.contains(needle))
}

fn matches_notice_route_realm(realm: Option<&str>, route: &str) -> bool {
    realm.is_none_or(|needle| route_triplet(route).is_some_and(|parts| parts.realm == needle))
}

fn matches_rpc_route_realm(realm: Option<&str>, route: &str) -> bool {
    realm.is_none_or(|needle| {
        parse_pattern_segments(route)
            .first()
            .is_some_and(|segment| match segment {
                PatternSegment::Literal(value) => value == needle,
                PatternSegment::Star | PatternSegment::DoubleStar => true,
            })
    })
}

fn collect_slice_matches<T: Clone>(items: &[T], include: impl Fn(&T) -> bool) -> Vec<T> {
    items.iter().filter(|item| include(item)).cloned().collect()
}

fn collect_map_value_matches<K, T: Clone>(
    items: &HashMap<K, T>,
    include: impl Fn(&T) -> bool,
) -> Vec<T> {
    items
        .values()
        .filter(|item| include(item))
        .cloned()
        .collect()
}

#[derive(Default)]
pub struct AdminReadModel {
    kv_transactions: RwLock<Vec<KvTransaction>>,
    streams: RwLock<Vec<StreamInfo>>,
    stream_realm_watermarks: RwLock<BTreeMap<StreamRealmIdentity, StreamRealmWatermarkDetail>>,
    stream_area_watermarks: RwLock<BTreeMap<StreamAreaIdentity, StreamAreaWatermarkDetail>>,
    stream_events_total: RwLock<usize>,
    notice_subscriptions: RwLock<Vec<NoticeSubscription>>,
    notice_routes: RwLock<Vec<NoticeRouteInfo>>,
    queues: RwLock<Vec<QueueInfo>>,
    queue_inflight: RwLock<Vec<QueueInflight>>,
    queue_dead_letters: RwLock<Vec<QueueDeadLetter>>,
    rpc_workers: RwLock<Vec<RpcWorker>>,
    rpc_pending: RwLock<Vec<RpcPendingRequest>>,
    leases: RwLock<BTreeMap<LeaseIdentity, LeaseInfo>>,
    lease_waiter_counts: RwLock<BTreeMap<u64, usize>>,
    schedules: RwLock<BTreeMap<ScheduleIdentity, ScheduleInfo>>,
    schedule_pending_fire_counts: RwLock<BTreeMap<u64, usize>>,
    sessions: RwLock<HashMap<u64, SessionInfo>>,
}

impl AdminReadModel {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_kv_transactions(&self, transactions: Vec<KvTransaction>) {
        *self.kv_transactions.write() = transactions;
    }

    pub(crate) fn upsert_kv_transaction(&self, transaction: KvTransaction) {
        let mut transactions = self.kv_transactions.write();
        if let Some(existing) = transactions.iter_mut().find(|existing| {
            existing.tx_id == transaction.tx_id && existing.mode == transaction.mode
        }) {
            *existing = transaction;
        } else {
            transactions.push(transaction);
        }
    }

    pub(crate) fn remove_kv_transaction(&self, session_id: u64, tx_id: u64) {
        let session_mode = kv_transaction_session_mode(session_id);
        self.kv_transactions
            .write()
            .retain(|transaction| transaction.tx_id != tx_id || transaction.mode != session_mode);
    }

    pub(crate) fn remove_kv_transactions_for_session(&self, session_id: u64) {
        let session_mode = kv_transaction_session_mode(session_id);
        self.kv_transactions
            .write()
            .retain(|transaction| transaction.mode != session_mode);
    }

    pub fn kv_transactions(&self, realm: Option<&str>) -> Vec<KvTransaction> {
        let transactions = self.kv_transactions.read();
        collect_slice_matches(&transactions, |item| matches_realm(realm, &item.realm))
    }

    #[cfg(test)]
    pub(crate) fn kv_transaction_count(&self) -> usize {
        self.kv_transactions.read().len()
    }

    #[cfg(test)]
    pub(crate) fn kv_transaction_count_for_resource(
        &self,
        route_family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> usize {
        self.kv_transactions
            .read()
            .iter()
            .filter(|transaction| {
                transaction.route_family == route_family
                    && transaction.realm == realm
                    && transaction.area == area
                    && transaction.resource == resource
            })
            .count()
    }

    pub fn replace_streams(&self, streams: Vec<StreamInfo>) {
        *self.streams.write() = streams;
    }

    pub fn streams(&self, realm: Option<&str>) -> Vec<StreamInfo> {
        let streams = self.streams.read();
        collect_slice_matches(&streams, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_stream_realm_watermarks(&self, watermarks: Vec<StreamRealmWatermarkDetail>) {
        *self.stream_realm_watermarks.write() = watermarks
            .into_iter()
            .map(|detail| (detail.realm.clone(), detail))
            .collect();
    }

    pub fn stream_realm_watermark(&self, realm: &str) -> Option<StreamRealmWatermarkDetail> {
        self.stream_realm_watermarks.read().get(realm).cloned()
    }

    pub fn stream_realm_watermarks(&self) -> Vec<StreamRealmWatermarkDetail> {
        self.stream_realm_watermarks
            .read()
            .values()
            .cloned()
            .collect()
    }

    pub fn replace_stream_area_watermarks(&self, watermarks: Vec<StreamAreaWatermarkDetail>) {
        *self.stream_area_watermarks.write() = watermarks
            .into_iter()
            .map(|detail| ((detail.realm.clone(), detail.area.clone()), detail))
            .collect();
    }

    pub fn stream_area_watermark(
        &self,
        realm: &str,
        area: &str,
    ) -> Option<StreamAreaWatermarkDetail> {
        self.stream_area_watermarks
            .read()
            .get(&(realm.to_string(), area.to_string()))
            .cloned()
    }

    pub fn stream_area_watermarks(&self) -> Vec<StreamAreaWatermarkDetail> {
        self.stream_area_watermarks
            .read()
            .values()
            .cloned()
            .collect()
    }

    pub fn replace_stream_events_total(&self, total: usize) {
        *self.stream_events_total.write() = total;
    }

    pub fn stream_events_total(&self) -> usize {
        *self.stream_events_total.read()
    }

    pub fn replace_notice_subscriptions(&self, subscriptions: Vec<NoticeSubscription>) {
        *self.notice_subscriptions.write() = subscriptions;
    }

    pub(crate) fn replace_notice_family_subscriptions(
        &self,
        route_family: u64,
        subscriptions: Vec<NoticeSubscription>,
    ) {
        let mut current = self.notice_subscriptions.write();
        current.retain(|subscription| subscription.route_family != route_family);
        current.extend(subscriptions);
    }

    pub fn notice_subscriptions(
        &self,
        realm: Option<&str>,
        route_pattern: Option<&str>,
    ) -> Vec<NoticeSubscription> {
        let subscriptions = self.notice_subscriptions.read();
        collect_slice_matches(&subscriptions, |item| {
            matches_realm(realm, &item.realm) && matches_substring(route_pattern, &item.pattern)
        })
    }

    pub fn replace_notice_routes(&self, routes: Vec<NoticeRouteInfo>) {
        *self.notice_routes.write() = routes;
    }

    pub(crate) fn replace_notice_family_routes(
        &self,
        route_family: u64,
        routes: Vec<NoticeRouteInfo>,
    ) {
        let mut current = self.notice_routes.write();
        current.retain(|route| route.route_family != route_family);
        current.extend(routes);
    }

    pub fn notice_routes(&self, realm: Option<&str>) -> Vec<NoticeRouteInfo> {
        let routes = self.notice_routes.read();
        collect_slice_matches(&routes, |item| {
            matches_notice_route_realm(realm, &item.route)
        })
    }

    pub fn replace_queues(&self, queues: Vec<QueueInfo>) {
        *self.queues.write() = queues;
    }

    pub fn queues(&self, realm: Option<&str>) -> Vec<QueueInfo> {
        let queues = self.queues.read();
        collect_slice_matches(&queues, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_queue_inflight(&self, inflight: Vec<QueueInflight>) {
        *self.queue_inflight.write() = inflight;
    }

    pub fn queue_inflight(&self, realm: Option<&str>) -> Vec<QueueInflight> {
        let inflight = self.queue_inflight.read();
        collect_slice_matches(&inflight, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_queue_dead_letters(&self, messages: Vec<QueueDeadLetter>) {
        *self.queue_dead_letters.write() = messages;
    }

    pub fn queue_dead_letters(&self, realm: Option<&str>) -> Vec<QueueDeadLetter> {
        let messages = self.queue_dead_letters.read();
        collect_slice_matches(&messages, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_rpc_workers(&self, workers: Vec<RpcWorker>) {
        *self.rpc_workers.write() = workers;
    }

    pub(crate) fn replace_rpc_family_workers(&self, route_family: u64, workers: Vec<RpcWorker>) {
        let mut current = self.rpc_workers.write();
        current.retain(|worker| worker.route_family != route_family);
        current.extend(workers);
    }

    pub fn rpc_workers(&self, realm: Option<&str>) -> Vec<RpcWorker> {
        let workers = self.rpc_workers.read();
        collect_slice_matches(&workers, |item| matches_rpc_route_realm(realm, &item.route))
    }

    pub fn replace_rpc_pending(&self, requests: Vec<RpcPendingRequest>) {
        *self.rpc_pending.write() = requests;
    }

    pub(crate) fn replace_rpc_family_pending(
        &self,
        route_family: u64,
        requests: Vec<RpcPendingRequest>,
    ) {
        let mut current = self.rpc_pending.write();
        current.retain(|request| request.route_family != route_family);
        current.extend(requests);
    }

    pub fn rpc_pending(&self, realm: Option<&str>) -> Vec<RpcPendingRequest> {
        let pending = self.rpc_pending.read();
        collect_slice_matches(&pending, |item| matches_rpc_route_realm(realm, &item.route))
    }

    pub fn replace_leases(&self, leases: Vec<LeaseInfo>) {
        *self.leases.write() = leases
            .into_iter()
            .map(|lease| (lease_identity_for(&lease), lease))
            .collect();
    }

    pub(crate) fn set_lease_family_waiter_count(&self, route_family: u64, count: usize) -> usize {
        let mut counts = self.lease_waiter_counts.write();
        counts.insert(route_family, count);
        counts.values().sum()
    }

    pub(crate) fn lease_count(&self) -> usize {
        self.leases.read().len()
    }

    pub fn upsert_lease(&self, lease: LeaseInfo) {
        self.leases
            .write()
            .insert(lease_identity_for(&lease), lease);
    }

    pub fn remove_lease(&self, route_family: u64, realm: &str, area: &str, resource: &str) {
        self.leases
            .write()
            .remove(&lease_identity_key(route_family, realm, area, resource));
    }

    pub fn leases(&self, realm: Option<&str>) -> Vec<LeaseInfo> {
        let leases = self.leases.read();
        leases
            .values()
            .filter(|item| matches_realm(realm, &item.realm))
            .cloned()
            .collect()
    }

    pub fn leases_for_route_family(&self, route_family: u64) -> Vec<LeaseInfo> {
        let leases = self.leases.read();
        leases
            .values()
            .filter(|item| item.route_family == route_family)
            .cloned()
            .collect()
    }

    pub fn replace_schedules(&self, schedules: Vec<ScheduleInfo>) {
        *self.schedules.write() = schedules
            .into_iter()
            .map(|schedule| (schedule_identity_for(&schedule), schedule))
            .collect();
    }

    pub(crate) fn replace_schedule_family(&self, route_family: u64, schedules: Vec<ScheduleInfo>) {
        let mut current = self.schedules.write();
        current.retain(|identity, _| identity.0 != route_family);
        current.extend(
            schedules
                .into_iter()
                .map(|schedule| (schedule_identity_for(&schedule), schedule)),
        );
    }

    pub(crate) fn schedule_count(&self) -> usize {
        self.schedules.read().len()
    }

    pub(crate) fn set_schedule_family_pending_fire_count(
        &self,
        route_family: u64,
        count: usize,
    ) -> usize {
        let mut counts = self.schedule_pending_fire_counts.write();
        counts.insert(route_family, count);
        counts.values().sum()
    }

    pub fn upsert_schedule(&self, schedule: ScheduleInfo) {
        let mut schedules = self.schedules.write();
        let identity = schedule_identity_for(&schedule);
        if let Some(existing) = schedules.get_mut(&identity) {
            // Fast path for idempotent create/upsert calls: avoid rewriting
            // the admin model when durable schedule identity is unchanged.
            if existing.cron == schedule.cron
                && existing.delivery_mode == schedule.delivery_mode
                && existing.next_run == schedule.next_run
                && existing.last_run == schedule.last_run
                && existing.executions_total == schedule.executions_total
                && existing.enabled == schedule.enabled
            {
                return;
            }
            *existing = schedule;
        } else {
            schedules.insert(identity, schedule);
        }
    }

    pub fn upsert_schedule_fields(
        &self,
        route_family: u64,
        realm: String,
        area: String,
        resource: String,
        operation: String,
        cron: String,
    ) {
        let next_run = Utc::now().to_rfc3339();
        self.upsert_schedule(ScheduleInfo::enabled_snapshot(
            route_family,
            realm,
            area,
            resource,
            operation,
            cron,
            &next_run,
        ));
    }

    pub fn remove_schedule(
        &self,
        route_family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        operation: &str,
    ) {
        self.schedules.write().remove(&schedule_identity_key(
            route_family,
            realm,
            area,
            resource,
            operation,
        ));
    }

    pub fn schedules(&self, realm: Option<&str>) -> Vec<ScheduleInfo> {
        let schedules = self.schedules.read();
        schedules
            .values()
            .filter(|item| matches_realm(realm, &item.realm))
            .cloned()
            .collect()
    }

    pub fn schedules_for_route_family(&self, route_family: u64) -> Vec<ScheduleInfo> {
        let schedules = self.schedules.read();
        schedules
            .values()
            .filter(|item| item.route_family == route_family)
            .cloned()
            .collect()
    }

    pub fn record_session_open(&self, session: &RuntimeSessionInfo) {
        let connected_at = DateTime::<Utc>::from(session.connected_at()).to_rfc3339();
        self.sessions.write().insert(
            session.session_id,
            snapshot_session_info(session, connected_at),
        );
    }

    pub fn record_session_update(&self, session: &RuntimeSessionInfo) {
        let connected_at = self.sessions.read().get(&session.session_id).map_or_else(
            || DateTime::<Utc>::from(session.connected_at()).to_rfc3339(),
            |info| info.connected_at.clone(),
        );

        self.sessions.write().insert(
            session.session_id,
            snapshot_session_info(session, connected_at),
        );
    }

    pub fn record_session_close(&self, session_id: u64) {
        self.sessions.write().remove(&session_id);
    }

    pub fn sessions(&self) -> Vec<SessionInfo> {
        let sessions = self.sessions.read();
        collect_map_value_matches(&sessions, |_| true)
    }
}

#[cfg(test)]
#[path = "read_model/tests.rs"]
mod tests;
