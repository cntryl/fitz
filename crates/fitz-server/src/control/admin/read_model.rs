use super::{
    KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueDeadLetter, QueueInflight,
    QueueInfo, RpcPendingRequest, RpcWorker, ScheduleInfo, SessionInfo, StreamAreaWatermarkDetail,
    StreamInfo, StreamRealmWatermarkDetail,
};
use crate::session::session::SessionInfo as RuntimeSessionInfo;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::sync::Arc;

mod bounded;
mod family_rows;
mod inventory;
mod resource_snapshot;
pub(crate) use bounded::AdminSnapshot;
use family_rows::SessionRows;
pub(crate) use inventory::{InventoryEntry, InventoryScope};

fn snapshot_session_info(session: &RuntimeSessionInfo, connected_at: String) -> SessionInfo {
    let claims = session.claims.as_ref();
    SessionInfo {
        session_id: session.session_id.to_string(),
        service_name: session.metadata.service_name(),
        route_family: session.route_family.as_u64(),
        subject: claims.map_or_default(|claims| claims.sub.clone()),
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
        remote_addr: session.peer_addr.map_or_default(|addr| addr.to_string()),
    }
}

#[derive(Default)]
pub struct AdminReadModel {
    pub(crate) kv: Arc<fitz_kv::control::admin::AdminReadModel>,
    pub(crate) stream: Arc<fitz_stream::control::admin::AdminReadModel>,
    pub(crate) notice: Arc<fitz_notice::control::admin::AdminReadModel>,
    pub(crate) queue: Arc<fitz_queue::control::admin::AdminReadModel>,
    pub(crate) rpc: Arc<fitz_rpc::control::admin::AdminReadModel>,
    pub(crate) lease: Arc<fitz_lease::control::admin::AdminReadModel>,
    pub(crate) schedule: Arc<fitz_schedule::control::admin::AdminReadModel>,
    sessions: RwLock<SessionRows>,
}

impl AdminReadModel {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_kv_transactions(&self, transactions: Vec<KvTransaction>) {
        self.kv.replace_kv_transactions(transactions);
    }

    #[cfg(test)]
    pub(crate) fn upsert_kv_transaction(&self, transaction: KvTransaction) {
        self.kv.upsert_kv_transaction(transaction);
    }

    #[cfg(test)]
    pub(crate) fn remove_kv_transaction(&self, session_id: u64, tx_id: u64) {
        self.kv.remove_kv_transaction(session_id, tx_id);
    }

    #[cfg(test)]
    pub(crate) fn remove_kv_transactions_for_session(&self, session_id: u64) {
        self.kv.remove_kv_transactions_for_session(session_id);
    }

    pub fn kv_transactions(&self, realm: Option<&str>) -> Vec<KvTransaction> {
        self.kv.kv_transactions(realm)
    }

    #[cfg(test)]
    pub(crate) fn kv_transaction_count(&self) -> usize {
        self.kv.kv_transaction_count()
    }

    #[cfg(test)]
    pub(crate) fn kv_transaction_count_for_resource(
        &self,
        route_family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> usize {
        self.kv
            .kv_transaction_count_for_resource(route_family, realm, area, resource)
    }

    pub fn replace_streams(&self, streams: Vec<StreamInfo>) {
        self.stream.replace_streams(streams);
    }

    pub fn streams(&self, realm: Option<&str>) -> Vec<StreamInfo> {
        self.stream.streams(realm)
    }

    pub fn replace_stream_realm_watermarks(&self, watermarks: Vec<StreamRealmWatermarkDetail>) {
        self.stream.replace_stream_realm_watermarks(watermarks);
    }

    pub fn stream_realm_watermark(&self, realm: &str) -> Option<StreamRealmWatermarkDetail> {
        self.stream.stream_realm_watermark(realm)
    }

    pub fn stream_realm_watermarks(&self) -> Vec<StreamRealmWatermarkDetail> {
        self.stream.stream_realm_watermarks()
    }

    pub fn replace_stream_area_watermarks(&self, watermarks: Vec<StreamAreaWatermarkDetail>) {
        self.stream.replace_stream_area_watermarks(watermarks);
    }

    pub fn stream_area_watermark(
        &self,
        realm: &str,
        area: &str,
    ) -> Option<StreamAreaWatermarkDetail> {
        self.stream.stream_area_watermark(realm, area)
    }

    pub fn stream_area_watermarks(&self) -> Vec<StreamAreaWatermarkDetail> {
        self.stream.stream_area_watermarks()
    }

    pub fn replace_stream_events_total(&self, total: usize) {
        self.stream.replace_stream_events_total(total);
    }

    pub fn stream_events_total(&self) -> usize {
        self.stream.stream_events_total()
    }

    pub fn replace_notice_subscriptions(&self, subscriptions: Vec<NoticeSubscription>) {
        self.notice.replace_notice_subscriptions(subscriptions);
    }

    pub fn notice_subscriptions(
        &self,
        realm: Option<&str>,
        route_pattern: Option<&str>,
    ) -> Vec<NoticeSubscription> {
        self.notice.notice_subscriptions(realm, route_pattern)
    }

    pub fn replace_notice_routes(&self, routes: Vec<NoticeRouteInfo>) {
        self.notice.replace_notice_routes(routes);
    }

    pub fn notice_routes(&self, realm: Option<&str>) -> Vec<NoticeRouteInfo> {
        self.notice.notice_routes(realm)
    }

    pub fn replace_queues(&self, queues: Vec<QueueInfo>) {
        self.queue.replace_queues(queues);
    }

    pub fn queues(&self, realm: Option<&str>) -> Vec<QueueInfo> {
        self.queue.queues(realm)
    }

    pub fn replace_queue_inflight(&self, inflight: Vec<QueueInflight>) {
        self.queue.replace_queue_inflight(inflight);
    }

    pub fn queue_inflight(&self, realm: Option<&str>) -> Vec<QueueInflight> {
        self.queue.queue_inflight(realm)
    }

    pub fn replace_queue_dead_letters(&self, messages: Vec<QueueDeadLetter>) {
        self.queue.replace_queue_dead_letters(messages);
    }

    pub fn queue_dead_letters(&self, realm: Option<&str>) -> Vec<QueueDeadLetter> {
        self.queue.queue_dead_letters(realm)
    }

    pub(crate) fn queue_dead_letter(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: u64,
    ) -> Option<QueueDeadLetter> {
        self.queue
            .queue_dead_letter(family, realm, area, resource, message_id)
    }

    pub fn replace_rpc_workers(&self, workers: Vec<RpcWorker>) {
        self.rpc.replace_rpc_workers(workers);
    }

    pub fn rpc_workers(&self, realm: Option<&str>) -> Vec<RpcWorker> {
        self.rpc.rpc_workers(realm)
    }

    pub fn replace_rpc_pending(&self, requests: Vec<RpcPendingRequest>) {
        self.rpc.replace_rpc_pending(requests);
    }

    pub fn rpc_pending(&self, realm: Option<&str>) -> Vec<RpcPendingRequest> {
        self.rpc.rpc_pending(realm)
    }

    pub fn replace_leases(&self, leases: Vec<LeaseInfo>) {
        self.lease.replace_leases(leases);
    }

    pub fn upsert_lease(&self, lease: LeaseInfo) {
        self.lease.upsert_lease(lease);
    }

    pub fn remove_lease(&self, route_family: u64, realm: &str, area: &str, resource: &str) {
        self.lease.remove_lease(route_family, realm, area, resource);
    }

    pub fn leases(&self, realm: Option<&str>) -> Vec<LeaseInfo> {
        self.lease.leases(realm)
    }

    pub fn leases_for_route_family(&self, route_family: u64) -> Vec<LeaseInfo> {
        self.lease.leases_for_route_family(route_family)
    }

    pub fn replace_schedules(&self, schedules: Vec<ScheduleInfo>) {
        self.schedule.replace_schedules(schedules);
    }

    #[cfg(test)]
    pub(crate) fn set_schedule_family_pending_fire_count(
        &self,
        route_family: u64,
        count: usize,
    ) -> usize {
        self.schedule
            .set_schedule_family_pending_fire_count(route_family, count)
    }

    pub fn upsert_schedule(&self, schedule: ScheduleInfo) {
        self.schedule.upsert_schedule(schedule);
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
        self.schedule
            .upsert_schedule_fields(route_family, realm, area, resource, operation, cron);
    }

    pub fn remove_schedule(
        &self,
        route_family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        operation: &str,
    ) {
        self.schedule
            .remove_schedule(route_family, realm, area, resource, operation);
    }

    pub fn schedules(&self, realm: Option<&str>) -> Vec<ScheduleInfo> {
        self.schedule.schedules(realm)
    }

    pub fn schedules_for_route_family(&self, route_family: u64) -> Vec<ScheduleInfo> {
        self.schedule.schedules_for_route_family(route_family)
    }

    pub fn record_session_open(&self, session: &RuntimeSessionInfo) {
        let connected_at = DateTime::<Utc>::from(session.connected_at()).to_rfc3339();
        self.sessions.write().insert(
            session.session_id,
            snapshot_session_info(session, connected_at),
        );
    }

    pub fn record_session_update(&self, session: &RuntimeSessionInfo) {
        let connected_at = self.sessions.read().get(session.session_id).map_or_else(
            || DateTime::<Utc>::from(session.connected_at()).to_rfc3339(),
            |info| info.connected_at.clone(),
        );

        self.sessions.write().insert(
            session.session_id,
            snapshot_session_info(session, connected_at),
        );
    }

    pub fn record_session_close(&self, session_id: u64) {
        self.sessions.write().remove(session_id);
    }

    pub fn sessions(&self) -> Vec<SessionInfo> {
        let sessions = self.sessions.read();
        sessions.values().cloned().collect()
    }
}

#[cfg(test)]
#[path = "read_model/tests.rs"]
mod tests;
