use super::Runtime;
use crate::domains::queue::{MessageId, QueueKey};
use crate::domains::stream::sink::AdminStreamReadRequest;
use crate::runtime::routing::RouteFamily;
use chrono::{DateTime, Utc};

impl Runtime {
    fn refresh_queue_admin_snapshot(&self) {
        let domains = self.domains.read().clone();
        if let Some(domains) = domains {
            domains.queue.refresh_admin_snapshot_if_dirty();
        }
    }

    fn refresh_rpc_admin_snapshot(&self) {
        let domains = self.domains.read().clone();
        if let Some(domains) = domains {
            domains.rpc.refresh_admin_snapshot_if_dirty();
        }
    }

    fn refresh_notice_admin_snapshot(&self) {
        let domains = self.domains.read().clone();
        if let Some(domains) = domains {
            domains.notice.refresh_admin_snapshot_if_dirty();
        }
    }

    fn refresh_schedule_admin_snapshot(&self) {
        let domains = self.domains.read().clone();
        if let Some(domains) = domains {
            domains.schedule.refresh_admin_snapshot_if_dirty();
        }
    }

    pub(crate) fn refresh_stream_admin_snapshot(&self) {
        let domains = self.domains.read().clone();
        if let Some(domains) = domains {
            domains.stream.refresh_admin_snapshot_if_dirty();
        }
    }

    pub fn kv_list_transactions(
        &self,
        realm: Option<&str>,
    ) -> Vec<crate::api::admin::KvTransaction> {
        self.admin_read_model.kv_transactions(realm)
    }

    pub fn kv_get_committed_value(
        &self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        let domains = self
            .domains
            .read()
            .clone()
            .ok_or_else(|| "KV domain is not initialized".to_string())?;
        domains
            .kv
            .admin_get_committed_value(family, realm, area, resource, key)
    }

    pub fn kv_scan_committed_prefix(
        &self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<crate::domains::kv::sink::AdminKvPrefixScanResult, String> {
        let domains = self
            .domains
            .read()
            .clone()
            .ok_or_else(|| "KV domain is not initialized".to_string())?;
        domains
            .kv
            .admin_scan_committed_prefix(family, realm, area, resource, key_prefix, limit)
    }

    pub fn stream_list_streams(&self, realm: Option<&str>) -> Vec<crate::api::admin::StreamInfo> {
        self.refresh_stream_admin_snapshot();
        self.admin_read_model.streams(realm)
    }

    pub fn stream_read_resource_records(
        &self,
        request: AdminStreamReadRequest<'_>,
    ) -> Result<
        (
            Vec<crate::domains::stream::protocol::StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        let domains = self
            .domains
            .read()
            .clone()
            .ok_or_else(|| "Stream domain is not initialized".to_string())?;
        domains.stream.admin_read_resource_records(request)
    }

    pub(crate) fn stream_list_realm_watermark_details(
        &self,
    ) -> Vec<crate::api::admin::StreamRealmWatermarkDetail> {
        self.refresh_stream_admin_snapshot();
        self.admin_read_model.stream_realm_watermarks()
    }

    pub(crate) fn stream_realm_watermark_detail(
        &self,
        realm: &str,
    ) -> Option<crate::api::admin::StreamRealmWatermarkDetail> {
        self.refresh_stream_admin_snapshot();
        self.admin_read_model.stream_realm_watermark(realm)
    }

    pub fn stream_list_realm_watermarks(
        &self,
        realm: &str,
    ) -> Vec<crate::api::admin::StreamRealmWatermark> {
        self.stream_realm_watermark_detail(realm)
            .map(|detail| detail.family_watermarks)
            .unwrap_or_default()
    }

    pub(crate) fn stream_list_area_watermark_details(
        &self,
    ) -> Vec<crate::api::admin::StreamAreaWatermarkDetail> {
        self.refresh_stream_admin_snapshot();
        self.admin_read_model.stream_area_watermarks()
    }

    pub(crate) fn stream_area_watermark_detail(
        &self,
        realm: &str,
        area: &str,
    ) -> Option<crate::api::admin::StreamAreaWatermarkDetail> {
        self.refresh_stream_admin_snapshot();
        self.admin_read_model.stream_area_watermark(realm, area)
    }

    pub fn stream_list_area_watermarks(
        &self,
        realm: &str,
        area: &str,
    ) -> Vec<crate::api::admin::StreamAreaWatermark> {
        self.stream_area_watermark_detail(realm, area)
            .map(|detail| detail.family_watermarks)
            .unwrap_or_default()
    }

    pub fn notice_list_subscriptions(
        &self,
        realm: Option<&str>,
        route_pattern: Option<&str>,
    ) -> Vec<crate::api::admin::NoticeSubscription> {
        self.refresh_notice_admin_snapshot();
        self.admin_read_model
            .notice_subscriptions(realm, route_pattern)
    }

    pub fn notice_list_routes(
        &self,
        realm: Option<&str>,
    ) -> Vec<crate::api::admin::NoticeRouteInfo> {
        self.refresh_notice_admin_snapshot();
        self.admin_read_model.notice_routes(realm)
    }

    pub fn queue_list_queues(&self, realm: Option<&str>) -> Vec<crate::api::admin::QueueInfo> {
        self.refresh_queue_admin_snapshot();
        self.admin_read_model.queues(realm)
    }

    pub fn queue_list_inflight(
        &self,
        realm: Option<&str>,
    ) -> Vec<crate::api::admin::QueueInflight> {
        self.refresh_queue_admin_snapshot();
        self.admin_read_model.queue_inflight(realm)
    }

    pub fn queue_list_dead_letters(
        &self,
        realm: Option<&str>,
    ) -> Vec<crate::api::admin::QueueDeadLetter> {
        self.refresh_queue_admin_snapshot();
        self.admin_read_model.queue_dead_letters(realm)
    }

    pub fn queue_replay_dead_letter(
        &self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: u64,
    ) -> Result<bool, String> {
        let domains = self
            .domains
            .read()
            .clone()
            .ok_or_else(|| "Queue domain is not initialized".to_string())?;
        domains.queue.replay_dead_letter(
            QueueKey {
                family,
                realm: realm.to_string(),
                area: area.to_string(),
                resource: resource.to_string(),
            },
            MessageId::new(message_id),
        )
    }

    pub fn queue_purge_dead_letter(
        &self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: u64,
    ) -> Result<bool, String> {
        let domains = self
            .domains
            .read()
            .clone()
            .ok_or_else(|| "Queue domain is not initialized".to_string())?;
        domains.queue.purge_dead_letter(
            QueueKey {
                family,
                realm: realm.to_string(),
                area: area.to_string(),
                resource: resource.to_string(),
            },
            MessageId::new(message_id),
        )
    }

    pub fn rpc_list_workers(&self, realm: Option<&str>) -> Vec<crate::api::admin::RpcWorker> {
        self.refresh_rpc_admin_snapshot();
        self.admin_read_model.rpc_workers(realm)
    }

    pub fn rpc_list_pending(
        &self,
        realm: Option<&str>,
    ) -> Vec<crate::api::admin::RpcPendingRequest> {
        self.refresh_rpc_admin_snapshot();
        self.admin_read_model.rpc_pending(realm)
    }

    pub(crate) fn rpc_pending_snapshot(&self) -> Vec<crate::api::admin::RpcPendingRequest> {
        self.rpc_list_pending(None)
    }

    pub fn lease_list_leases(&self, realm: Option<&str>) -> Vec<crate::api::admin::LeaseInfo> {
        self.admin_read_model.leases(realm)
    }

    pub fn lease_list_waiters(&self) -> Vec<crate::api::admin::LeaseWaiterInfo> {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.lease.admin_waiters())
            .unwrap_or_default()
    }

    pub fn schedule_list_schedules(
        &self,
        realm: Option<&str>,
    ) -> Vec<crate::api::admin::ScheduleInfo> {
        self.refresh_schedule_admin_snapshot();
        self.admin_read_model.schedules(realm)
    }

    pub fn schedule_list_pending_claims(
        &self,
        family: RouteFamily,
    ) -> Vec<crate::api::admin::SchedulePendingClaimInfo> {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.admin_pending_claims(family))
            .unwrap_or_default()
    }

    pub fn list_sessions(&self) -> Vec<crate::api::admin::SessionInfo> {
        let Some(ingress) = self.ingress.read().clone() else {
            return self.admin_read_model.sessions();
        };

        ingress
            .active_sessions()
            .into_iter()
            .map(|session| {
                let claims = session.claims.as_ref();
                crate::api::admin::SessionInfo {
                    session_id: session.session_id.to_string(),
                    route_family: session.route_family.as_u64(),
                    subject: claims.map(|claims| claims.sub.clone()).unwrap_or_default(),
                    identity_claim: claims
                        .and_then(|claims| claims.identity_claim.clone())
                        .unwrap_or_default(),
                    identity_value: claims
                        .and_then(|claims| claims.identity_value.clone())
                        .unwrap_or_default(),
                    connected_at: DateTime::<Utc>::from(session.connected_at()).to_rfc3339(),
                    idle_seconds: session.idle_seconds(),
                    messages_received: session.messages_received(),
                    messages_sent: session.messages_sent(),
                    transport: session.transport_kind.to_string(),
                    remote_addr: session
                        .peer_addr
                        .map(|addr| addr.to_string())
                        .unwrap_or_default(),
                }
            })
            .collect()
    }
}
