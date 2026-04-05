use super::Runtime;
use crate::domains::queue::{MessageId, QueueKey};
use crate::runtime::routing::RouteFamily;

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

    pub fn stream_list_streams(&self, realm: Option<&str>) -> Vec<crate::api::admin::StreamInfo> {
        self.refresh_stream_admin_snapshot();
        self.admin_read_model.streams(realm)
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

    pub fn lease_list_leases(&self, realm: Option<&str>) -> Vec<crate::api::admin::LeaseInfo> {
        self.admin_read_model.leases(realm)
    }

    pub fn schedule_list_schedules(
        &self,
        realm: Option<&str>,
    ) -> Vec<crate::api::admin::ScheduleInfo> {
        self.refresh_schedule_admin_snapshot();
        self.admin_read_model.schedules(realm)
    }

    pub fn list_sessions(&self, realm: Option<&str>) -> Vec<crate::api::admin::SessionInfo> {
        let Some(ingress) = self.ingress.read().clone() else {
            return self.admin_read_model.sessions(realm);
        };

        ingress
            .active_sessions()
            .into_iter()
            .filter(|session| match realm {
                Some(realm) => session
                    .claims
                    .as_ref()
                    .map(|claims| claims.tenant == realm)
                    .unwrap_or(false),
                None => true,
            })
            .map(|session| crate::api::admin::SessionInfo {
                session_id: session.session_id.to_string(),
                realm: session
                    .claims
                    .as_ref()
                    .map(|claims| claims.tenant.clone())
                    .unwrap_or_default(),
                connected_at: String::new(),
                idle_seconds: 0,
                messages_received: 0,
                messages_sent: 0,
                transport: session.transport_kind.to_string(),
                remote_addr: session
                    .peer_addr
                    .map(|addr| addr.to_string())
                    .unwrap_or_default(),
            })
            .collect()
    }
}
