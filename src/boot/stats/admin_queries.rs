use super::Runtime;

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

    fn refresh_stream_admin_snapshot(&self) {
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

    pub fn queue_list_leases(&self, realm: Option<&str>) -> Vec<crate::api::admin::QueueLease> {
        self.refresh_queue_admin_snapshot();
        self.admin_read_model.queue_leases(realm)
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
