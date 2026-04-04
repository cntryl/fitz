use super::Runtime;

impl Runtime {
    pub fn queue_messages_ready(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.ready_message_count())
            .unwrap_or_else(|| {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_ready)
                    .sum()
            })
    }

    pub fn queue_messages_delayed(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.delayed_message_count())
            .unwrap_or_else(|| {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_delayed)
                    .sum()
            })
    }

    pub fn kv_transactions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.kv.active_transaction_count())
            .unwrap_or_else(|| self.admin_read_model.kv_transactions(None).len())
    }

    pub fn kv_keys_total(&self) -> usize {
        0
    }

    pub fn notice_subscriptions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.notice.subscription_count())
            .unwrap_or_else(|| self.admin_read_model.notice_subscriptions(None, None).len())
    }

    pub fn queue_messages_pending(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.pending_message_count())
            .unwrap_or_else(|| {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_ready + queue.messages_delayed)
                    .sum()
            })
    }

    pub fn queue_messages_dead_lettered(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.dead_letter_count())
            .unwrap_or_else(|| {
                self.admin_read_model
                    .queues(None)
                    .into_iter()
                    .map(|queue| queue.messages_dead_lettered)
                    .sum()
            })
    }

    pub fn queue_leases_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.queue.active_lease_count())
            .unwrap_or_else(|| self.admin_read_model.queue_leases(None).len())
    }

    pub fn rpc_workers_registered(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.rpc.worker_count())
            .unwrap_or_else(|| self.admin_read_model.rpc_workers(None).len())
    }

    pub fn rpc_requests_pending(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.rpc.pending_request_count())
            .unwrap_or_else(|| self.admin_read_model.rpc_pending(None).len())
    }

    pub fn lease_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.lease.lease_count())
            .unwrap_or_else(|| self.admin_read_model.leases(None).len())
    }

    pub fn stream_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.stream.stream_count())
            .unwrap_or_else(|| self.admin_read_model.streams(None).len())
    }

    pub fn kv_operations_per_second(&self) -> f64 {
        0.0
    }

    pub fn stream_events_total(&self) -> usize {
        self.refresh_stream_admin_snapshot();
        self.admin_read_model
            .streams(None)
            .into_iter()
            .map(|stream| stream.offset.saturating_add(1) as usize)
            .sum()
    }

    pub fn stream_operations_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            return 0.0;
        }

        let total_operations =
            crate::boot::observability::metrics().counter_get("fitz_stream_operations_total");
        total_operations as f64 / uptime_secs
    }

    pub fn stream_subscriptions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.stream.subscription_count())
            .unwrap_or(0)
    }

    pub fn notice_publishes_per_second(&self) -> f64 {
        0.0
    }

    pub fn queue_operations_per_second(&self) -> f64 {
        0.0
    }

    pub fn rpc_operations_per_second(&self) -> f64 {
        0.0
    }

    pub fn lease_operations_per_second(&self) -> f64 {
        0.0
    }

    pub fn schedule_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.schedule_count())
            .unwrap_or_else(|| self.admin_read_model.schedules(None).len())
    }

    pub fn schedule_executions_per_minute(&self) -> f64 {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.executions_per_minute())
            .unwrap_or(0.0)
    }

    pub fn schedule_subscriptions_active(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.subscription_count())
            .unwrap_or(0)
    }

    pub fn schedule_pending_fires(&self) -> usize {
        self.domains
            .read()
            .as_ref()
            .map(|domains| domains.schedule.pending_fire_count())
            .unwrap_or(0)
    }
}
