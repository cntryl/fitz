//! Worker registration and unregistration message handling.

use super::state_model::{
    session_inbox_address, Envelope, RpcClientResponseBody, RpcDeliveryOutcome as DeliveryOutcome,
    RpcDomainRuntime, RpcRequestState, RpcWorker, RpcWorkerRegistration,
};

impl RpcDomainRuntime<'_> {
    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn handle_register_worker_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        worker_addr: crate::runtime::routing::RouteAddress,
        max_concurrent: usize,
    ) -> DeliveryOutcome {
        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
            session_inbox_address(*envelope.destination().family(), meta.session_id)
        });
        {
            let mut state = self.state.lock();
            if matches!(
                RpcRequestState::register(
                    &mut *state,
                    RpcWorker::new(
                        worker_addr.clone(),
                        worker_inbox_addr,
                        meta.session_id,
                        max_concurrent,
                    )
                ),
                RpcWorkerRegistration::WildcardLimit
            ) {
                return (
                    Some(RpcClientResponseBody::CodeError {
                        code: crate::dispatch::protocol::error_codes::rpc::ERR_SUBSCRIPTION_LIMIT,
                        message: "wildcard subscription limit exceeded (128 per session)"
                            .to_string(),
                    }),
                    Some(false),
                    false,
                );
            }
        }
        tracing::debug!(
            domain = "rpc",
            worker = worker_addr.route().as_str(),
            session = meta.session_id,
            "Worker registered"
        );
        self.refresh_metrics_gauges();
        (
            Some(RpcClientResponseBody::Ok { data: vec![] }),
            Some(true),
            false,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn handle_unregister_worker_message(
        &self,
        meta: &crate::runtime::ClientFrameMeta,
        worker_addr: crate::runtime::routing::RouteAddress,
    ) -> DeliveryOutcome {
        let cleanup_result = self.apply_worker_unsubscribe(&worker_addr, meta.session_id);
        self.forward_worker_disconnect_errors(cleanup_result.disconnect_deliveries);
        tracing::debug!(
            domain = "rpc",
            worker = worker_addr.route().as_str(),
            session = meta.session_id,
            removed_workers = cleanup_result.removed_registrations,
            removed_pending = cleanup_result.removed_pending,
            "Worker unregistered"
        );
        (
            Some(RpcClientResponseBody::Ok { data: vec![] }),
            Some(true),
            false,
        )
    }
}
