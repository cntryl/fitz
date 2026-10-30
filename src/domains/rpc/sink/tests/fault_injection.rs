//! Fault-injection tests for the RPC sink: force a send to fail at a
//! mutate -> send -> (rollback | wake | measure) boundary and assert the
//! state-model invariant that must hold afterwards.

use super::*;
use crate::runtime::Mailbox;
use bytes::Bytes;

/// A worker-side sink that fails delivery a fixed number of times, then
/// succeeds and records every envelope it actually accepts.
struct FlakyWorkerSink {
    remaining_failures: std::sync::atomic::AtomicUsize,
    error: DeliveryError,
    delivered: parking_lot::Mutex<Vec<Envelope>>,
}

impl FlakyWorkerSink {
    fn new(failures: usize, error: DeliveryError) -> Self {
        Self {
            remaining_failures: std::sync::atomic::AtomicUsize::new(failures),
            error,
            delivered: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

impl MailboxSink for FlakyWorkerSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        use std::sync::atomic::Ordering;
        let mut current = self.remaining_failures.load(Ordering::Acquire);
        loop {
            if current == 0 {
                self.delivered.lock().push(envelope);
                return Ok(());
            }
            match self.remaining_failures.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Err(self.error.clone()),
                Err(observed) => current = observed,
            }
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

fn submit_request(
    sink: &RpcDomain,
    family: RouteFamily,
    route: &Route,
    caller_session_id: u64,
) -> uuid::Uuid {
    let correlation_id = uuid::Uuid::new_v4();
    let request = crate::domains::rpc::RpcClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            caller_session_id,
            crate::runtime::ClientChannel::Rpc,
            302,
            family,
        ),
        Ok(crate::domains::rpc::RpcMessage::Request(
            crate::domains::rpc::RpcRequest::new(
                family,
                correlation_id,
                route.clone(),
                bytes::Bytes::from_static(b"request"),
            ),
        )),
    );
    sink.deliver(Envelope::from_route(
        session_inbox_address(family, caller_session_id),
        RouteAddress::new(family, Route::new("rpc://inbound")),
        request,
    ))
    .expect("deliver RPC request");
    correlation_id
}

#[test]
fn should_release_failed_family_pending_capacity_for_healthy_families() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let failed_family = RouteFamily::new(1);
    let healthy_family = RouteFamily::new(2);
    let sink = RpcDomain::new_with_families(
        router.clone(),
        admin_read_model.clone(),
        &[failed_family, healthy_family],
    )
    .with_global_pending_capacity_for_tests(1);
    let failed_route = Route::new("rpc://acme/work/failed-family");
    let healthy_route = Route::new("rpc://acme/work/healthy-family");
    let failed_caller = session_inbox_address(failed_family, 7);
    let healthy_caller = session_inbox_address(healthy_family, 8);
    let failed_worker = session_inbox_address(failed_family, 42);
    let healthy_worker = session_inbox_address(healthy_family, 43);
    let failed_caller_mailbox = Arc::new(Mailbox::new(4));
    let healthy_caller_mailbox = Arc::new(Mailbox::new(4));
    let failed_worker_mailbox = Arc::new(Mailbox::new(4));
    let healthy_worker_mailbox = Arc::new(Mailbox::new(4));
    router.register(failed_caller, failed_caller_mailbox);
    router.register(healthy_caller, healthy_caller_mailbox.clone());
    router.register(failed_worker.clone(), failed_worker_mailbox.clone());
    router.register(healthy_worker.clone(), healthy_worker_mailbox.clone());
    sink.register_registration_for_tests(RpcWorker::with_stats(
        RouteAddress::new(failed_family, failed_route.clone()),
        failed_worker,
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    sink.register_registration_for_tests(RpcWorker::with_stats(
        RouteAddress::new(healthy_family, healthy_route.clone()),
        healthy_worker,
        43,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));

    // Act
    submit_request(&sink, failed_family, &failed_route, 7);
    failed_worker_mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .expect("failed family request dispatched before panic");
    sink.sync_admin_snapshot();
    sink.panic_family_actor_for_failpoint(failed_family);
    let deadline = Instant::now() + Duration::from_secs(1);
    while !sink
        .family_health_snapshot()
        .failed_families
        .contains(&failed_family)
        && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert!(
        sink.family_health_snapshot()
            .failed_families
            .contains(&failed_family),
        "injected family failure should be observed"
    );
    submit_request(&sink, healthy_family, &healthy_route, 8);
    let healthy_dispatch = healthy_worker_mailbox
        .receiver()
        .recv_timeout(Duration::from_millis(100));

    // Assert
    assert!(
        healthy_dispatch.is_ok(),
        "a failed family's abandoned pending call must not consume healthy-family capacity"
    );
    assert_eq!(
        sink.config
            .global_pending_count
            .load(std::sync::atomic::Ordering::Acquire),
        1,
        "only the healthy family's dispatched request remains pending"
    );
    assert!(admin_read_model
        .rpc_workers(None)
        .iter()
        .all(|worker| { worker.route_family != failed_family.as_u64() }));
    assert!(admin_read_model
        .rpc_pending(None)
        .iter()
        .all(|pending| { pending.route_family != failed_family.as_u64() }));
    assert!(healthy_caller_mailbox.receiver().try_recv().is_err());
}

/// Sequence: request accepted -> worker slot claimed + pending recorded ->
/// immediate dispatch to the worker fails `MailboxFull`.
///
/// Invariant: the pending entry is removed, the caller is told, and the
/// worker's slot is released so a following request can use it again -
/// exercised here by proving a second request reaches the same worker.
#[test]
fn should_recover_worker_credit_after_immediate_dispatch_hits_mailbox_full() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomain::new(router.clone(), admin_read_model);
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/mailbox-full");
    let caller = session_inbox_address(family, 7);
    let worker_inbox = session_inbox_address(family, 42);
    let caller_mailbox = Arc::new(Mailbox::new(4));
    router.register(caller.clone(), caller_mailbox.clone());
    let worker_sink = Arc::new(FlakyWorkerSink::new(
        1,
        DeliveryError::MailboxFull {
            capacity: 1,
            current_len: 1,
        },
    ));
    router.register(
        worker_inbox.clone(),
        worker_sink.clone() as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(RpcWorker::with_stats(
        RouteAddress::new(family, route.clone()),
        worker_inbox,
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));

    // Act
    // The first request hits the flaky failure on immediate dispatch.
    submit_request(&sink, family, &route, 7);
    let response = caller_mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .expect("caller terminal response")
        .into_payload::<FrameContext>()
        .expect("RPC response frame");
    let response = parse_forwarded_rpc_response(&response);
    let (code, _) = crate::dispatch::protocol::rpc_codec::decode_error_body(&response.body)
        .expect("RPC error body");
    let pending_after_rejection = sink.pending_request_count();
    let workers_after_rejection = sink.worker_count();
    // A second request must be able to claim the same worker's slot,
    // proving the failed attempt released its credit instead of leaking it.
    submit_request(&sink, family, &route, 8);

    // Assert
    assert_eq!(code, crate::dispatch::protocol::error_codes::rpc::ERR_BUSY);
    assert_eq!(pending_after_rejection, 0);
    assert_eq!(
        workers_after_rejection, 1,
        "worker registration must survive a full mailbox"
    );
    assert_eq!(sink.pending_request_count(), 1);
    assert_eq!(worker_sink.delivered.lock().len(), 1);
}

/// Sequence: request accepted -> worker slot claimed + pending recorded ->
/// immediate dispatch fails `ActorStopped` (worker actually gone).
///
/// Invariant: the dead registration is cleaned up, the caller receives
/// exactly one terminal error (no duplicate frame from both the disconnect
/// cleanup path and the direct rejection path).
#[test]
fn should_send_single_terminal_error_when_immediate_dispatch_hits_actor_stopped() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomain::new(router.clone(), admin_read_model);
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/actor-stopped");
    let caller = session_inbox_address(family, 7);
    let worker_inbox = session_inbox_address(family, 42);
    let caller_mailbox = Arc::new(Mailbox::new(4));
    router.register(caller.clone(), caller_mailbox.clone());
    let worker_sink = Arc::new(FlakyWorkerSink::new(1, DeliveryError::ActorStopped));
    router.register(worker_inbox.clone(), worker_sink as Arc<dyn MailboxSink>);
    sink.register_registration_for_tests(RpcWorker::with_stats(
        RouteAddress::new(family, route.clone()),
        worker_inbox,
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));

    // Act
    submit_request(&sink, family, &route, 7);

    // Assert
    // Exactly one terminal frame reaches the caller.
    let response = caller_mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .expect("caller terminal response")
        .into_payload::<FrameContext>()
        .expect("RPC response frame");
    let response = parse_forwarded_rpc_response(&response);
    let (code, _) = crate::dispatch::protocol::rpc_codec::decode_error_body(&response.body)
        .expect("RPC error body");
    assert_eq!(
        code,
        crate::dispatch::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND
    );
    assert!(
        caller_mailbox
            .receiver()
            .recv_timeout(Duration::from_millis(50))
            .is_err(),
        "caller must not receive a second, duplicate terminal frame"
    );
    assert_eq!(sink.pending_request_count(), 0);
    assert_eq!(
        sink.worker_count(),
        0,
        "dead worker registration must be removed"
    );
}

/// Sequence: REGISTER inserts a worker registration -> the REGISTER ack
/// cannot be delivered back to the registering session (its mailbox is
/// full).
///
/// Invariant: a registration whose caller never learned it succeeded must
/// not be left live - otherwise requests can be routed to a worker that
/// believes registration failed and never reads its inbox.
#[test]
fn should_roll_back_registration_when_register_ack_delivery_fails() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomain::new(router.clone(), admin_read_model);
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/register-ack");
    let worker_session_id = 42;
    let worker_addr = RouteAddress::new(family, route.clone());
    let worker_inbox = session_inbox_address(family, worker_session_id);
    let ack_mailbox = Arc::new(Mailbox::new(1));
    router.register(worker_inbox.clone(), ack_mailbox.clone());
    // Fill the worker's own inbox so the REGISTER ack cannot be delivered.
    ack_mailbox
        .deliver(Envelope::new(worker_inbox.clone(), Bytes::new()))
        .expect("fill ack mailbox");

    let register = crate::domains::rpc::RpcClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            worker_session_id,
            crate::runtime::ClientChannel::Rpc,
            300,
            family,
        ),
        Ok(crate::domains::rpc::RpcMessage::RegisterWorker {
            worker_addr: worker_addr.clone(),
            max_concurrent: 1,
        }),
    );

    // Act
    sink.deliver(Envelope::from_route(
        worker_inbox.clone(),
        RouteAddress::new(family, Route::new("rpc://inbound")),
        register,
    ))
    .expect("deliver REGISTER");

    // Assert
    // The registration must not survive an ack it never reached.
    assert_eq!(
        sink.worker_count(),
        0,
        "registration must roll back when its ack cannot be delivered"
    );
}
