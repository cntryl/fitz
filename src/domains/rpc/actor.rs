//! RpcRouteActor: manages the worker pool and request queue for one exact RPC
//! route inside the current broker process.
//!
//! Each route key (for example, `rpc://acme/auth/user/create`) has a dedicated
//! actor that queues inbound requests, assigns them to registered workers using
//! round-robin distribution, and forwards responses back to clients.
//!
//! # State Model
//!
//! ```text
//! [Client Request] → [Queue] → [Worker Assignment + Lease] → [Worker Processing]
//!                                                              ↓
//!                      [Client Inbox] ← [Response Forwarding] ←
//! ```
//!
//! # Failure Model
//!
//! Worker registrations, queued requests, and active leases exist only inside
//! the running broker process. This actor does not provide durable retry,
//! restart recovery, or wildcard worker registration semantics.
//!
//! # Invariants
//!
//! 1. **FIFO ordering**: Requests are dispatched in arrival order
//! 2. **Round-robin**: Workers receive requests in rotation
//! 3. **Bounded queue**: Backpressure when queue is full
//! 4. **No durability**: Worker registrations, queue state, and leases are ephemeral
//! 5. **Lease enforcement**: Workers must respond before lease expiry
//! Correlation IDs are used only to match live in-flight responses in this
//! process; they are not durable deduplication or replay tokens.
//! 6. **Correlation tracking**: Maps correlation_id → worker for proper cleanup

use super::errors::RpcError;
use super::protocol::{RpcMessage, RpcRequest, RpcResponse, RpcWorkItem};
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::{RouteAddress, RouteFamily};
use fxhash::FxBuildHasher;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::time::{Duration, Instant};
use uuid::Uuid;

type FastMap<K, V> = HashMap<K, V, FxBuildHasher>;

/// Default queue capacity per route
const DEFAULT_QUEUE_CAPACITY: usize = 1000;

/// Default lease timeout (5 seconds)
const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Expiration queue entry for efficient lease timeout checking
///
/// Implements min-heap ordering (earliest expiration first) for O(K) expiration checks
/// where K is the number of expired leases, not total lease count.
#[derive(Debug, Clone, Eq, PartialEq)]
struct ExpiringLease {
    expiration: Instant,
    correlation_id: Uuid,
}

impl Ord for ExpiringLease {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (earliest expiration at top)
        other.expiration.cmp(&self.expiration)
    }
}

impl PartialOrd for ExpiringLease {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Tracks a leased request assigned to a worker
///
/// Optimized for minimal allocation:
/// - Stores worker_index instead of RouteAddress (no clone)
/// - Does not retain reply routing state while reply forwarding is a no-op
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Lease {
    worker_index: usize, // Index into workers vec (no allocation)
    expiration: Instant,
}

/// Worker registration for a route
#[derive(Debug, Clone)]
struct WorkerRegistration {
    addr: RouteAddress,
    /// Number of in-flight requests assigned to this worker
    in_flight: usize,
    /// Max concurrent requests this worker can handle
    max_concurrent: usize,
}

impl WorkerRegistration {
    fn new(addr: RouteAddress) -> Self {
        Self {
            addr,
            in_flight: 0,
            max_concurrent: 1, // Default: one request at a time
        }
    }

    fn is_available(&self) -> bool {
        self.in_flight < self.max_concurrent
    }
}

/// RPC route actor managing a single RPC route
///
/// Maintains a queue of pending requests and a pool of registered workers.
/// Dispatches requests to workers in round-robin fashion with lease tracking.
/// Uses a ready queue for O(1) worker selection.
pub struct RpcRouteActor {
    /// Route family this actor belongs to (for validation)
    family: RouteFamily,

    /// Queue of pending requests
    pending: VecDeque<RpcRequest>,

    /// Registered workers
    workers: Vec<WorkerRegistration>,

    /// Ready queue: indices of workers available to take requests (O(1) selection)
    ready_queue: VecDeque<usize>,

    /// Maximum queue size
    capacity: usize,

    /// Active leases (correlation_id → Lease)
    leases: FastMap<Uuid, Lease>,

    /// Expiration queue for O(K) expired lease detection (min-heap)
    expiration_queue: BinaryHeap<ExpiringLease>,

    /// Lease timeout duration
    lease_timeout: Duration,
}

impl RpcRouteActor {
    /// Create new RPC route actor with default capacity
    pub fn new(family: RouteFamily) -> Self {
        Self::with_capacity(family, DEFAULT_QUEUE_CAPACITY)
    }

    /// Create new RPC route actor with specific capacity
    pub fn with_capacity(family: RouteFamily, capacity: usize) -> Self {
        Self {
            family,
            pending: VecDeque::with_capacity(capacity), // Pre-allocate
            workers: Vec::with_capacity(16),            // Reserve space for typical worker count
            ready_queue: VecDeque::with_capacity(16),
            capacity,
            leases: HashMap::with_capacity_and_hasher(capacity, FxBuildHasher::default()), // Pre-allocate for expected load
            expiration_queue: BinaryHeap::with_capacity(capacity), // Pre-allocate for expected load
            lease_timeout: DEFAULT_LEASE_TIMEOUT,
        }
    }

    /// Create RPC route actor with custom lease timeout
    pub fn with_timeout(family: RouteFamily, capacity: usize, lease_timeout: Duration) -> Self {
        Self {
            family,
            pending: VecDeque::with_capacity(capacity),
            workers: Vec::with_capacity(16),
            ready_queue: VecDeque::with_capacity(16),
            capacity,
            leases: HashMap::with_capacity_and_hasher(capacity, FxBuildHasher::default()),
            expiration_queue: BinaryHeap::with_capacity(capacity),
            lease_timeout,
        }
    }

    /// Get number of pending requests
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get number of registered workers
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Get number of active leases
    pub fn active_leases(&self) -> usize {
        self.leases.len()
    }

    /// Handle worker subscription
    fn handle_subscribe(&mut self, worker_addr: RouteAddress, ctx: &mut Context<Self>) {
        // Add worker to pool
        let worker_idx = self.workers.len();
        self.workers.push(WorkerRegistration::new(worker_addr));

        // Add to ready queue (new workers are available)
        self.ready_queue.push_back(worker_idx);

        // Try to dispatch pending requests
        self.try_dispatch_pending(ctx);
    }

    /// Handle worker unsubscription
    fn handle_unsubscribe(&mut self, worker_addr: &RouteAddress) {
        if let Some(idx) = self.workers.iter().position(|w| w.addr == *worker_addr) {
            self.workers.remove(idx);

            // Remove from ready queue and adjust indices
            self.ready_queue.retain(|&i| i != idx);
            // Adjust indices > removed index
            for ready_idx in &mut self.ready_queue {
                if *ready_idx > idx {
                    *ready_idx -= 1;
                }
            }
        }
    }

    /// Handle incoming request
    fn handle_request(&mut self, request: RpcRequest, ctx: &mut Context<Self>) {
        // Validate route family matches actor family
        if request.family_id != self.family {
            let error = RpcError::new(
                request.correlation_id,
                crate::domains::rpc::errors::RpcErrorCode::InvalidRoute,
                format!(
                    "Route family mismatch: request family {:?} != actor family {:?}",
                    request.family_id, self.family
                ),
            );
            self.send_error(error);
            return;
        }

        // Check for expired leases first
        self.check_expired_leases(ctx);

        // Check queue capacity
        if self.pending.len() >= self.capacity {
            // Send backpressure error to client
            let error = RpcError::backpressure(request.correlation_id);
            self.send_error(error);
            return;
        }

        // Try immediate dispatch if workers available
        if self.has_available_worker() {
            self.dispatch_to_worker(request, ctx);
        } else {
            // Enqueue for later
            self.pending.push_back(request);
        }
    }

    /// Handle response from worker
    fn handle_response(&mut self, response: RpcResponse, ctx: &mut Context<Self>) {
        // Terminal responses release the worker lease. Late responses naturally no-op.
        if response.stream_end {
            self.release_lease(&response.correlation_id, ctx);
        }
    }

    /// Handle worker acknowledgment
    fn handle_ack(&mut self, correlation_id: Uuid, ctx: &mut Context<Self>) {
        // Worker completed processing, release the lease
        self.release_lease(&correlation_id, ctx);
    }

    /// Dispatch request to a worker using ready queue (O(1) selection)
    ///
    /// Optimized for zero-allocation hot path:
    /// - No request clone (already has ownership)
    /// - No worker_addr clone (use index)
    /// - Arc for reply_route (shared ownership)
    #[inline]
    fn dispatch_to_worker(&mut self, request: RpcRequest, ctx: &mut Context<Self>) {
        // Pop next ready worker from queue
        if let Some(idx) = self.ready_queue.pop_front() {
            // Track in-flight request
            self.workers[idx].in_flight += 1;
            let worker_addr = self.workers[idx].addr.clone(); // Only clone for work item

            // If worker still has capacity, put it back in ready queue
            if self.workers[idx].is_available() {
                self.ready_queue.push_back(idx);
            }

            // Create lease with minimal data
            let expiration = Instant::now() + self.lease_timeout;
            let lease = Lease {
                worker_index: idx, // Store index, not address
                expiration,
            };
            self.leases.insert(request.correlation_id, lease);

            // Add to expiration queue for O(K) timeout checking
            self.expiration_queue.push(ExpiringLease {
                expiration,
                correlation_id: request.correlation_id,
            });

            // Create work item
            let work_item = RpcWorkItem::from_request(&request);

            // Send REQUEST to worker actor (encoded as message type 302 on wire)
            let _ = ctx.send(
                worker_addr,
                crate::domains::rpc::protocol::RpcMessage::Deliver(work_item),
            );
        } else {
            // No available workers, re-queue
            self.pending.push_back(request);
        }
    }

    /// Release a lease without dispatching pending (internal use)
    ///
    /// Uses worker_index for O(1) lookup (no linear search needed)
    #[inline]
    fn release_lease_internal(&mut self, correlation_id: &Uuid) -> bool {
        if let Some(lease) = self.leases.remove(correlation_id) {
            // Direct O(1) lookup by index (no linear search)
            let idx = lease.worker_index;
            if idx < self.workers.len() {
                let worker = &mut self.workers[idx];
                let was_full = !worker.is_available();
                worker.in_flight = worker.in_flight.saturating_sub(1);

                // If worker was full and now has capacity, add back to ready queue
                if was_full && worker.is_available() {
                    self.ready_queue.push_back(idx);
                }
            }
            true
        } else {
            false
        }
    }

    /// Release a lease and try to dispatch pending requests
    fn release_lease(&mut self, correlation_id: &Uuid, ctx: &mut Context<Self>) {
        if self.release_lease_internal(correlation_id) {
            self.try_dispatch_pending(ctx);
        }
    }

    /// Check for expired leases and re-enqueue requests
    ///
    /// Optimized O(K) algorithm where K = number of expired leases:
    /// - Uses min-heap to avoid scanning all leases
    /// - Only processes actually expired entries
    /// - Maintains O(1) dispatch even with 10k+ in-flight requests
    fn check_expired_leases(&mut self, ctx: &mut Context<Self>) {
        let now = Instant::now();
        let mut had_expired = false;

        // Process expired leases from min-heap (O(K log N) where K = expired count)
        while let Some(entry) = self.expiration_queue.peek() {
            if entry.expiration > now {
                break; // All remaining leases are still valid
            }

            let expired = self.expiration_queue.pop().unwrap();
            let correlation_id = expired.correlation_id;

            // Only process if lease still exists (may have been released already)
            if let Some(lease) = self.leases.remove(&correlation_id) {
                had_expired = true;

                // Decrement worker in-flight count (O(1) with worker_index)
                let idx = lease.worker_index;
                if idx < self.workers.len() {
                    let worker = &mut self.workers[idx];
                    let was_full = !worker.is_available();
                    worker.in_flight = worker.in_flight.saturating_sub(1);

                    // If worker was full and now has capacity, add back to ready queue
                    if was_full && worker.is_available() {
                        self.ready_queue.push_back(idx);
                    }
                }

                // Timeout is currently observed only through lease release.
                // Error forwarding remains a no-op until reply inbox routing lands.
                let error = RpcError::timeout(correlation_id);
                self.send_error(error);

                // NOTE: We don't re-enqueue for retry since we don't have the original request
                // (removed request clone for performance). Client should retry if needed.
            }
        }

        // Try to dispatch pending requests if we freed up workers
        if had_expired {
            self.try_dispatch_pending(ctx);
        }
    }

    /// Send error to client inbox
    fn send_error(&self, _error: RpcError) {
        // NOTE: This actor is a semantics reference used in tests and benchmarks.
        // Production RPC reply and error forwarding happens in RpcDomainSink,
        // which still treats all worker and pending-request state as ephemeral.
    }

    /// Try to dispatch pending requests to available workers
    fn try_dispatch_pending(&mut self, ctx: &mut Context<Self>) {
        while !self.pending.is_empty() && self.has_available_worker() {
            if let Some(request) = self.pending.pop_front() {
                self.dispatch_to_worker(request, ctx);
            }
        }
    }

    /// Check if any worker is available (O(1) with ready_queue)
    fn has_available_worker(&self) -> bool {
        !self.ready_queue.is_empty()
    }
}

impl Actor for RpcRouteActor {
    type Message = RpcMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            RpcMessage::Subscribe { worker_addr } => {
                self.handle_subscribe(worker_addr, ctx);
            }
            RpcMessage::Unsubscribe { worker_addr } => {
                self.handle_unsubscribe(&worker_addr);
            }
            RpcMessage::Request(request) => {
                self.handle_request(request, ctx);
            }
            RpcMessage::Response(response) => {
                self.handle_response(response, ctx);
            }
            RpcMessage::Ack { correlation_id } => {
                self.handle_ack(correlation_id, ctx);
            }
            RpcMessage::Deliver(_) => {
                // Deliver is sent TO workers, not received by route actor
                // Ignore if misrouted
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::Route;
    use bytes::Bytes;

    #[test]
    fn should_create_actor_with_default_capacity() {
        let actor = RpcRouteActor::new(RouteFamily::new(1));
        assert_eq!(
            (actor.capacity, actor.pending_count(), actor.worker_count()),
            (DEFAULT_QUEUE_CAPACITY, 0, 0)
        );
    }

    #[test]
    fn should_create_actor_with_custom_capacity() {
        let actor = RpcRouteActor::with_capacity(RouteFamily::new(1), 500);
        assert_eq!(actor.capacity, 500);
    }

    #[test]
    fn should_register_worker() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("rpc://test/route"));
        let mut ctx = Context::new(addr, router);

        let worker_addr =
            RouteAddress::new(RouteFamily::new(1), Route::new("worker://test/worker1"));

        // Act
        actor.handle_subscribe(worker_addr, &mut ctx);

        // Assert
        assert_eq!(actor.worker_count(), 1);
    }

    #[test]
    fn should_unregister_worker() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("rpc://test/route"));
        let mut ctx = Context::new(addr, router);

        let worker_addr =
            RouteAddress::new(RouteFamily::new(1), Route::new("worker://test/worker1"));

        actor.handle_subscribe(worker_addr.clone(), &mut ctx);

        // Act
        actor.handle_unsubscribe(&worker_addr);

        // Assert
        assert_eq!(actor.worker_count(), 0);
    }

    #[test]
    fn should_enqueue_request_when_no_workers() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("rpc://test/route"));
        let mut ctx = Context::new(addr, router);

        let request = RpcRequest {
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://test/route"),
            reply_route: Route::new("inbox://session/123"),
            body: Bytes::from(vec![1, 2, 3]),
        };

        // Act
        actor.handle_request(request, &mut ctx);

        // Assert
        assert_eq!(actor.pending_count(), 1);
    }

    fn make_ctx() -> (
        Context<RpcRouteActor>,
        std::sync::Arc<crate::runtime::router::Router>,
    ) {
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("rpc://test/route"));
        let ctx = Context::new(addr, router.clone());
        (ctx, router)
    }

    fn make_worker_addr(n: u64) -> RouteAddress {
        RouteAddress::new(
            RouteFamily::new(1),
            Route::new(format!("worker://test/worker{}", n)),
        )
    }

    fn make_request(family: u64) -> RpcRequest {
        RpcRequest {
            family_id: RouteFamily::new(family),
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://test/route"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from("payload"),
        }
    }

    #[test]
    fn should_dispatch_request_immediately_when_worker_is_available() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let (mut ctx, _router) = make_ctx();
        actor.handle_subscribe(make_worker_addr(1), &mut ctx);
        let request = make_request(1);

        // Act
        actor.handle_request(request, &mut ctx);

        // Assert — request dispatched, not queued; one lease created
        assert_eq!(actor.pending_count(), 0);
        assert_eq!(actor.active_leases(), 1);
    }

    #[test]
    fn should_reject_request_with_mismatched_route_family() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let (mut ctx, _router) = make_ctx();
        actor.handle_subscribe(make_worker_addr(1), &mut ctx);
        // Request uses family 2, actor is family 1
        let request = make_request(2);

        // Act
        actor.handle_request(request, &mut ctx);

        // Assert — misrouted request: nothing queued, no lease
        assert_eq!(actor.pending_count(), 0);
        assert_eq!(actor.active_leases(), 0);
    }

    #[test]
    fn should_reject_request_when_queue_is_full() {
        // Arrange — capacity of 2, no workers so requests queue
        let mut actor = RpcRouteActor::with_capacity(RouteFamily::new(1), 2);
        let (mut ctx, _router) = make_ctx();
        actor.handle_request(make_request(1), &mut ctx);
        actor.handle_request(make_request(1), &mut ctx);
        assert_eq!(actor.pending_count(), 2);

        // Act — one more request over capacity
        actor.handle_request(make_request(1), &mut ctx);

        // Assert — rejected: queue stays at 2
        assert_eq!(actor.pending_count(), 2);
    }

    #[test]
    fn should_release_lease_when_response_has_stream_end_true() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let (mut ctx, _router) = make_ctx();
        actor.handle_subscribe(make_worker_addr(1), &mut ctx);
        let request = make_request(1);
        let corr = request.correlation_id;
        actor.handle_request(request, &mut ctx);
        assert_eq!(actor.active_leases(), 1);

        // Act
        let response = RpcResponse::single(corr, Bytes::from("result"));
        actor.handle_response(response, &mut ctx);

        // Assert
        assert_eq!(actor.active_leases(), 0);
    }

    #[test]
    fn should_retain_lease_when_response_has_stream_end_false() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let (mut ctx, _router) = make_ctx();
        actor.handle_subscribe(make_worker_addr(1), &mut ctx);
        let request = make_request(1);
        let corr = request.correlation_id;
        actor.handle_request(request, &mut ctx);

        // Act — intermediate streaming chunk (not terminal)
        let chunk = RpcResponse::chunk(corr, 0, Bytes::from("partial"), false);
        actor.handle_response(chunk, &mut ctx);

        // Assert
        assert_eq!(actor.active_leases(), 1);
    }

    #[test]
    fn should_release_lease_on_ack() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let (mut ctx, _router) = make_ctx();
        actor.handle_subscribe(make_worker_addr(1), &mut ctx);
        let request = make_request(1);
        let corr = request.correlation_id;
        actor.handle_request(request, &mut ctx);
        assert_eq!(actor.active_leases(), 1);

        // Act
        actor.handle_ack(corr, &mut ctx);

        // Assert
        assert_eq!(actor.active_leases(), 0);
    }

    #[test]
    fn should_be_noop_when_unsubscribing_unknown_worker() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let unknown = make_worker_addr(99);

        // Act — no panic, count stays 0
        actor.handle_unsubscribe(&unknown);

        // Assert
        assert_eq!(actor.worker_count(), 0);
    }

    #[test]
    fn should_dispatch_pending_requests_when_worker_registers() {
        // Arrange — queue 2 requests before any worker exists
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let (mut ctx, _router) = make_ctx();
        actor.handle_request(make_request(1), &mut ctx);
        actor.handle_request(make_request(1), &mut ctx);
        assert_eq!(actor.pending_count(), 2);

        // Act — register a worker; should drain the pending queue
        actor.handle_subscribe(make_worker_addr(1), &mut ctx);

        // Assert — worker took one pending request (max_concurrent=1)
        assert_eq!(actor.pending_count(), 1);
        assert_eq!(actor.active_leases(), 1);
    }

    #[test]
    fn should_return_false_when_releasing_unknown_correlation_id() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let unknown_id = Uuid::new_v4();

        // Act
        let released = actor.release_lease_internal(&unknown_id);

        // Assert
        assert!(!released);
    }
}
