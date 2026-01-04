//! RpcRouteActor: manages worker pool and request queue for a single RPC route
//!
//! Each RPC route (e.g., `rpc://acme/auth/user/create`) has a dedicated actor
//! that queues inbound requests, assigns them to registered workers using
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
//! # Lease Mechanism
//!
//! Each request assigned to a worker gets a lease with expiration time.
//! If the worker doesn't respond before expiration, the request is re-enqueued
//! and assigned to another worker.
//!
//! # Invariants
//!
//! 1. **FIFO ordering**: Requests are dispatched in arrival order
//! 2. **Round-robin**: Workers receive requests in rotation
//! 3. **Bounded queue**: Backpressure when queue is full
//! 4. **No durability**: All state is ephemeral
//! 5. **Lease enforcement**: Workers must respond before lease expiry
//! 6. **Correlation tracking**: Maps correlation_id → worker for proper cleanup

use super::protocol::{RpcMessage, RpcRequest, RpcResponse, RpcWorkItem};
use super::errors::RpcError;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::{RouteAddress, RouteFamily};
use std::collections::{VecDeque, HashMap};
use std::time::{Duration, Instant};

/// Default queue capacity per route
const DEFAULT_QUEUE_CAPACITY: usize = 1000;

/// Default lease timeout (5 seconds)
const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Tracks a leased request assigned to a worker
#[derive(Debug, Clone)]
struct Lease {
    #[allow(dead_code)] // Used as HashMap key
    correlation_id: String,
    worker_addr: RouteAddress,
    request: RpcRequest,
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
pub struct RpcRouteActor {
    /// Route family this actor belongs to
    _family: RouteFamily,
    
    /// Queue of pending requests
    pending: VecDeque<RpcRequest>,
    
    /// Registered workers
    workers: Vec<WorkerRegistration>,
    
    /// Next worker index for round-robin
    next_worker_idx: usize,
    
    /// Maximum queue size
    capacity: usize,
    
    /// Active leases (correlation_id → Lease)
    leases: HashMap<String, Lease>,
    
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
            _family: family,
            pending: VecDeque::new(),
            workers: Vec::new(),
            next_worker_idx: 0,
            capacity,
            leases: HashMap::new(),
            lease_timeout: DEFAULT_LEASE_TIMEOUT,
        }
    }
    
    /// Create RPC route actor with custom lease timeout
    pub fn with_timeout(family: RouteFamily, capacity: usize, lease_timeout: Duration) -> Self {
        Self {
            _family: family,
            pending: VecDeque::new(),
            workers: Vec::new(),
            next_worker_idx: 0,
            capacity,
            leases: HashMap::new(),
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
        self.workers.push(WorkerRegistration::new(worker_addr));
        
        // Try to dispatch pending requests
        self.try_dispatch_pending(ctx);
    }
    
    /// Handle worker unsubscription
    fn handle_unsubscribe(&mut self, worker_addr: &RouteAddress) {
        self.workers.retain(|w| w.addr != *worker_addr);
    }
    
    /// Handle incoming request
    fn handle_request(&mut self, request: RpcRequest, ctx: &mut Context<Self>) {
        // Check for expired leases first
        self.check_expired_leases(ctx);
        
        // Check queue capacity
        if self.pending.len() >= self.capacity {
            // Send backpressure error to client
            let error = RpcError::backpressure(request.correlation_id.clone());
            let reply_route = request.reply_route.clone();
            self.send_error(error, &reply_route);
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
        // Check if we have a lease for this correlation ID
        if let Some(lease) = self.leases.get(&response.correlation_id) {
            let reply_route = lease.request.reply_route.clone();
            
            // Forward response to client inbox
            // TODO: Send to ReplyInboxActor at reply_route
            let _ = reply_route; // Silence unused warning for now
            
            // If this is the final chunk, release the lease
            if response.stream_end {
                let correlation_id = response.correlation_id.clone();
                self.release_lease(&correlation_id, ctx);
            }
        }
        // Else: late response after lease expired, drop it
    }
    
    /// Handle worker acknowledgment
    fn handle_ack(&mut self, correlation_id: String, ctx: &mut Context<Self>) {
        // Worker completed processing, release the lease
        self.release_lease(&correlation_id, ctx);
    }
    
    /// Dispatch request to a worker using round-robin
    fn dispatch_to_worker(&mut self, request: RpcRequest, _ctx: &mut Context<Self>) {
        // Find next available worker
        let available_worker_idx = (0..self.workers.len())
            .map(|i| (self.next_worker_idx + i) % self.workers.len())
            .find(|&idx| self.workers[idx].is_available());
        
        if let Some(idx) = available_worker_idx {
            self.next_worker_idx = (idx + 1) % self.workers.len();
            
            // Track in-flight request
            self.workers[idx].in_flight += 1;
            let worker_addr = self.workers[idx].addr.clone();
            
            // Create lease
            let lease = Lease {
                correlation_id: request.correlation_id.clone(),
                worker_addr: worker_addr.clone(),
                request: request.clone(),
                expiration: Instant::now() + self.lease_timeout,
            };
            self.leases.insert(request.correlation_id.clone(), lease);
            
            // Create work item
            let work_item = RpcWorkItem::from_request(&request);
            
            // Send to worker (TODO: integrate with actor messaging)
            let _ = (work_item, worker_addr); // Silence unused warnings
        } else {
            // No available workers, re-queue
            self.pending.push_back(request);
        }
    }
    
    /// Release a lease without dispatching pending (internal use)
    fn release_lease_internal(&mut self, correlation_id: &str) {
        if let Some(lease) = self.leases.remove(correlation_id) {
            // Find the worker and decrement in-flight count
            if let Some(worker) = self.workers.iter_mut()
                .find(|w| w.addr == lease.worker_addr) 
            {
                worker.in_flight = worker.in_flight.saturating_sub(1);
            }
        }
    }
    
    /// Release a lease and try to dispatch pending requests
    fn release_lease(&mut self, correlation_id: &str, ctx: &mut Context<Self>) {
        self.release_lease_internal(correlation_id);
        self.try_dispatch_pending(ctx);
    }
    
    /// Check for expired leases and re-enqueue requests
    fn check_expired_leases(&mut self, ctx: &mut Context<Self>) {
        let now = Instant::now();
        let expired: Vec<String> = self.leases.iter()
            .filter(|(_, lease)| lease.expiration <= now)
            .map(|(id, _)| id.clone())
            .collect();
        
        let had_expired = !expired.is_empty();
        
        for correlation_id in expired {
            if let Some(lease) = self.leases.remove(&correlation_id) {
                // Decrement worker in-flight count
                if let Some(worker) = self.workers.iter_mut()
                    .find(|w| w.addr == lease.worker_addr) 
                {
                    worker.in_flight = worker.in_flight.saturating_sub(1);
                }
                
                // Send timeout error to client
                let error = RpcError::timeout(correlation_id);
                let reply_route = lease.request.reply_route.clone();
                self.send_error(error, &reply_route);
                
                // Re-enqueue the request for retry
                self.pending.push_back(lease.request);
            }
        }
        
        // Try to dispatch pending requests if we freed up workers
        if had_expired {
            self.try_dispatch_pending(ctx);
        }
    }
    
    /// Send error to client inbox
    fn send_error(&self, _error: RpcError, _reply_route: &crate::runtime::routing::Route) {
        // TODO: Send to ReplyInboxActor at reply_route
    }
    
    /// Try to dispatch pending requests to available workers
    fn try_dispatch_pending(&mut self, ctx: &mut Context<Self>) {
        while !self.pending.is_empty() && self.has_available_worker() {
            if let Some(request) = self.pending.pop_front() {
                self.dispatch_to_worker(request, ctx);
            }
        }
    }
    
    /// Check if any worker is available
    fn has_available_worker(&self) -> bool {
        self.workers.iter().any(|w| w.is_available())
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::Route;
    
    #[test]
    fn should_create_actor_with_default_capacity() {
        let actor = RpcRouteActor::new(RouteFamily::new(1));
        assert_eq!((actor.capacity, actor.pending_count(), actor.worker_count()), 
                   (DEFAULT_QUEUE_CAPACITY, 0, 0));
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
        let addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new("rpc://test/route"),
        );
        let mut ctx = Context::new(addr, router);
        
        let worker_addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new("worker://test/worker1"),
        );
        
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
        let addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new("rpc://test/route"),
        );
        let mut ctx = Context::new(addr, router);
        
        let worker_addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new("worker://test/worker1"),
        );
        
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
        let addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new("rpc://test/route"),
        );
        let mut ctx = Context::new(addr, router);
        
        let request = RpcRequest {
            correlation_id: "test-001".to_string(),
            route: Route::new("rpc://test/route"),
            reply_route: Route::new("inbox://session/123"),
            body: vec![1, 2, 3],
        };
        
        // Act
        actor.handle_request(request, &mut ctx);
        
        // Assert
        assert_eq!(actor.pending_count(), 1);
    }
}
