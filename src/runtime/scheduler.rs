// LAYER: RUNTIME
//! Actor scheduling and execution coordination

use super::actor::{Actor, ActorError, ActorMetrics, ActorRef, Context};
use super::mailbox::Mailbox;
use crate::runtime::router::Router;
use crate::runtime::routing::RouteAddress;
use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Maximum high-priority messages per tick
const MAX_HIGH_PER_TICK: usize = 4;

/// Maximum normal-priority messages per tick (when high lane is active)
const MAX_NORMAL_PER_TICK: usize = 12;

/// Maximum time budget per tick in milliseconds
/// Prevents one actor from monopolizing the worker thread
const MAX_TICK_DURATION_MS: u64 = 5;

/// Minimum timeout for mailbox polling (adaptive based on load)
const MIN_POLL_TIMEOUT_MS: u64 = 10;

/// Maximum timeout for mailbox polling (when mailbox is empty)
const MAX_POLL_TIMEOUT_MS: u64 = 100;

/// Actor system scheduler that manages actor lifecycles and message processing
pub struct Scheduler {
    router: Arc<Router>,
    running: Arc<AtomicBool>,
    #[allow(dead_code)]
    worker_threads: usize,
}

impl Scheduler {
    /// Create a new scheduler with the specified number of worker threads
    pub fn new(worker_threads: usize) -> Self {
        Self {
            router: Arc::new(Router::new()),
            running: Arc::new(AtomicBool::new(false)),
            worker_threads: worker_threads.max(1),
        }
    }

    /// Create a scheduler with a shared router
    pub fn with_router(router: Arc<Router>, worker_threads: usize) -> Self {
        Self {
            router,
            running: Arc::new(AtomicBool::new(false)),
            worker_threads: worker_threads.max(1),
        }
    }

    /// Get a reference to the router
    pub fn router(&self) -> Arc<Router> {
        self.router.clone()
    }

    /// Spawn a new actor and return its reference
    ///
    /// The caller must provide a RouteAddress for the actor. The scheduler will:
    /// - Register the actor's mailbox with the router at the given address
    /// - Start a dedicated thread for message processing
    /// - Return an ActorRef for sending messages to the actor
    ///
    /// # Message Processing
    ///
    /// The actor processes messages in batches (up to 16 per iteration) to reduce
    /// scheduling overhead. Poll timeout is adaptive based on mailbox occupancy.
    pub fn spawn<A>(
        &self,
        mut actor: A,
        address: RouteAddress,
        mailbox_capacity: usize,
    ) -> ActorRef<A::Message>
    where
        A: Actor,
        A::Message: Any + Send + Sync + 'static,
    {
        let mailbox = Mailbox::new(mailbox_capacity);
        let actor_ref = ActorRef::new(address.clone(), self.router.clone());
        let metrics = Arc::new(ActorMetrics::new());

        // Register mailbox with router
        self.router
            .register(address.clone(), Arc::new(mailbox.clone()));

        let receiver = mailbox.receiver().clone();
        let high_receiver = mailbox.high_priority_receiver().clone();
        let router_clone = self.router.clone();
        let metrics_clone = metrics.clone();

        // Spawn actor execution thread
        thread::spawn(move || {
            let mut ctx = Context::with_metrics(address.clone(), router_clone, metrics_clone);

            // Call started hook
            actor.started(&mut ctx);

            // Process messages with two-phase priority lanes
            while ctx.is_running() {
                // Adaptive timeout based on mailbox occupancy
                let occupancy = mailbox.len() as f64 / mailbox.capacity() as f64;
                let timeout_ms = if occupancy > 0.5 {
                    MIN_POLL_TIMEOUT_MS // Fast drain when mailbox is filling up
                } else {
                    MAX_POLL_TIMEOUT_MS // Standard polling when idle
                };

                // Track tick start for time budget enforcement
                let tick_start = Instant::now();
                let mut processed_high = 0;
                let mut processed_normal = 0;

                // PHASE 1: High-priority messages (capped at MAX_HIGH_PER_TICK)
                while processed_high < MAX_HIGH_PER_TICK {
                    // INVARIANT: Time budget check to prevent thread monopolization
                    if tick_start.elapsed().as_millis() as u64 >= MAX_TICK_DURATION_MS {
                        break;
                    }

                    let envelope = match high_receiver.try_recv() {
                        Ok(env) => env,
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => {
                            ctx.stop();
                            break;
                        }
                    };

                    let start = Instant::now();

                    // Check deadline before processing
                    if envelope.is_expired() {
                        ctx.metrics().record_expired();
                        eprintln!(
                            "Dropped expired high-priority message {:?} for actor {:?}",
                            envelope.id(),
                            address
                        );
                        processed_high += 1;
                        continue;
                    }

                    // Process high-priority message
                    process_envelope(envelope, &mut actor, &mut ctx, &address, start);
                    processed_high += 1;
                }

                // PHASE 2: Normal-priority messages (remaining budget)
                // If high lane was idle, use full budget (16), otherwise use 12
                let normal_budget = if processed_high == 0 {
                    MAX_HIGH_PER_TICK + MAX_NORMAL_PER_TICK // 16 total when high is idle
                } else {
                    MAX_NORMAL_PER_TICK // 12 when high used its quota
                };

                while processed_normal < normal_budget {
                    // INVARIANT: Time budget check to prevent thread monopolization
                    if tick_start.elapsed().as_millis() as u64 >= MAX_TICK_DURATION_MS {
                        break;
                    }

                    let envelope = if processed_high == 0 && processed_normal == 0 {
                        // First message overall: use blocking receive with timeout
                        match receiver.recv_timeout(Duration::from_millis(timeout_ms)) {
                            Ok(env) => env,
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => break,
                            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                                ctx.stop();
                                break;
                            }
                        }
                    } else {
                        // Subsequent messages: try non-blocking receive
                        match receiver.try_recv() {
                            Ok(env) => env,
                            Err(_) => break, // No more messages, yield
                        }
                    };

                    let start = Instant::now();

                    // Check deadline before processing
                    if envelope.is_expired() {
                        ctx.metrics().record_expired();
                        eprintln!(
                            "Dropped expired message {:?} for actor {:?}",
                            envelope.id(),
                            address
                        );
                        processed_normal += 1;
                        continue;
                    }

                    // Process normal-priority message
                    process_envelope(envelope, &mut actor, &mut ctx, &address, start);
                    processed_normal += 1;
                }
            }

            // Call stopped hook
            actor.stopped();
        });

        actor_ref
    }

    /// Start the scheduler
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    /// Stop the scheduler
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if the scheduler is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new(num_cpus::get())
    }
}

/// Helper function to process a single envelope
fn process_envelope<A: Actor>(
    envelope: crate::runtime::envelope::Envelope,
    actor: &mut A,
    ctx: &mut Context<A>,
    address: &RouteAddress,
    start: Instant,
) where
    A::Message: Any + Send + Sync + 'static,
{
    // Extract typed message and metadata from envelope
    let (metadata, msg) = envelope.into_parts::<A::Message>();

    let msg = match msg {
        Some(m) => m,
        None => {
            // Type mismatch - record metric and notify actor
            ctx.metrics().record_type_mismatch();
            
            let error = ActorError::TypeMismatch {
                expected: std::any::type_name::<A::Message>().to_string(),
                envelope_id: metadata.id.as_u64(),
            };
            
            eprintln!(
                "Type mismatch: envelope {:?} for actor {:?} - expected {}, got different type",
                metadata.id, address, std::any::type_name::<A::Message>()
            );
            
            actor.on_error(error, ctx);
            return;
        }
    };

    // Set current metadata for causation tracking (no allocation)
    ctx.set_current_metadata(metadata);

    // Process message with panic recovery
    // INVARIANT: Panic => Stop. Supervisor restarts if configured.
    if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        actor.receive(msg, ctx);
    })) {
        // Structured panic error
        eprintln!(
            "Actor {:?} panicked during message processing: {:?}\nStopping actor. Supervisor will handle restart.",
            address, e
        );
        
        ctx.metrics().record_panic();
        let error = ActorError::Panic(format!("{:?}", e));
        
        // Call error handler but actor is now stopped
        actor.on_error(error, ctx);
        
        // CRITICAL: Stop actor immediately. No further message processing.
        ctx.stop();
    } else {
        // Record successful processing
        let elapsed = start.elapsed().as_micros() as u64;
        ctx.metrics().record_processed(elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::envelope::Envelope;
    use crate::runtime::routing::{Route, RouteFamily};

    fn test_address(family: u64, route: &str) -> RouteAddress {
        RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
    }

    #[derive(Debug)]
    enum TestMsg {
        Increment,
        GetCount(crossbeam_channel::Sender<u32>),
        Stop,
    }

    struct CounterActor {
        count: u32,
    }

    impl Actor for CounterActor {
        type Message = TestMsg;

        fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
            match msg {
                TestMsg::Increment => {
                    self.count += 1;
                }
                TestMsg::GetCount(reply) => {
                    let _ = reply.send(self.count);
                }
                TestMsg::Stop => {
                    ctx.stop();
                }
            }
        }
    }

    #[test]
    fn should_create_scheduler_with_workers() {
        // Arrange
        let worker_count = 4;

        // Act
        let scheduler = Scheduler::new(worker_count);

        // Assert
        assert!(!scheduler.is_running());
    }

    #[test]
    fn should_start_scheduler() {
        // Arrange
        let scheduler = Scheduler::new(2);

        // Act
        scheduler.start();

        // Assert
        assert!(scheduler.is_running());
    }

    #[test]
    fn should_stop_scheduler() {
        // Arrange
        let scheduler = Scheduler::new(2);
        scheduler.start();

        // Act
        scheduler.stop();

        // Assert
        assert!(!scheduler.is_running());
    }

    #[test]
    fn should_generate_unique_actor_ids() {
        // Arrange
        let scheduler = Scheduler::new(2);
        scheduler.start();
        let actor1 = CounterActor { count: 0 };
        let actor2 = CounterActor { count: 0 };

        // Act
        let ref1 = scheduler.spawn(actor1, test_address(1, "/test/actor1"), 10);
        let ref2 = scheduler.spawn(actor2, test_address(1, "/test/actor2"), 10);

        // Assert
        assert_ne!(ref1.address(), ref2.address());
    }

    #[test]
    fn should_process_messages_in_sequence() {
        // Arrange
        let scheduler = Scheduler::new(1);
        scheduler.start();
        let actor = CounterActor { count: 0 };
        let actor_ref = scheduler.spawn(actor, test_address(1, "/test/counter"), 10);

        // Act
        actor_ref.send(TestMsg::Increment).unwrap();
        actor_ref.send(TestMsg::Increment).unwrap();
        actor_ref.send(TestMsg::Increment).unwrap();
        let (tx, rx) = crossbeam_channel::bounded(1);
        actor_ref.send(TestMsg::GetCount(tx)).unwrap();
        let count = rx.recv_timeout(Duration::from_secs(1)).unwrap();

        // Assert
        assert_eq!(count, 3);

        actor_ref.send(TestMsg::Stop).unwrap();
    }

    #[test]
    fn should_drop_expired_messages() {
        // Arrange
        let scheduler = Scheduler::new(1);
        scheduler.start();
        let actor = CounterActor { count: 0 };
        let address = test_address(1, "/test/counter");
        let actor_ref = scheduler.spawn(actor, address.clone(), 10);

        // Send a message with an already-expired deadline
        let past_deadline = std::time::Instant::now() - Duration::from_secs(1);
        let expired_envelope =
            Envelope::new(address, TestMsg::Increment).with_deadline(past_deadline);
        scheduler.router().route(expired_envelope).unwrap();

        // Send a valid message to verify actor is still working
        actor_ref.send(TestMsg::Increment).unwrap();

        // Act
        let (tx, rx) = crossbeam_channel::bounded(1);
        actor_ref.send(TestMsg::GetCount(tx)).unwrap();
        let count = rx.recv_timeout(Duration::from_secs(1)).unwrap();

        // Assert - only the non-expired message was processed
        assert_eq!(count, 1);

        actor_ref.send(TestMsg::Stop).unwrap();
    }

    #[test]
    fn should_enable_actor_to_actor_messaging() {
        // Arrange
        let scheduler = Scheduler::new(2);
        scheduler.start();

        // Create two actors
        let actor1 = CounterActor { count: 0 };
        let actor2 = CounterActor { count: 0 };
        let addr1 = test_address(1, "/test/actor1");
        let addr2 = test_address(1, "/test/actor2");
        let ref1 = scheduler.spawn(actor1, addr1.clone(), 10);
        let ref2 = scheduler.spawn(actor2, addr2, 10);

        // Create a ping-pong actor that sends to another actor
        struct PingActor {
            target: RouteAddress,
            pings_sent: usize,
        }
        impl Actor for PingActor {
            type Message = String;
            fn receive(&mut self, msg: String, ctx: &mut Context<Self>) {
                if msg == "start" {
                    // Send a message to the target actor
                    ctx.send(self.target.clone(), TestMsg::Increment).ok();
                    self.pings_sent += 1;
                    ctx.stop();
                }
            }
        }

        let ping_actor = PingActor {
            target: addr1.clone(),
            pings_sent: 0,
        };
        let ping_ref = scheduler.spawn(ping_actor, test_address(1, "/test/ping"), 10);

        // Act - trigger the ping
        ping_ref.send("start".to_string()).unwrap();
        thread::sleep(Duration::from_millis(100));

        // Check that actor1 received the increment
        let (tx, rx) = crossbeam_channel::bounded(1);
        ref1.send(TestMsg::GetCount(tx)).unwrap();
        let count = rx.recv_timeout(Duration::from_secs(1)).unwrap();

        // Assert
        assert_eq!(count, 1);

        ref1.send(TestMsg::Stop).unwrap();
        ref2.send(TestMsg::Stop).unwrap();
    }

    #[test]
    fn should_support_reply_pattern() {
        // Arrange
        let scheduler = Scheduler::new(2);
        scheduler.start();

        // Create a request-response actor pair
        struct RequestActor {
            response_received: Arc<parking_lot::Mutex<Option<String>>>,
        }
        impl Actor for RequestActor {
            type Message = String;
            fn receive(&mut self, msg: String, _ctx: &mut Context<Self>) {
                *self.response_received.lock() = Some(msg);
            }
        }

        struct ResponseActor;
        impl Actor for ResponseActor {
            type Message = String;
            fn receive(&mut self, msg: String, ctx: &mut Context<Self>) {
                if msg == "hello" {
                    // Reply to the sender
                    ctx.reply("world".to_string()).ok();
                }
            }
        }

        let response_received = Arc::new(parking_lot::Mutex::new(None));
        let request_actor = RequestActor {
            response_received: response_received.clone(),
        };
        let response_actor = ResponseActor;

        let request_addr = test_address(1, "/test/request");
        let response_addr = test_address(1, "/test/response");
        let _request_ref = scheduler.spawn(request_actor, request_addr.clone(), 10);
        let _response_ref = scheduler.spawn(response_actor, response_addr.clone(), 10);

        // Act - send request from _request_ref to _response_ref
        // We need to manually create an envelope with source set
        let request_envelope =
            Envelope::from_route(request_addr, response_addr, "hello".to_string());
        scheduler.router().route(request_envelope).unwrap();

        // Wait for reply
        thread::sleep(Duration::from_millis(100));

        // Assert
        let response = response_received.lock().clone();
        assert_eq!(response, Some("world".to_string()));
    }
}
