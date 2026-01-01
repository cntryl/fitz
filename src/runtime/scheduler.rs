//! Actor scheduling and execution coordination

use super::actor::{Actor, ActorError, ActorRef, Context};
use super::mailbox::Mailbox;
use crate::transport::envelope::Envelope;
use crate::transport::router::Router;
use crate::transport::routing::RouteAddress;
use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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

        // Register mailbox with router
        self.router
            .register(address.clone(), Arc::new(mailbox.clone()));

        let receiver = mailbox.receiver().clone();
        let router_clone = self.router.clone();

        // Spawn actor execution thread
        thread::spawn(move || {
            let mut ctx = Context::new(address.clone(), router_clone);

            // Call started hook
            actor.started(&mut ctx);

            // Process messages
            while ctx.is_running() {
                match receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(envelope) => {
                        // Check deadline before processing
                        if envelope.is_expired() {
                            eprintln!(
                                "Dropped expired message {:?} for actor {:?}",
                                envelope.id(),
                                address
                            );
                            continue;
                        }

                        // Extract typed message from envelope
                        // We need to clone the envelope for context, then extract payload
                        let envelope_id = envelope.id();
                        let envelope_source = envelope.source().cloned();
                        let envelope_causation = envelope.causation();
                        let envelope_deadline = envelope.deadline();

                        let msg = match envelope.into_payload::<A::Message>() {
                            Some(m) => m,
                            None => {
                                // Type mismatch - log and skip
                                eprintln!(
                                    "Type mismatch: envelope {:?} for actor {:?} contains wrong message type",
                                    envelope_id,
                                    address
                                );
                                continue;
                            }
                        };

                        // Reconstruct envelope for context (without payload)
                        // Create a dummy envelope with the same metadata
                        let ctx_envelope = if let Some(src) = envelope_source {
                            let mut env = Envelope::from_route(src, address.clone(), ());
                            if let Some(causation) = envelope_causation {
                                env = env.with_causation(causation);
                            }
                            if let Some(deadline) = envelope_deadline {
                                env = env.with_deadline(deadline);
                            }
                            env
                        } else {
                            let mut env = Envelope::new(address.clone(), ());
                            if let Some(causation) = envelope_causation {
                                env = env.with_causation(causation);
                            }
                            if let Some(deadline) = envelope_deadline {
                                env = env.with_deadline(deadline);
                            }
                            env
                        };

                        // Set current envelope for causation tracking
                        ctx.set_current_envelope(ctx_envelope);

                        // Process message with panic recovery
                        if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || {
                                actor.receive(msg, &mut ctx);
                            },
                        )) {
                            let error = ActorError::Panic(format!("{:?}", e));
                            actor.on_error(error, &mut ctx);
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        // Check if we should stop
                        continue;
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        // Mailbox closed, stop actor
                        break;
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::envelope::Envelope;
    use crate::transport::routing::{Route, RouteFamily};

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
        let expired_envelope = Envelope::new(address, TestMsg::Increment)
            .with_deadline(past_deadline);
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
            response_received: Arc<std::sync::Mutex<Option<String>>>,
        }
        impl Actor for RequestActor {
            type Message = String;
            fn receive(&mut self, msg: String, _ctx: &mut Context<Self>) {
                *self.response_received.lock().unwrap() = Some(msg);
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

        let response_received = Arc::new(std::sync::Mutex::new(None));
        let request_actor = RequestActor {
            response_received: response_received.clone(),
        };
        let response_actor = ResponseActor;

        let request_addr = test_address(1, "/test/request");
        let response_addr = test_address(1, "/test/response");
        let request_ref = scheduler.spawn(request_actor, request_addr.clone(), 10);
        let response_ref = scheduler.spawn(response_actor, response_addr.clone(), 10);

        // Act - send request from request_ref to response_ref
        // We need to manually create an envelope with source set
        let request_envelope = Envelope::from_route(
            request_addr,
            response_addr,
            "hello".to_string(),
        );
        scheduler.router().route(request_envelope).unwrap();

        // Wait for reply
        thread::sleep(Duration::from_millis(100));

        // Assert
        let response = response_received.lock().unwrap().clone();
        assert_eq!(response, Some("world".to_string()));
    }
}
