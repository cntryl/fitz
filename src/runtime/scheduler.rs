//! Actor scheduling and execution coordination

use super::actor::{Actor, ActorError, ActorId, ActorRef, Context};
use super::mailbox::Mailbox;
use crate::transport::router::Router;
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Actor system scheduler that manages actor lifecycles and message processing
pub struct Scheduler {
    router: Arc<Router>,
    next_actor_id: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    #[allow(dead_code)]
    worker_threads: usize,
}

impl Scheduler {
    /// Create a new scheduler with the specified number of worker threads
    pub fn new(worker_threads: usize) -> Self {
        Self {
            router: Arc::new(Router::new()),
            next_actor_id: Arc::new(AtomicU64::new(1)),
            running: Arc::new(AtomicBool::new(false)),
            worker_threads: worker_threads.max(1),
        }
    }

    /// Create a scheduler with a shared router
    pub fn with_router(router: Arc<Router>, worker_threads: usize) -> Self {
        Self {
            router,
            next_actor_id: Arc::new(AtomicU64::new(1)),
            running: Arc::new(AtomicBool::new(false)),
            worker_threads: worker_threads.max(1),
        }
    }

    /// Get a reference to the router
    pub fn router(&self) -> Arc<Router> {
        self.router.clone()
    }

    /// Generate a new unique actor ID
    fn next_actor_id(&self) -> ActorId {
        let id = self.next_actor_id.fetch_add(1, Ordering::SeqCst);
        ActorId::new(id)
    }

    /// Spawn a new actor and return its reference
    pub fn spawn<A>(&self, mut actor: A, mailbox_capacity: usize) -> ActorRef<A::Message>
    where
        A: Actor,
        A::Message: Any + Send + Sync + 'static,
    {
        let actor_id = self.next_actor_id();
        let mailbox = Mailbox::new(mailbox_capacity);
        let actor_ref = ActorRef::new(actor_id, self.router.clone());

        // Register mailbox with router
        self.router.register(actor_id, Arc::new(mailbox.clone()));

        let receiver = mailbox.receiver().clone();

        // Spawn actor execution thread
        thread::spawn(move || {
            let mut ctx = Context::new(actor_id);

            // Call started hook
            actor.started(&mut ctx);

            // Process messages
            while ctx.is_running() {
                match receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(envelope) => {
                        // Unwrap envelope to extract typed message
                        let msg = match envelope.into_payload::<A::Message>() {
                            Some(m) => m,
                            None => {
                                // Type mismatch - log and skip
                                eprintln!(
                                    "Type mismatch: envelope for {:?} contains wrong message type",
                                    actor_id
                                );
                                continue;
                            }
                        };

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
        let ref1 = scheduler.spawn(actor1, 10);
        let ref2 = scheduler.spawn(actor2, 10);

        // Assert
        assert_ne!(ref1.actor_id(), ref2.actor_id());
    }

    #[test]
    fn should_process_messages_in_sequence() {
        // Arrange
        let scheduler = Scheduler::new(1);
        scheduler.start();
        let actor = CounterActor { count: 0 };
        let actor_ref = scheduler.spawn(actor, 10);

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
}
