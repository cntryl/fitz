// LAYER: RUNTIME
//! Actor scheduling and execution coordination

use super::actor::{Actor, ActorError, ActorMetrics, ActorRef, Context};
use super::mailbox::Mailbox;
use crate::observability as obs;
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
const MIN_POLL_TIMEOUT_MS: u64 = 1;

/// Maximum timeout for mailbox polling (when mailbox is empty)
const MAX_POLL_TIMEOUT_MS: u64 = 1;

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn usize_to_f64_saturating(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn record_duration_counter(name: &str, duration: Duration) {
    let duration_us = u128_to_u64_saturating(duration.as_micros().min(u128::from(u64::MAX)));
    if duration_us == 0 {
        return;
    }

    crate::observability::counter_add(name, duration_us);
}

fn record_worker_busy_time(duration: Duration) {
    record_duration_counter(obs::METRIC_WORKER_BUSY_TIME, duration);
}

fn record_worker_idle_time(duration: Duration) {
    record_duration_counter(obs::METRIC_WORKER_IDLE_TIME, duration);
}

fn record_mailbox_observability(mailbox: &Mailbox, envelope: &crate::runtime::envelope::Envelope) {
    if let Some(queued_at) = envelope.queued_at() {
        crate::observability::histogram_observe_us(
            obs::METRIC_QUEUE_WAIT_LATENCY,
            u128_to_u64_saturating(
                Instant::now()
                    .saturating_duration_since(queued_at)
                    .as_micros(),
            ),
        );
    }

    crate::observability::gauge_set(
        obs::METRIC_MAILBOX_DEPTH,
        mailbox.len().saturating_add(mailbox.high_priority_len()) as u64,
    );
}

fn poll_timeout_ms(mailbox: &Mailbox) -> u64 {
    let occupancy =
        usize_to_f64_saturating(mailbox.len()) / usize_to_f64_saturating(mailbox.capacity());
    if occupancy > 0.5 {
        MIN_POLL_TIMEOUT_MS
    } else {
        MAX_POLL_TIMEOUT_MS
    }
}

fn normal_message_budget(processed_high: usize) -> usize {
    if processed_high == 0 {
        MAX_HIGH_PER_TICK + MAX_NORMAL_PER_TICK
    } else {
        MAX_NORMAL_PER_TICK
    }
}

fn notify_actor_error<A: Actor>(
    actor: &mut A,
    error: ActorError,
    ctx: &mut Context<A>,
    address: &RouteAddress,
) {
    if let Err(error_hook_panic) =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.on_error(error, ctx)))
    {
        tracing::error!(
            actor = ?address,
            error = ?error_hook_panic,
            "Actor panicked while handling actor error"
        );
    }
}

fn start_actor<A: Actor>(actor: &mut A, ctx: &mut Context<A>, address: &RouteAddress) {
    if let Err(error) =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.started(ctx)))
    {
        tracing::error!(actor = ?address, error = ?error, "Actor panicked during startup");
        ctx.metrics().record_panic();
        notify_actor_error(
            actor,
            ActorError::Panic(format!("startup panic: {error:?}")),
            ctx,
            address,
        );
        ctx.stop();
    }
}

fn handle_fired_timers<A: Actor>(actor: &mut A, ctx: &mut Context<A>, address: &RouteAddress) {
    let fired_timers = ctx.timer_manager().fired_timers();
    for timer_id in fired_timers {
        let timer_start = Instant::now();
        if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            actor.on_timer(timer_id, ctx);
        })) {
            record_worker_busy_time(timer_start.elapsed());
            tracing::error!(actor = ?address, error = ?error, "Actor panicked during timer handling");

            ctx.metrics().record_panic();
            let actor_error = ActorError::Panic(format!("timer panic: {error:?}"));
            notify_actor_error(actor, actor_error, ctx, address);
            ctx.stop();
            break;
        }

        let elapsed = timer_start.elapsed();
        record_worker_busy_time(elapsed);
        ctx.metrics()
            .record_processed(u128_to_u64_saturating(elapsed.as_micros()));
        if !ctx.is_running() {
            break;
        }
    }
}

/// Actor system scheduler that manages actor lifecycles and message processing
pub struct Scheduler {
    router: Arc<Router>,
    running: Arc<AtomicBool>,
}

impl Scheduler {
    /// Create a new scheduler.
    ///
    /// Each spawned actor gets its own dedicated processing thread, so the
    /// scheduler holds no worker-thread pool of its own.
    #[must_use]
    pub fn new() -> Self {
        Self {
            router: Arc::new(Router::new()),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get a reference to the router
    #[must_use]
    pub fn router(&self) -> Arc<Router> {
        self.router.clone()
    }

    /// Spawn a new actor and return its reference
    ///
    /// The caller must provide a `RouteAddress` for the actor. The scheduler will:
    /// - Register the actor's mailbox with the router at the given address
    /// - Start a dedicated thread for message processing
    /// - Return an `ActorRef` for sending messages to the actor
    ///
    /// # Cost and variance
    ///
    /// Full spawn cost includes mailbox creation, router registration, and `thread::spawn`.
    /// Tier 2 `subsystem_scheduler` benchmarks measure this full cost; variance (`rel_stddev`
    /// ~0.10–0.15) is expected from OS thread scheduling. Use the `register_only` bench
    /// to isolate registration cost from thread creation.
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
            let mut ctx =
                Context::with_metrics(address.clone(), router_clone.clone(), metrics_clone);

            start_actor(&mut actor, &mut ctx, &address);

            // Process messages with two-phase priority lanes
            while ctx.is_running() {
                let timeout_ms = poll_timeout_ms(&mailbox);

                // Track tick start for time budget enforcement
                let tick_start = Instant::now();
                let mut processed_high = 0;
                let mut processed_normal = 0;

                // PHASE 1: High-priority messages (capped at MAX_HIGH_PER_TICK)
                while ctx.is_running() && processed_high < MAX_HIGH_PER_TICK {
                    // INVARIANT: Time budget check to prevent thread monopolization
                    if u128_to_u64_saturating(tick_start.elapsed().as_millis())
                        >= MAX_TICK_DURATION_MS
                    {
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

                    record_mailbox_observability(&mailbox, &envelope);

                    let busy_start = Instant::now();

                    // Check deadline before processing
                    if envelope.is_expired() {
                        ctx.metrics().record_expired();
                        tracing::warn!(
                            message_id = envelope.id().as_u64(),
                            actor = ?address,
                            priority = "high",
                            "Dropped expired message"
                        );
                        record_worker_busy_time(busy_start.elapsed());
                        processed_high += 1;
                        continue;
                    }

                    // Process high-priority message
                    process_envelope(envelope, &mut actor, &mut ctx, &address, busy_start);
                    record_worker_busy_time(busy_start.elapsed());
                    processed_high += 1;
                }

                // PHASE 2: Normal-priority messages (remaining budget)
                // If high lane was idle, use full budget (16), otherwise use 12
                let normal_budget = normal_message_budget(processed_high);

                while ctx.is_running() && processed_normal < normal_budget {
                    // INVARIANT: Time budget check to prevent thread monopolization
                    if u128_to_u64_saturating(tick_start.elapsed().as_millis())
                        >= MAX_TICK_DURATION_MS
                    {
                        break;
                    }

                    let envelope = if processed_high == 0 && processed_normal == 0 {
                        // First message overall: use blocking receive with timeout
                        let idle_start = Instant::now();
                        let received_envelope =
                            receiver.recv_timeout(Duration::from_millis(timeout_ms));
                        record_worker_idle_time(idle_start.elapsed());

                        match received_envelope {
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

                    record_mailbox_observability(&mailbox, &envelope);

                    let busy_start = Instant::now();

                    // Check deadline before processing
                    if envelope.is_expired() {
                        ctx.metrics().record_expired();
                        tracing::warn!(
                            message_id = envelope.id().as_u64(),
                            actor = ?address,
                            priority = "normal",
                            "Dropped expired message"
                        );
                        record_worker_busy_time(busy_start.elapsed());
                        processed_normal += 1;
                        continue;
                    }

                    // Process normal-priority message
                    process_envelope(envelope, &mut actor, &mut ctx, &address, busy_start);
                    record_worker_busy_time(busy_start.elapsed());
                    processed_normal += 1;
                }

                handle_fired_timers(&mut actor, &mut ctx, &address);
            }

            router_clone.unregister(&address);
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
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
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

    let Some(msg) = msg else {
        ctx.metrics().record_type_mismatch();

        let error = ActorError::TypeMismatch {
            expected: std::any::type_name::<A::Message>().to_string(),
            envelope_id: metadata.id.as_u64(),
        };

        tracing::warn!(
            message_id = metadata.id.as_u64(),
            actor = ?address,
            expected = std::any::type_name::<A::Message>(),
            "Actor received message with mismatched type"
        );

        notify_actor_error(actor, error, ctx, address);
        return;
    };

    // Set current metadata for causation tracking (no allocation)
    ctx.set_current_metadata(metadata);

    // Process message with panic recovery
    // INVARIANT: Panic => Stop. Supervisor restarts if configured.
    if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        actor.receive(msg, ctx);
    })) {
        // Structured panic error
        tracing::error!(
            actor = ?address,
            error = ?e,
            "Actor panicked during message processing"
        );

        ctx.metrics().record_panic();
        let error = ActorError::Panic(format!("{e:?}"));

        // Call error handler but actor is now stopped
        notify_actor_error(actor, error, ctx, address);

        // CRITICAL: Stop actor immediately. No further message processing.
        ctx.stop();
    } else {
        // Record successful processing
        let elapsed = u128_to_u64_saturating(start.elapsed().as_micros());
        ctx.metrics().record_processed(elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability as obs;
    use crate::runtime::envelope::Envelope;
    use crate::runtime::routing::{Route, RouteFamily};

    fn test_address(family: u64, route: &str) -> RouteAddress {
        RouteAddress::new(
            RouteFamily::try_from(family).expect("test family must fit in u32"),
            Route::new(route),
        )
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

    struct StopReportingActor {
        stopped: crossbeam_channel::Sender<()>,
    }

    struct BatchStopActor {
        count: u32,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
        stopped: crossbeam_channel::Sender<u32>,
    }

    struct PanickingErrorActor {
        error_entered: crossbeam_channel::Sender<()>,
    }

    struct PanickingStartedActor {
        started_entered: crossbeam_channel::Sender<()>,
    }

    impl Actor for PanickingStartedActor {
        type Message = ();

        fn started(&mut self, _ctx: &mut Context<Self>) {
            let _ = self.started_entered.send(());
            panic!("started hook panic");
        }

        fn receive(&mut self, (): (), _ctx: &mut Context<Self>) {}
    }

    impl Actor for PanickingErrorActor {
        type Message = ();

        fn receive(&mut self, (): (), _ctx: &mut Context<Self>) {
            panic!("receive panic");
        }

        fn on_error(&mut self, _error: ActorError, _ctx: &mut Context<Self>) {
            let _ = self.error_entered.send(());
            panic!("error hook panic");
        }
    }

    impl Actor for BatchStopActor {
        type Message = TestMsg;

        fn started(&mut self, _ctx: &mut Context<Self>) {
            let _ = self.entered.send(());
            let _ = self.release.recv();
        }

        fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
            match msg {
                TestMsg::Increment => self.count = self.count.saturating_add(1),
                TestMsg::Stop => ctx.stop(),
                TestMsg::GetCount(_) => {}
            }
        }

        fn stopped(&mut self) {
            let _ = self.stopped.send(self.count);
        }
    }

    impl Actor for StopReportingActor {
        type Message = TestMsg;

        fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
            if matches!(msg, TestMsg::Stop) {
                ctx.stop();
            }
        }

        fn stopped(&mut self) {
            let _ = self.stopped.send(());
        }
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
    fn should_create_scheduler_in_stopped_state() {
        // Arrange

        // Act
        let scheduler = Scheduler::new();

        // Assert
        assert!(!scheduler.is_running());
    }

    #[test]
    fn should_unregister_actor_route_after_actor_stops() {
        // Arrange
        let scheduler = Scheduler::new();
        let (stopped_tx, stopped_rx) = crossbeam_channel::bounded(1);
        let actor_ref = scheduler.spawn(
            StopReportingActor {
                stopped: stopped_tx,
            },
            test_address(1, "test://stopping"),
            8,
        );

        // Act
        actor_ref.send(TestMsg::Stop).expect("stop should enqueue");
        stopped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor should stop");
        let send_after_stop = actor_ref.send(TestMsg::Increment);

        // Assert
        assert!(send_after_stop.is_err());
    }

    #[test]
    fn should_stop_processing_batch_after_actor_stops() {
        // Arrange
        let scheduler = Scheduler::new();
        let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let (stopped_tx, stopped_rx) = crossbeam_channel::bounded(1);
        let actor_ref = scheduler.spawn(
            BatchStopActor {
                count: 0,
                entered: entered_tx,
                release: release_rx,
                stopped: stopped_tx,
            },
            test_address(1, "test://batch-stop"),
            8,
        );
        entered_rx.recv().expect("actor should start");
        actor_ref.send(TestMsg::Stop).expect("stop should enqueue");
        actor_ref
            .send(TestMsg::Increment)
            .expect("increment should enqueue behind stop");

        // Act
        release_tx.send(()).expect("actor should start processing");
        let count = stopped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor should stop");

        // Assert
        assert_eq!(count, 0);
    }

    #[test]
    fn should_unregister_actor_when_error_hook_panics() {
        // Arrange
        let scheduler = Scheduler::new();
        let (error_entered_tx, error_entered_rx) = crossbeam_channel::bounded(1);
        let actor_ref = scheduler.spawn(
            PanickingErrorActor {
                error_entered: error_entered_tx,
            },
            test_address(1, "test://panicking-error-hook"),
            8,
        );

        // Act
        actor_ref.send(()).expect("panic message should enqueue");
        error_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("error hook should run");
        let deadline = Instant::now() + Duration::from_secs(1);
        let send_after_panic = loop {
            let result = actor_ref.send(());
            if matches!(result, Err(crate::runtime::SendError::RouteNotFound { .. }))
                || Instant::now() >= deadline
            {
                break result;
            }
            std::thread::yield_now();
        };

        // Assert
        assert!(matches!(
            send_after_panic,
            Err(crate::runtime::SendError::RouteNotFound { .. })
        ));
    }

    #[test]
    fn should_unregister_actor_when_started_hook_panics() {
        // Arrange
        let scheduler = Scheduler::new();
        let (started_entered_tx, started_entered_rx) = crossbeam_channel::bounded(1);
        let actor_ref = scheduler.spawn(
            PanickingStartedActor {
                started_entered: started_entered_tx,
            },
            test_address(1, "test://panicking-started-hook"),
            8,
        );
        started_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("started hook should run");

        // Act
        let deadline = Instant::now() + Duration::from_secs(1);
        let send_after_panic = loop {
            let result = actor_ref.send(());
            if matches!(result, Err(crate::runtime::SendError::RouteNotFound { .. }))
                || Instant::now() >= deadline
            {
                break result;
            }
            std::thread::yield_now();
        };

        // Assert
        assert!(matches!(
            send_after_panic,
            Err(crate::runtime::SendError::RouteNotFound { .. })
        ));
    }

    #[test]
    fn should_start_scheduler() {
        // Arrange
        let scheduler = Scheduler::new();

        // Act
        scheduler.start();

        // Assert
        assert!(scheduler.is_running());
    }

    #[test]
    fn should_stop_scheduler() {
        // Arrange
        let scheduler = Scheduler::new();
        scheduler.start();

        // Act
        scheduler.stop();

        // Assert
        assert!(!scheduler.is_running());
    }

    #[test]
    fn should_generate_unique_actor_ids() {
        // Arrange
        let scheduler = Scheduler::new();
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
        let scheduler = Scheduler::new();
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
        let scheduler = Scheduler::new();
        scheduler.start();
        let actor = CounterActor { count: 0 };
        let address = test_address(1, "/test/counter");
        let actor_ref = scheduler.spawn(actor, address.clone(), 10);

        // Send a message with an already-expired deadline
        let past_deadline = std::time::Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("past deadline should be representable");
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
    #[allow(clippy::items_after_statements)]
    fn should_enable_actor_to_actor_messaging() {
        // Arrange
        let scheduler = Scheduler::new();
        scheduler.start();
        let (incremented_tx, incremented_rx) = crossbeam_channel::bounded(1);

        // Create two actors
        struct NotifyingCounterActor {
            count: u32,
            incremented: crossbeam_channel::Sender<()>,
        }
        impl Actor for NotifyingCounterActor {
            type Message = TestMsg;

            fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
                match msg {
                    TestMsg::Increment => {
                        self.count += 1;
                        let _ = self.incremented.send(());
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

        let actor1 = NotifyingCounterActor {
            count: 0,
            incremented: incremented_tx,
        };
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
        incremented_rx.recv_timeout(Duration::from_secs(1)).unwrap();

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
    #[allow(clippy::items_after_statements)]
    fn should_support_reply_pattern() {
        // Arrange
        let scheduler = Scheduler::new();
        scheduler.start();
        let (response_tx, response_rx) = crossbeam_channel::bounded(1);

        // Create a request-response actor pair
        struct RequestActor {
            response_received: Arc<parking_lot::Mutex<Option<String>>>,
            response_tx: crossbeam_channel::Sender<String>,
        }
        impl Actor for RequestActor {
            type Message = String;
            fn receive(&mut self, msg: String, ctx: &mut Context<Self>) {
                *self.response_received.lock() = Some(msg.clone());
                let _ = self.response_tx.send(msg);
                ctx.stop();
            }
        }

        struct ResponseActor;
        impl Actor for ResponseActor {
            type Message = String;
            fn receive(&mut self, msg: String, ctx: &mut Context<Self>) {
                if msg == "hello" {
                    // Reply to the sender
                    ctx.reply("world".to_string()).ok();
                    ctx.stop();
                }
            }
        }

        let response_received = Arc::new(parking_lot::Mutex::new(None));
        let request_actor = RequestActor {
            response_received: response_received.clone(),
            response_tx,
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
        let response = response_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        // Assert
        assert_eq!(response, "world");
        assert_eq!(response_received.lock().clone(), Some("world".to_string()));
    }

    #[test]
    fn should_process_high_priority_messages_before_normal_messages_given_both_lanes_ready() {
        // Arrange
        #[derive(Debug)]
        enum PriorityMsg {
            High,
            Normal,
        }

        struct PriorityActor {
            order: Arc<parking_lot::Mutex<Vec<&'static str>>>,
            started_tx: crossbeam_channel::Sender<()>,
            release_rx: crossbeam_channel::Receiver<()>,
            done_tx: crossbeam_channel::Sender<()>,
        }

        impl Actor for PriorityActor {
            type Message = PriorityMsg;

            fn started(&mut self, _ctx: &mut Context<Self>) {
                self.started_tx.send(()).unwrap();
                self.release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap();
            }

            fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
                let label = match msg {
                    PriorityMsg::High => "high",
                    PriorityMsg::Normal => "normal",
                };

                let mut order = self.order.lock();
                order.push(label);
                if order.len() == 2 {
                    let _ = self.done_tx.send(());
                    ctx.stop();
                }
            }
        }

        let scheduler = Scheduler::new();
        scheduler.start();
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (started_tx, started_rx) = crossbeam_channel::bounded(1);
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let address = test_address(1, "/test/high-priority");

        let _actor_ref = scheduler.spawn(
            PriorityActor {
                order: order.clone(),
                started_tx,
                release_rx,
                done_tx,
            },
            address.clone(),
            10,
        );

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor to start before enqueueing messages");

        // Act
        scheduler
            .router()
            .route(Envelope::new(address.clone(), PriorityMsg::Normal))
            .unwrap();
        scheduler
            .router()
            .route_high_priority(Envelope::new(address.clone(), PriorityMsg::High))
            .unwrap();
        release_tx.send(()).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor to process both priority lanes");

        // Assert
        assert_eq!(order.lock().as_slice(), ["high", "normal"]);
    }

    #[test]
    fn should_accumulate_duration_counter_in_microseconds() {
        // Arrange
        let metric_name = "test_scheduler_duration_counter_us_total";
        let metrics = crate::observability::metrics();
        let before = metrics.counter_get(metric_name);

        // Act
        record_duration_counter(metric_name, Duration::from_micros(250));

        // Assert
        assert_eq!(metrics.counter_get(metric_name), before + 250);
    }

    #[test]
    fn should_record_worker_busy_time_when_processing_messages() {
        // Arrange
        let metrics = crate::observability::metrics();
        let before = metrics.counter_get(obs::METRIC_WORKER_BUSY_TIME);
        let scheduler = Scheduler::new();
        scheduler.start();
        let actor = CounterActor { count: 0 };
        let actor_ref = scheduler.spawn(actor, test_address(1, "/test/busy-counter"), 10);

        // Act
        actor_ref.send(TestMsg::Increment).unwrap();
        let (tx, rx) = crossbeam_channel::bounded(1);
        actor_ref.send(TestMsg::GetCount(tx)).unwrap();
        let count = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut after = before;
        while after == before && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
            after = metrics.counter_get(obs::METRIC_WORKER_BUSY_TIME);
        }
        actor_ref.send(TestMsg::Stop).unwrap();

        // Assert
        assert_eq!(count, 1);
        assert!(after > before);
    }

    #[test]
    fn should_record_worker_idle_time_while_waiting_for_messages() {
        // Arrange
        let metrics = crate::observability::metrics();
        let before = metrics.counter_get(obs::METRIC_WORKER_IDLE_TIME);
        let scheduler = Scheduler::new();
        scheduler.start();
        let actor = CounterActor { count: 0 };
        let actor_ref = scheduler.spawn(actor, test_address(1, "/test/idle-counter"), 10);

        // Act
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut after = before;
        while after == before && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
            after = metrics.counter_get(obs::METRIC_WORKER_IDLE_TIME);
        }
        actor_ref.send(TestMsg::Stop).unwrap();

        // Assert
        assert!(after > before);
    }
}
