// LAYER: RUNTIME
//! Actor scheduling and execution coordination

use super::actor::{Actor, ActorError, ActorMetrics, ActorRef, Context};
use super::actor_lifecycle::{actor_error_from_panic, notify_actor_error, start_actor};
use super::mailbox::Mailbox;
use crate::observability as obs;
use crate::runtime::router::{MailboxSink, Router};
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

fn start_scheduled_actor<A: Actor>(actor: &mut A, ctx: &mut Context<A>, address: &RouteAddress) {
    if !start_actor(actor, ctx, address) {
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
            let actor_error = actor_error_from_panic(error.as_ref());
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
        let mailbox_sink: Arc<dyn MailboxSink> = Arc::new(mailbox.clone());
        self.router.register(address.clone(), mailbox_sink.clone());

        let receiver = mailbox.receiver().clone();
        let high_receiver = mailbox.high_priority_receiver().clone();
        let router_clone = self.router.clone();
        let metrics_clone = metrics.clone();

        // Spawn actor execution thread
        thread::spawn(move || {
            let mut ctx =
                Context::with_metrics(address.clone(), router_clone.clone(), metrics_clone);

            start_scheduled_actor(&mut actor, &mut ctx, &address);

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

            router_clone.unregister_sink(&address, &mailbox_sink);
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
#[path = "scheduler/tests.rs"]
mod tests;
