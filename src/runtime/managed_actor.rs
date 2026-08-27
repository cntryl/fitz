// LAYER: RUNTIME
//! Managed actor lifecycle for production domain actors.

use crate::observability as obs;
use crate::runtime::actor::{Actor, ActorError, ActorMetrics, ActorRef, Context};
use crate::runtime::envelope::Envelope;
use crate::runtime::mailbox::Mailbox;
use crate::runtime::router::{DeliveryError, MailboxSink, Router};
use crate::runtime::routing::RouteAddress;
use crate::runtime::supervision::{RestartTracker, SupervisionAction, SupervisorStrategy};
use parking_lot::Mutex;
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const MANAGED_ACTOR_POLL_TIMEOUT: Duration = Duration::from_millis(1);

#[derive(Debug, Default)]
struct PriorityDrainState {
    consecutive_high: usize,
    normal_drain_remaining: usize,
}

fn route_error_to_delivery_error(error: crate::runtime::router::RouteError) -> DeliveryError {
    match error {
        crate::runtime::router::RouteError::RouteNotFound(_) => DeliveryError::ActorStopped,
        crate::runtime::router::RouteError::DeliveryFailed(_, delivery_error) => delivery_error,
    }
}

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn record_mailbox_observability(mailbox: &Mailbox, envelope: &Envelope) {
    if !obs::hot_path_metrics_enabled() {
        return;
    }

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

fn receive_next_envelope(
    mailbox: &Mailbox,
    normal_receiver: &crossbeam_channel::Receiver<Envelope>,
    high_receiver: &crossbeam_channel::Receiver<Envelope>,
    priority_state: &mut PriorityDrainState,
) -> Option<Envelope> {
    if priority_state.normal_drain_remaining > 0 {
        match normal_receiver.try_recv() {
            Ok(envelope) => {
                priority_state.normal_drain_remaining -= 1;
                record_mailbox_observability(mailbox, &envelope);
                return Some(envelope);
            }
            Err(
                crossbeam_channel::TryRecvError::Empty
                | crossbeam_channel::TryRecvError::Disconnected,
            ) => {
                priority_state.normal_drain_remaining = 0;
            }
        }
    }

    crossbeam_channel::select_biased! {
        recv(high_receiver) -> message => match message {
            Ok(envelope) => {
                priority_state.consecutive_high += 1;
                if priority_state.consecutive_high >= mailbox.capacity() {
                    priority_state.normal_drain_remaining = normal_receiver.len();
                    priority_state.consecutive_high = 0;
                }
                record_mailbox_observability(mailbox, &envelope);
                Some(envelope)
            }
            Err(_) => None,
        },
        recv(normal_receiver) -> message => match message {
            Ok(envelope) => {
                priority_state.consecutive_high = 0;
                record_mailbox_observability(mailbox, &envelope);
                Some(envelope)
            }
            Err(_) => None,
        },
        default(MANAGED_ACTOR_POLL_TIMEOUT) => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorStep {
    Continue,
    Panicked,
}

#[derive(Debug, Clone, Copy)]
pub struct ManagedActorHealthSnapshot {
    pub running: bool,
    pub restart_count: u64,
    pub panic_count: u64,
    pub restart_exhausted: bool,
}

#[derive(Debug, Default)]
struct ManagedActorHealth {
    restart_count: AtomicU64,
    panic_count: AtomicU64,
    restart_exhausted: AtomicBool,
}

impl ManagedActorHealth {
    fn record_panic(&self) {
        self.panic_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_restart(&self) {
        self.restart_count.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_exhausted(&self) {
        self.restart_exhausted.store(true, Ordering::SeqCst);
    }

    fn snapshot(&self, running: bool) -> ManagedActorHealthSnapshot {
        ManagedActorHealthSnapshot {
            running,
            restart_count: self.restart_count.load(Ordering::Relaxed),
            panic_count: self.panic_count.load(Ordering::Relaxed),
            restart_exhausted: self.restart_exhausted.load(Ordering::SeqCst),
        }
    }
}

fn actor_error_from_panic(error: &(dyn Any + Send)) -> ActorError {
    if let Some(message) = error.downcast_ref::<&'static str>() {
        ActorError::Panic((*message).to_string())
    } else if let Some(message) = error.downcast_ref::<String>() {
        ActorError::Panic(message.clone())
    } else {
        ActorError::Panic("non-string panic payload".to_string())
    }
}

fn notify_actor_error<A: Actor>(
    actor: &mut A,
    actor_error: ActorError,
    ctx: &mut Context<A>,
    address: &RouteAddress,
) {
    if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        actor.on_error(actor_error, ctx);
    })) {
        tracing::error!(
            actor = ?address,
            error = ?error,
            "Managed actor panicked while handling actor error"
        );
    }
}

fn start_managed_actor<A: Actor>(
    actor: &mut A,
    ctx: &mut Context<A>,
    address: &RouteAddress,
) -> ActorStep {
    if let Err(error) =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.started(ctx)))
    {
        tracing::error!(actor = ?address, error = ?error, "Managed actor panicked during startup");
        ctx.metrics().record_panic();
        notify_actor_error(actor, actor_error_from_panic(error.as_ref()), ctx, address);
        ActorStep::Panicked
    } else {
        ActorStep::Continue
    }
}

fn process_envelope<A: Actor>(
    envelope: Envelope,
    actor: &mut A,
    ctx: &mut Context<A>,
    address: &RouteAddress,
    started_at: Instant,
) -> ActorStep
where
    A::Message: Any + Send + Sync + 'static,
{
    let (metadata, msg) = envelope.into_parts::<A::Message>();

    let Some(msg) = msg else {
        ctx.metrics().record_type_mismatch();
        notify_actor_error(
            actor,
            ActorError::TypeMismatch {
                expected: std::any::type_name::<A::Message>().to_string(),
                envelope_id: metadata.id.as_u64(),
            },
            ctx,
            address,
        );
        return ActorStep::Continue;
    };

    ctx.set_current_metadata(metadata);

    if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        actor.receive(msg, ctx);
    })) {
        tracing::error!(
            actor = ?address,
            error = ?error,
            "Managed actor panicked during message processing"
        );
        ctx.metrics().record_panic();
        let actor_error = actor_error_from_panic(error.as_ref());
        notify_actor_error(actor, actor_error, ctx, address);
        ActorStep::Panicked
    } else {
        ctx.metrics()
            .record_processed(u128_to_u64_saturating(started_at.elapsed().as_micros()));
        ActorStep::Continue
    }
}

fn handle_fired_timers<A: Actor>(
    actor: &mut A,
    ctx: &mut Context<A>,
    address: &RouteAddress,
) -> ActorStep {
    let fired_timers = ctx.timer_manager().fired_timers();
    for timer_id in fired_timers {
        let started_at = Instant::now();
        if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            actor.on_timer(timer_id, ctx);
        })) {
            tracing::error!(
                actor = ?address,
                error = ?error,
                "Managed actor panicked during timer handling"
            );
            ctx.metrics().record_panic();
            let actor_error = actor_error_from_panic(error.as_ref());
            notify_actor_error(actor, actor_error, ctx, address);
            return ActorStep::Panicked;
        }

        ctx.metrics()
            .record_processed(u128_to_u64_saturating(started_at.elapsed().as_micros()));
        if !ctx.is_running() {
            break;
        }
    }

    ActorStep::Continue
}

fn run_actor<A>(
    mut actor: A,
    address: &RouteAddress,
    router: &Arc<Router>,
    mailbox: &Arc<Mailbox>,
    running: &Arc<AtomicBool>,
    metrics: Arc<ActorMetrics>,
) where
    A: Actor,
    A::Message: Any + Send + Sync + 'static,
{
    let normal_receiver = mailbox.receiver().clone();
    let high_receiver = mailbox.high_priority_receiver().clone();
    let mut ctx = Context::with_metrics(address.clone(), router.clone(), metrics);
    let mut priority_state = PriorityDrainState::default();

    if start_managed_actor(&mut actor, &mut ctx, address) == ActorStep::Panicked {
        running.store(false, Ordering::SeqCst);
        router.unregister(address);
        ctx.stop();
        actor.stopped();
        return;
    }

    while running.load(Ordering::SeqCst) && ctx.is_running() {
        let Some(envelope) = receive_next_envelope(
            mailbox,
            &normal_receiver,
            &high_receiver,
            &mut priority_state,
        ) else {
            continue;
        };

        if envelope.is_expired() {
            ctx.metrics().record_expired();
            continue;
        }

        if process_envelope(envelope, &mut actor, &mut ctx, address, Instant::now())
            == ActorStep::Panicked
        {
            ctx.stop();
        }
    }

    running.store(false, Ordering::SeqCst);
    router.unregister(address);
    actor.stopped();
}

fn restart_tracker_for(strategy: &SupervisorStrategy) -> Option<RestartTracker> {
    if let SupervisorStrategy::Restart {
        max_restarts,
        within,
    } = strategy
    {
        Some(RestartTracker::new(*max_restarts, *within))
    } else {
        None
    }
}

fn stop_current_actor<A: Actor>(actor: &mut A, ctx: &mut Context<A>) {
    ctx.stop();
    actor.stopped();
}

#[derive(Clone, Copy)]
struct SupervisedActorRuntime<'a> {
    address: &'a RouteAddress,
    router: &'a Arc<Router>,
    mailbox: &'a Arc<Mailbox>,
    running: &'a Arc<AtomicBool>,
    metrics: &'a Arc<ActorMetrics>,
    health: &'a Arc<ManagedActorHealth>,
    strategy: &'a SupervisorStrategy,
}

fn run_supervised_actor<A, F>(actor_factory: F, runtime: SupervisedActorRuntime<'_>)
where
    A: Actor,
    A::Message: Any + Send + Sync + 'static,
    F: Fn() -> A,
{
    let normal_receiver = runtime.mailbox.receiver().clone();
    let high_receiver = runtime.mailbox.high_priority_receiver().clone();
    let mut priority_state = PriorityDrainState::default();
    let mut tracker = restart_tracker_for(runtime.strategy);
    let mut actor = actor_factory();
    let mut ctx = Context::with_metrics(
        runtime.address.clone(),
        runtime.router.clone(),
        Arc::clone(runtime.metrics),
    );

    if start_managed_actor(&mut actor, &mut ctx, runtime.address) == ActorStep::Panicked
        && !restart_supervised_actor(
            &mut actor,
            &mut ctx,
            &actor_factory,
            runtime.address,
            runtime.router,
            runtime.running,
            runtime.health,
            runtime.strategy,
            tracker.as_mut(),
        )
    {
        runtime.running.store(false, Ordering::SeqCst);
        runtime.router.unregister(runtime.address);
        actor.stopped();
        return;
    }

    while runtime.running.load(Ordering::SeqCst) && ctx.is_running() {
        if handle_fired_timers(&mut actor, &mut ctx, runtime.address) == ActorStep::Panicked {
            if !restart_supervised_actor(
                &mut actor,
                &mut ctx,
                &actor_factory,
                runtime.address,
                runtime.router,
                runtime.running,
                runtime.health,
                runtime.strategy,
                tracker.as_mut(),
            ) {
                break;
            }
            continue;
        }

        let Some(envelope) = receive_next_envelope(
            runtime.mailbox,
            &normal_receiver,
            &high_receiver,
            &mut priority_state,
        ) else {
            continue;
        };

        if envelope.is_expired() {
            ctx.metrics().record_expired();
            continue;
        }

        if process_envelope(
            envelope,
            &mut actor,
            &mut ctx,
            runtime.address,
            Instant::now(),
        ) == ActorStep::Panicked
            && !restart_supervised_actor(
                &mut actor,
                &mut ctx,
                &actor_factory,
                runtime.address,
                runtime.router,
                runtime.running,
                runtime.health,
                runtime.strategy,
                tracker.as_mut(),
            )
        {
            break;
        }
    }

    runtime.running.store(false, Ordering::SeqCst);
    runtime.router.unregister(runtime.address);
    actor.stopped();
}

#[allow(clippy::too_many_arguments)]
fn restart_supervised_actor<A, F>(
    actor: &mut A,
    ctx: &mut Context<A>,
    actor_factory: &F,
    address: &RouteAddress,
    router: &Arc<Router>,
    running: &Arc<AtomicBool>,
    health: &Arc<ManagedActorHealth>,
    strategy: &SupervisorStrategy,
    tracker: Option<&mut RestartTracker>,
) -> bool
where
    A: Actor,
    F: Fn() -> A,
{
    health.record_panic();

    match strategy.decide(&ActorError::Panic("supervised actor panic".to_string())) {
        SupervisionAction::Restart => {
            let Some(tracker) = tracker else {
                running.store(false, Ordering::SeqCst);
                router.unregister(address);
                ctx.stop();
                return false;
            };

            if !tracker.record_restart() {
                health.mark_exhausted();
                running.store(false, Ordering::SeqCst);
                router.unregister(address);
                ctx.stop();
                tracing::error!(
                    actor = ?address,
                    "Managed actor restart budget exhausted"
                );
                return false;
            }

            health.record_restart();
            stop_current_actor(actor, ctx);
            *actor = actor_factory();
            *ctx = Context::with_metrics(address.clone(), router.clone(), ctx.metrics().clone());
            actor.started(ctx);
            tracing::warn!(
                actor = ?address,
                restart_count = health.restart_count.load(Ordering::Relaxed),
                "Managed actor restarted after panic"
            );
            true
        }
        SupervisionAction::Resume => true,
        SupervisionAction::Stop | SupervisionAction::Escalate => {
            health.mark_exhausted();
            running.store(false, Ordering::SeqCst);
            router.unregister(address);
            ctx.stop();
            tracing::error!(
                actor = ?address,
                "Managed actor failed closed after panic"
            );
            false
        }
    }
}

/// Owned actor handle that unregisters routes and joins its worker on stop/drop.
pub struct ManagedActor<M: Send + 'static> {
    address: RouteAddress,
    router: Arc<Router>,
    mailbox: Arc<Mailbox>,
    actor_ref: ActorRef<M>,
    running: Arc<AtomicBool>,
    health: Arc<ManagedActorHealth>,
    join_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl<M: Send + 'static> ManagedActor<M> {
    /// Spawn an actor with a route-registered mailbox and owned worker thread.
    ///
    /// Unsupervised actors do not fire timers; use a supervised spawn when the
    /// actor relies on `Actor::on_timer` callbacks.
    #[must_use]
    pub fn spawn<A>(
        router: Arc<Router>,
        address: RouteAddress,
        actor: A,
        mailbox_capacity: usize,
    ) -> Self
    where
        A: Actor<Message = M>,
        M: Any + Send + Sync + 'static,
    {
        let mailbox = Arc::new(Mailbox::new(mailbox_capacity));
        router.register(address.clone(), mailbox.clone() as Arc<dyn MailboxSink>);

        let actor_ref = ActorRef::new(address.clone(), router.clone());
        let running = Arc::new(AtomicBool::new(true));
        let health = Arc::new(ManagedActorHealth::default());
        let metrics = Arc::new(ActorMetrics::new());
        let join_handle = {
            let worker_address = address.clone();
            let worker_router = router.clone();
            let worker_mailbox = mailbox.clone();
            let worker_running = running.clone();
            thread::spawn(move || {
                run_actor(
                    actor,
                    &worker_address,
                    &worker_router,
                    &worker_mailbox,
                    &worker_running,
                    metrics,
                );
            })
        };

        Self {
            address,
            router,
            mailbox,
            actor_ref,
            running,
            health,
            join_handle: Mutex::new(Some(join_handle)),
        }
    }

    /// Spawn an actor with restart supervision using the default restart policy.
    #[must_use]
    pub fn spawn_supervised<A, F>(
        router: Arc<Router>,
        address: RouteAddress,
        actor_factory: F,
        mailbox_capacity: usize,
    ) -> Self
    where
        A: Actor<Message = M>,
        F: Fn() -> A + Send + Sync + 'static,
        M: Any + Send + Sync + 'static,
    {
        Self::spawn_supervised_with_strategy(
            router,
            address,
            actor_factory,
            mailbox_capacity,
            SupervisorStrategy::default(),
        )
    }

    /// Spawn an actor that fails closed on its first panic.
    ///
    /// Production domain actors use this policy because their state ownership
    /// and readiness contract cannot safely be reconstructed by a transparent
    /// restart. The owning runtime observes the failed health state and stops
    /// accepting data-plane traffic.
    #[must_use]
    pub fn spawn_fail_closed<A, F>(
        router: Arc<Router>,
        address: RouteAddress,
        actor_factory: F,
        mailbox_capacity: usize,
    ) -> Self
    where
        A: Actor<Message = M>,
        F: Fn() -> A + Send + Sync + 'static,
        M: Any + Send + Sync + 'static,
    {
        Self::spawn_supervised_with_strategy(
            router,
            address,
            actor_factory,
            mailbox_capacity,
            SupervisorStrategy::stop(),
        )
    }

    /// Spawn an actor with restart supervision and an explicit strategy.
    #[must_use]
    pub fn spawn_supervised_with_strategy<A, F>(
        router: Arc<Router>,
        address: RouteAddress,
        actor_factory: F,
        mailbox_capacity: usize,
        strategy: SupervisorStrategy,
    ) -> Self
    where
        A: Actor<Message = M>,
        F: Fn() -> A + Send + Sync + 'static,
        M: Any + Send + Sync + 'static,
    {
        let mailbox = Arc::new(Mailbox::new(mailbox_capacity));
        router.register(address.clone(), mailbox.clone() as Arc<dyn MailboxSink>);

        let actor_ref = ActorRef::new(address.clone(), router.clone());
        let running = Arc::new(AtomicBool::new(true));
        let health = Arc::new(ManagedActorHealth::default());
        let metrics = Arc::new(ActorMetrics::new());
        let join_handle = {
            let worker_address = address.clone();
            let worker_router = router.clone();
            let worker_mailbox = mailbox.clone();
            let worker_running = running.clone();
            let worker_health = health.clone();
            thread::spawn(move || {
                run_supervised_actor(
                    actor_factory,
                    SupervisedActorRuntime {
                        address: &worker_address,
                        router: &worker_router,
                        mailbox: &worker_mailbox,
                        running: &worker_running,
                        metrics: &metrics,
                        health: &worker_health,
                        strategy: &strategy,
                    },
                );
            })
        };

        Self {
            address,
            router,
            mailbox,
            actor_ref,
            running,
            health,
            join_handle: Mutex::new(Some(join_handle)),
        }
    }

    #[must_use]
    pub fn actor_ref(&self) -> ActorRef<M> {
        self.actor_ref.clone()
    }

    /// Enqueue a message to the actor's normal mailbox lane.
    ///
    /// # Errors
    ///
    /// Returns `DeliveryError` when the actor route has stopped or the mailbox
    /// rejects the message due to backpressure.
    pub fn try_send(&self, msg: M) -> Result<(), DeliveryError>
    where
        M: Send + Sync + 'static,
    {
        if !self.is_running() {
            return Err(DeliveryError::ActorStopped);
        }

        self.mailbox
            .deliver(Envelope::new(self.address.clone(), msg))
    }

    /// Enqueue a message to the actor's high-priority mailbox lane.
    ///
    /// # Errors
    ///
    /// Returns `DeliveryError` when the actor route has stopped or the
    /// high-priority lane rejects the message due to backpressure.
    pub fn try_send_high_priority(&self, msg: M) -> Result<(), DeliveryError>
    where
        M: Send + Sync + 'static,
    {
        if !self.is_running() {
            return Err(DeliveryError::ActorStopped);
        }

        self.router
            .route_high_priority(Envelope::new(self.address.clone(), msg))
            .map_err(route_error_to_delivery_error)
    }

    #[must_use]
    pub fn address(&self) -> &RouteAddress {
        &self.address
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst) && !self.health.restart_exhausted.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn health_snapshot(&self) -> ManagedActorHealthSnapshot {
        self.health.snapshot(self.is_running())
    }

    #[cfg(test)]
    pub(crate) fn mark_permanently_failed_for_tests(&self) {
        self.health.mark_exhausted();
    }

    /// Stop the actor, unregister its route, and join the worker thread.
    pub fn stop(&self) {
        let was_running = self.running.swap(false, Ordering::SeqCst);
        if was_running {
            self.router.unregister(&self.address);
        }

        if let Some(join_handle) = self.join_handle.lock().take() {
            if let Err(error) = join_handle.join() {
                tracing::error!(
                    actor = ?self.address,
                    error = ?error,
                    "Managed actor worker panicked before join"
                );
            }
        }
    }
}

impl<M: Send + 'static> Drop for ManagedActor<M> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests;
