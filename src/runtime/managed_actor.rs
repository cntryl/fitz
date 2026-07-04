// LAYER: RUNTIME
//! Managed actor lifecycle for production domain actors.

use crate::observability as obs;
use crate::runtime::actor::{Actor, ActorError, ActorMetrics, ActorRef, Context};
use crate::runtime::envelope::Envelope;
use crate::runtime::mailbox::Mailbox;
use crate::runtime::router::{DeliveryError, MailboxSink, Router};
use crate::runtime::routing::RouteAddress;
use parking_lot::Mutex;
use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const MANAGED_ACTOR_POLL_TIMEOUT: Duration = Duration::from_millis(1);

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
) -> Option<Envelope> {
    crossbeam_channel::select_biased! {
        recv(high_receiver) -> message => match message {
            Ok(envelope) => {
                record_mailbox_observability(mailbox, &envelope);
                Some(envelope)
            }
            Err(_) => None,
        },
        recv(normal_receiver) -> message => match message {
            Ok(envelope) => {
                record_mailbox_observability(mailbox, &envelope);
                Some(envelope)
            }
            Err(_) => None,
        },
        default(MANAGED_ACTOR_POLL_TIMEOUT) => None,
    }
}

fn process_envelope<A: Actor>(
    envelope: Envelope,
    actor: &mut A,
    ctx: &mut Context<A>,
    address: &RouteAddress,
    started_at: Instant,
) where
    A::Message: Any + Send + Sync + 'static,
{
    let (metadata, msg) = envelope.into_parts::<A::Message>();

    let Some(msg) = msg else {
        ctx.metrics().record_type_mismatch();
        actor.on_error(
            ActorError::TypeMismatch {
                expected: std::any::type_name::<A::Message>().to_string(),
                envelope_id: metadata.id.as_u64(),
            },
            ctx,
        );
        return;
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
        actor.on_error(ActorError::Panic(format!("{error:?}")), ctx);
        ctx.stop();
    } else {
        ctx.metrics()
            .record_processed(u128_to_u64_saturating(started_at.elapsed().as_micros()));
    }
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

    actor.started(&mut ctx);

    while running.load(Ordering::SeqCst) && ctx.is_running() {
        let Some(envelope) = receive_next_envelope(mailbox, &normal_receiver, &high_receiver)
        else {
            continue;
        };

        if envelope.is_expired() {
            ctx.metrics().record_expired();
            continue;
        }

        process_envelope(envelope, &mut actor, &mut ctx, address, Instant::now());
    }

    running.store(false, Ordering::SeqCst);
    router.unregister(address);
    actor.stopped();
}

/// Owned actor handle that unregisters routes and joins its worker on stop/drop.
pub struct ManagedActor<M: Send + 'static> {
    address: RouteAddress,
    router: Arc<Router>,
    mailbox: Arc<Mailbox>,
    actor_ref: ActorRef<M>,
    running: Arc<AtomicBool>,
    join_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl<M: Send + 'static> ManagedActor<M> {
    /// Spawn an actor with a route-registered mailbox and owned worker thread.
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

        self.mailbox
            .deliver_high_priority(Envelope::new(self.address.clone(), msg))
    }

    #[must_use]
    pub fn address(&self) -> &RouteAddress {
        &self.address
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
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
mod tests {
    use super::*;
    use crate::runtime::routing::{Route, RouteFamily};

    enum TestManagedMessage {
        Increment,
        Read(crossbeam_channel::Sender<u32>),
    }

    struct CounterActor {
        count: u32,
        stopped: Option<crossbeam_channel::Sender<()>>,
    }

    impl CounterActor {
        fn new(stopped: crossbeam_channel::Sender<()>) -> Self {
            Self {
                count: 0,
                stopped: Some(stopped),
            }
        }
    }

    impl Actor for CounterActor {
        type Message = TestManagedMessage;

        fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
            match msg {
                TestManagedMessage::Increment => {
                    self.count = self.count.saturating_add(1);
                }
                TestManagedMessage::Read(reply) => {
                    let _ = reply.send(self.count);
                }
            }
        }

        fn stopped(&mut self) {
            if let Some(stopped) = self.stopped.take() {
                let _ = stopped.send(());
            }
        }
    }

    fn test_address() -> RouteAddress {
        RouteAddress::new(RouteFamily::new(9), Route::new("managed://counter"))
    }

    #[test]
    fn should_deliver_managed_actor_messages_through_mailbox() {
        // Arrange
        let router = Arc::new(Router::new());
        let (stopped_tx, _stopped_rx) = crossbeam_channel::bounded(1);
        let managed = ManagedActor::spawn(router, test_address(), CounterActor::new(stopped_tx), 8);
        let actor_ref = managed.actor_ref();
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        // Act
        actor_ref
            .send(TestManagedMessage::Increment)
            .expect("increment should enqueue");
        actor_ref
            .send(TestManagedMessage::Read(reply_tx))
            .expect("read should enqueue");
        let count = reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor should reply");

        // Assert
        assert_eq!(count, 1);
    }

    #[test]
    fn should_enqueue_managed_actor_message_through_delivery_api() {
        // Arrange
        let router = Arc::new(Router::new());
        let (stopped_tx, _stopped_rx) = crossbeam_channel::bounded(1);
        let managed = ManagedActor::spawn(router, test_address(), CounterActor::new(stopped_tx), 8);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        // Act
        managed
            .try_send_high_priority(TestManagedMessage::Increment)
            .expect("increment should enqueue");
        managed
            .try_send(TestManagedMessage::Read(reply_tx))
            .expect("read should enqueue");
        let count = reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor should reply");

        // Assert
        assert_eq!(count, 1);
    }

    #[test]
    fn should_unregister_managed_actor_route_when_stopped() {
        // Arrange
        let router = Arc::new(Router::new());
        let address = test_address();
        let (stopped_tx, _stopped_rx) = crossbeam_channel::bounded(1);
        let managed = ManagedActor::spawn(
            router.clone(),
            address.clone(),
            CounterActor::new(stopped_tx),
            8,
        );

        // Act
        managed.stop();
        let result = router.route(Envelope::new(address, TestManagedMessage::Increment));

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_managed_actor_delivery_api_when_stopped() {
        // Arrange
        let router = Arc::new(Router::new());
        let (stopped_tx, _stopped_rx) = crossbeam_channel::bounded(1);
        let managed = ManagedActor::spawn(router, test_address(), CounterActor::new(stopped_tx), 8);

        // Act
        managed.stop();
        let normal_result = managed.try_send(TestManagedMessage::Increment);
        let high_result = managed.try_send_high_priority(TestManagedMessage::Increment);

        // Assert
        assert!(matches!(normal_result, Err(DeliveryError::ActorStopped)));
        assert!(matches!(high_result, Err(DeliveryError::ActorStopped)));
    }

    #[test]
    fn should_join_managed_actor_worker_when_stopped() {
        // Arrange
        let router = Arc::new(Router::new());
        let (stopped_tx, stopped_rx) = crossbeam_channel::bounded(1);
        let managed = ManagedActor::spawn(router, test_address(), CounterActor::new(stopped_tx), 8);

        // Act
        managed.stop();
        let stopped = stopped_rx.recv_timeout(Duration::from_secs(1));

        // Assert
        assert!(stopped.is_ok());
        assert!(!managed.is_running());
    }

    #[test]
    fn should_not_unregister_replacement_route_when_stopped_handle_drops() {
        // Arrange
        let router = Arc::new(Router::new());
        let address = test_address();
        let (first_stopped_tx, _first_stopped_rx) = crossbeam_channel::bounded(1);
        let first = ManagedActor::spawn(
            router.clone(),
            address.clone(),
            CounterActor::new(first_stopped_tx),
            8,
        );
        first.stop();
        let (second_stopped_tx, _second_stopped_rx) = crossbeam_channel::bounded(1);
        let second = ManagedActor::spawn(router, address, CounterActor::new(second_stopped_tx), 8);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        // Act
        drop(first);
        second
            .try_send(TestManagedMessage::Read(reply_tx))
            .expect("replacement actor route should remain registered");
        let count = reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("replacement actor should reply");

        // Assert
        assert_eq!(count, 0);
    }

    #[test]
    fn should_join_managed_actor_worker_when_dropped() {
        // Arrange
        let router = Arc::new(Router::new());
        let address = test_address();
        let (stopped_tx, stopped_rx) = crossbeam_channel::bounded(1);

        // Act
        {
            let _managed = ManagedActor::spawn(
                router.clone(),
                address.clone(),
                CounterActor::new(stopped_tx),
                8,
            );
        }
        let stopped = stopped_rx.recv_timeout(Duration::from_secs(1));
        let result = router.route(Envelope::new(address, TestManagedMessage::Increment));

        // Assert
        assert!(stopped.is_ok());
        assert!(result.is_err());
    }
}
