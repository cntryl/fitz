use super::*;
use crate::runtime::routing::{Route, RouteFamily};

enum TestManagedMessage {
    Increment,
    Panic,
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
            TestManagedMessage::Panic => {
                panic!("managed actor test panic");
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

fn wait_until(label: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("{label}");
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

#[test]
fn should_restart_supervised_actor_after_one_panic_plus_accept_next_message() {
    // Arrange
    let router = Arc::new(Router::new());
    let (stopped_tx, _stopped_rx) = crossbeam_channel::bounded(8);
    let managed = ManagedActor::spawn_supervised(
        router,
        test_address(),
        move || CounterActor::new(stopped_tx.clone()),
        8,
    );
    managed
        .try_send(TestManagedMessage::Increment)
        .expect("increment should enqueue");
    managed
        .try_send(TestManagedMessage::Panic)
        .expect("panic should enqueue");
    wait_until("supervised actor should restart", || {
        managed.health_snapshot().restart_count == 1
    });
    let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

    // Act
    managed
        .try_send(TestManagedMessage::Read(reply_tx))
        .expect("read should enqueue after restart");
    let count = reply_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("actor should reply after restart");

    // Assert
    assert_eq!(count, 0);
    let health = managed.health_snapshot();
    assert!(health.running);
    assert_eq!(health.panic_count, 1);
    assert!(!health.restart_exhausted);
}

#[test]
fn should_exhaust_supervised_restart_budget_plus_report_permanent_failure() {
    // Arrange
    let router = Arc::new(Router::new());
    let (stopped_tx, _stopped_rx) = crossbeam_channel::bounded(8);
    let managed = ManagedActor::spawn_supervised_with_strategy(
        router,
        test_address(),
        move || CounterActor::new(stopped_tx.clone()),
        8,
        SupervisorStrategy::restart(1, Duration::from_mins(1)),
    );
    managed
        .try_send(TestManagedMessage::Panic)
        .expect("first panic should enqueue");
    wait_until("first restart should be recorded", || {
        managed.health_snapshot().restart_count == 1
    });

    // Act
    managed
        .try_send(TestManagedMessage::Panic)
        .expect("second panic should enqueue");
    wait_until("restart budget should exhaust", || {
        managed.health_snapshot().restart_exhausted
    });
    let send_after_exhaustion = managed.try_send(TestManagedMessage::Increment);

    // Assert
    let health = managed.health_snapshot();
    assert!(!health.running);
    assert_eq!(health.restart_count, 1);
    assert_eq!(health.panic_count, 2);
    assert!(matches!(
        send_after_exhaustion,
        Err(DeliveryError::ActorStopped)
    ));
}

#[test]
fn should_keep_unsupervised_actor_stop_on_panic_behavior() {
    // Arrange
    let router = Arc::new(Router::new());
    let (stopped_tx, _stopped_rx) = crossbeam_channel::bounded(1);
    let managed = ManagedActor::spawn(router, test_address(), CounterActor::new(stopped_tx), 8);

    // Act
    managed
        .try_send(TestManagedMessage::Panic)
        .expect("panic should enqueue");
    wait_until("unsupervised actor should stop", || !managed.is_running());
    let send_after_panic = managed.try_send(TestManagedMessage::Increment);

    // Assert
    assert!(matches!(send_after_panic, Err(DeliveryError::ActorStopped)));
}
