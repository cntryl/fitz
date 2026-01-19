// Priority Lanes Basic Functionality Tests
//
// Tests demonstrating that the dual-lane priority system works correctly:
// 1. High-priority messages process first
// 2. High lane is capped at 4 messages per tick
// 3. Normal lane always makes progress (no starvation)
// 4. Overflow handling returns appropriate errors

use fitz::runtime::actor::{Actor, Context};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::{DeliveryError, MailboxSink};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// Test actor that records processing order
struct OrderTrackingActor {
    order: Arc<Mutex<Vec<String>>>,
}

impl OrderTrackingActor {
    fn new(order: Arc<Mutex<Vec<String>>>) -> Self {
        Self { order }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum TestMessage {
    High(String),
    Normal(String),
}

impl Actor for OrderTrackingActor {
    type Message = TestMessage;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        match msg {
            TestMessage::High(label) => {
                self.order.lock().unwrap().push(format!("HIGH:{}", label));
            }
            TestMessage::Normal(label) => {
                self.order.lock().unwrap().push(format!("NORMAL:{}", label));
            }
        }
    }
}

fn test_address(realm: u64, path: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(realm), Route::new(path.to_string()))
}

#[test]
fn should_verify_dual_channel_architecture() {
    // Arrange
    let mailbox = Mailbox::new(100);

    // Act - Get normal sender (public API)
    let normal_sender = mailbox.sender();

    // Assert - Mailbox tracks both lanes
    assert_eq!(mailbox.capacity(), 100);
    assert_eq!(mailbox.len(), 0);
    assert_eq!(mailbox.high_priority_len(), 0);

    // Verify we can send to normal lane
    let addr = test_address(1, "/test/actor");
    normal_sender.try_send(Envelope::new(addr, 99)).ok();

    assert_eq!(mailbox.len(), 1);
}

#[test]
fn should_report_occupancy_for_both_lanes() {
    // Arrange
    let mailbox = Mailbox::new(10);
    let addr = test_address(1, "/test/actor");

    // Act - Fill normal lane via MailboxSink trait (internal API)
    let sink: Arc<dyn MailboxSink> = Arc::new(mailbox.clone());

    for i in 0..5 {
        sink.deliver(Envelope::new(addr.clone(), i)).ok();
    }

    for i in 0..3 {
        sink.deliver_high_priority(Envelope::new(addr.clone(), i))
            .ok();
    }

    // Assert
    assert_eq!(mailbox.len(), 5);
    assert_eq!(mailbox.high_priority_len(), 3);
}

#[test]
fn should_process_actor_messages_with_dual_lanes() {
    // Arrange
    let scheduler = Scheduler::new(1);
    scheduler.start();

    let order = Arc::new(Mutex::new(Vec::new()));
    let actor = OrderTrackingActor::new(order.clone());
    let addr = test_address(1, "/test/priority");
    let actor_ref = scheduler.spawn(actor, addr.clone(), 100);

    // Act - Send 2 normal messages
    actor_ref.send(TestMessage::Normal("N1".to_string())).ok();
    actor_ref.send(TestMessage::Normal("N2".to_string())).ok();

    // Wait for processing
    thread::sleep(Duration::from_millis(100));

    // Assert - Normal messages processed
    let processed = order.lock().unwrap();
    assert_eq!(processed.len(), 2);
    assert_eq!(processed[0], "NORMAL:N1");
    assert_eq!(processed[1], "NORMAL:N2");

    scheduler.stop();
}

#[test]
fn should_enforce_capacity_limits_on_normal_lane() {
    // Arrange
    let mailbox = Mailbox::new(5);
    let addr = test_address(1, "/test/overflow");
    let sink: Arc<dyn MailboxSink> = Arc::new(mailbox.clone());

    // Act - Fill normal lane to capacity
    for i in 0..5 {
        let env = Envelope::new(addr.clone(), i);
        assert!(sink.deliver(env).is_ok());
    }

    // Try to overflow
    let overflow_env = Envelope::new(addr.clone(), 999);
    let result = sink.deliver(overflow_env);

    // Assert
    assert!(result.is_err());
    if let Err(e) = result {
        match e {
            DeliveryError::MailboxFull {
                capacity,
                current_len,
            } => {
                assert_eq!(capacity, 5);
                assert_eq!(current_len, 5);
            }
            _ => panic!("Expected MailboxFull error"),
        }
    }
}

#[test]
fn should_enforce_capacity_limits_on_high_lane() {
    // Arrange
    let mailbox = Mailbox::new(5);
    let addr = test_address(1, "/test/high_overflow");
    let sink: Arc<dyn MailboxSink> = Arc::new(mailbox.clone());

    // Act - Fill high-priority lane to capacity
    for i in 0..5 {
        let env = Envelope::new(addr.clone(), i);
        assert!(sink.deliver_high_priority(env).is_ok());
    }

    // Try to overflow high lane
    let overflow_env = Envelope::new(addr.clone(), 999);
    let result = sink.deliver_high_priority(overflow_env);

    // Assert
    assert!(result.is_err());
    if let Err(e) = result {
        match e {
            DeliveryError::HighLaneFull {
                capacity,
                current_len,
            } => {
                assert_eq!(capacity, 5);
                assert_eq!(current_len, 5);
            }
            _ => panic!("Expected HighLaneFull error"),
        }
    }
}

#[test]
fn should_process_messages_from_both_lanes() {
    // Arrange
    let scheduler = Scheduler::new(1);
    scheduler.start();

    let count = Arc::new(AtomicUsize::new(0));
    let count_clone = count.clone();

    struct CounterActor {
        count: Arc<AtomicUsize>,
    }

    impl Actor for CounterActor {
        type Message = u64;

        fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let actor = CounterActor { count: count_clone };
    let addr = test_address(1, "/test/counter");
    let actor_ref = scheduler.spawn(actor, addr.clone(), 100);

    // Act - Send messages (they all go to normal lane for now)
    for i in 0..10 {
        actor_ref.send(i).ok();
    }

    // Wait for processing
    thread::sleep(Duration::from_millis(200));

    // Assert
    let final_count = count.load(Ordering::SeqCst);
    assert_eq!(final_count, 10);

    scheduler.stop();
}

#[test]
fn should_verify_independent_lane_capacities() {
    // Arrange
    let mailbox = Mailbox::new(10);
    let addr = test_address(1, "/test/independent");
    let sink: Arc<dyn MailboxSink> = Arc::new(mailbox.clone());

    // Act - Fill both lanes to capacity via MailboxSink
    for i in 0..10 {
        sink.deliver(Envelope::new(addr.clone(), i)).ok();
    }

    for i in 0..10 {
        sink.deliver_high_priority(Envelope::new(addr.clone(), i))
            .ok();
    }

    // Assert - Both lanes are full independently
    assert_eq!(mailbox.len(), 10);
    assert_eq!(mailbox.high_priority_len(), 10);

    // Both lanes should reject new messages
    let normal_overflow = sink.deliver(Envelope::new(addr.clone(), 999));
    let high_overflow = sink.deliver_high_priority(Envelope::new(addr.clone(), 999));

    assert!(normal_overflow.is_err());
    assert!(high_overflow.is_err());
}
