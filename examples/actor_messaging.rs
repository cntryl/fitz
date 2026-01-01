//! Example demonstrating actor-to-actor messaging with Envelope routing
//!
//! This example shows:
//! - Actor-to-actor communication via Context::send()
//! - Request-reply pattern using Context::reply()
//! - Causation tracking across message chains
//! - Deadline enforcement (expired messages are dropped)
//!
//! Run with: cargo run --example actor_messaging

use fitz::runtime::actor::{Actor, ActorId, Context};
use fitz::runtime::scheduler::Scheduler;
use fitz::transport::envelope::Envelope;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

/// Ping actor that sends a message to another actor
struct PingActor {
    target: ActorId,
    pings_sent: Arc<Mutex<usize>>,
}

#[derive(Debug)]
enum PingMessage {
    Start,
    Pong(String),
}

impl Actor for PingActor {
    type Message = PingMessage;

    fn receive(&mut self, msg: PingMessage, ctx: &mut Context<Self>) {
        match msg {
            PingMessage::Start => {
                println!("[Ping {}] Sending ping to {:?}", ctx.actor_id(), self.target);
                // Send a message to the pong actor
                ctx.send(self.target, PongMessage::Ping("Hello".to_string()))
                    .expect("Failed to send ping");
                *self.pings_sent.lock().unwrap() += 1;
            }
            PingMessage::Pong(response) => {
                println!(
                    "[Ping {}] Received pong response: {}",
                    ctx.actor_id(),
                    response
                );
                *self.pings_sent.lock().unwrap() += 1;
                ctx.stop();
            }
        }
    }
}

/// Pong actor that responds to ping messages
struct PongActor {
    pongs_sent: Arc<Mutex<usize>>,
}

#[derive(Debug)]
enum PongMessage {
    Ping(String),
}

impl Actor for PongActor {
    type Message = PongMessage;

    fn receive(&mut self, msg: PongMessage, ctx: &mut Context<Self>) {
        match msg {
            PongMessage::Ping(data) => {
                println!(
                    "[Pong {}] Received ping with data: {}",
                    ctx.actor_id(),
                    data
                );
                // Reply to the sender
                ctx.reply(PingMessage::Pong(format!("Echo: {}", data)))
                    .expect("Failed to send pong");
                *self.pongs_sent.lock().unwrap() += 1;
                ctx.stop();
            }
        }
    }
}

fn main() {
    println!("=== Actor Messaging Example ===\n");

    // Create scheduler
    let scheduler = Scheduler::new(2);
    scheduler.start();

    // Example 1: Direct message sending
    println!("Example 1: Direct Actor-to-Actor Messaging");
    println!("-------------------------------------------");

    let pings_sent = Arc::new(Mutex::new(0));
    let pongs_sent = Arc::new(Mutex::new(0));

    let pong_actor = PongActor {
        pongs_sent: pongs_sent.clone(),
    };
    let pong_ref = scheduler.spawn(pong_actor, 10);

    let ping_actor = PingActor {
        target: pong_ref.actor_id(),
        pings_sent: pings_sent.clone(),
    };
    let ping_ref = scheduler.spawn(ping_actor, 10);

    // Trigger the ping
    ping_ref
        .send(PingMessage::Start)
        .expect("Failed to send start");

    // Wait for messages to process
    thread::sleep(Duration::from_millis(200));

    println!(
        "Total pings sent: {}",
        *pings_sent.lock().unwrap()
    );
    println!(
        "Total pongs sent: {}\n",
        *pongs_sent.lock().unwrap()
    );

    // Example 2: Manual envelope routing (for demonstration)
    println!("Example 2: Manual Envelope Routing with Causation");
    println!("--------------------------------------------------");

    struct LogActor {
        logs: Arc<Mutex<Vec<String>>>,
    }

    impl Actor for LogActor {
        type Message = String;

        fn receive(&mut self, msg: String, _ctx: &mut Context<Self>) {
            println!("[Log] Received: {}", msg);
            self.logs.lock().unwrap().push(msg);
        }
    }

    let logs = Arc::new(Mutex::new(Vec::new()));
    let log_actor = LogActor { logs: logs.clone() };
    let log_ref = scheduler.spawn(log_actor, 10);

    // Create envelopes with causation chain
    let envelope1 = Envelope::new(log_ref.actor_id(), "First message".to_string());
    let msg_id = envelope1.id();

    scheduler
        .router()
        .route(envelope1)
        .expect("Failed to route");

    let envelope2 = Envelope::new(log_ref.actor_id(), "Second message (caused by first)".to_string())
        .with_causation(msg_id);

    scheduler
        .router()
        .route(envelope2)
        .expect("Failed to route");

    thread::sleep(Duration::from_millis(100));

    println!(
        "Total logs received: {}\n",
        logs.lock().unwrap().len()
    );

    // Example 3: Deadline enforcement
    println!("Example 3: Deadline Enforcement (Expired Messages Dropped)");
    println!("-----------------------------------------------------------");

    struct CounterActor {
        count: Arc<Mutex<usize>>,
    }

    impl Actor for CounterActor {
        type Message = String;

        fn receive(&mut self, msg: String, _ctx: &mut Context<Self>) {
            println!("[Counter] Received: {}", msg);
            *self.count.lock().unwrap() += 1;
        }
    }

    let count = Arc::new(Mutex::new(0));
    let counter_actor = CounterActor {
        count: count.clone(),
    };
    let counter_ref = scheduler.spawn(counter_actor, 10);

    // Send an expired message (should be dropped)
    let past_deadline = std::time::Instant::now() - Duration::from_secs(1);
    let expired_envelope =
        Envelope::new(counter_ref.actor_id(), "Expired message".to_string())
            .with_deadline(past_deadline);

    println!("Sending expired message (will be dropped)...");
    scheduler
        .router()
        .route(expired_envelope)
        .expect("Failed to route");

    // Send a valid message
    let future_deadline = std::time::Instant::now() + Duration::from_secs(5);
    let valid_envelope = Envelope::new(counter_ref.actor_id(), "Valid message".to_string())
        .with_deadline(future_deadline);

    println!("Sending valid message (will be processed)...");
    scheduler
        .router()
        .route(valid_envelope)
        .expect("Failed to route");

    thread::sleep(Duration::from_millis(100));

    println!(
        "Messages processed (should be 1, not 2): {}",
        *count.lock().unwrap()
    );

    println!("\n=== All examples completed ===");
}
