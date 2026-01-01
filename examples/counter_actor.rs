//! Example: Counter actor demonstrating the runtime

use fitz::runtime::{Actor, ActorRef, Context, Scheduler};
use std::time::Duration;

#[derive(Debug)]
enum CounterMsg {
    Increment,
    Decrement,
    GetCount(crossbeam_channel::Sender<i32>),
    Stop,
}

struct CounterActor {
    count: i32,
}

impl CounterActor {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl Actor for CounterActor {
    type Message = CounterMsg;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            CounterMsg::Increment => {
                self.count += 1;
                println!("Count incremented to: {}", self.count);
            }
            CounterMsg::Decrement => {
                self.count -= 1;
                println!("Count decremented to: {}", self.count);
            }
            CounterMsg::GetCount(reply) => {
                println!("Current count: {}", self.count);
                let _ = reply.send(self.count);
            }
            CounterMsg::Stop => {
                println!("Stopping counter actor");
                ctx.stop();
            }
        }
    }

    fn started(&mut self, ctx: &mut Context<Self>) {
        println!("CounterActor {} started!", ctx.actor_id());
    }

    fn stopped(&mut self) {
        println!("CounterActor stopped. Final count: {}", self.count);
    }
}

fn main() {
    println!("=== Fitz Actor Runtime Example ===\n");

    // Create scheduler
    let scheduler = Scheduler::new(2);
    scheduler.start();

    // Spawn counter actor
    let actor = CounterActor::new();
    let actor_ref: ActorRef<CounterMsg> = scheduler.spawn(actor, 100);

    // Send messages
    println!("Sending increment messages...");
    actor_ref.send(CounterMsg::Increment).unwrap();
    actor_ref.send(CounterMsg::Increment).unwrap();
    actor_ref.send(CounterMsg::Increment).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    println!("\nSending decrement message...");
    actor_ref.send(CounterMsg::Decrement).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    // Query count
    println!("\nQuerying count...");
    let (tx, rx) = crossbeam_channel::bounded(1);
    actor_ref.send(CounterMsg::GetCount(tx)).unwrap();

    if let Ok(count) = rx.recv_timeout(Duration::from_secs(1)) {
        println!("Received count: {}", count);
    }

    std::thread::sleep(Duration::from_millis(100));

    // Stop actor
    println!("\nStopping actor...");
    actor_ref.send(CounterMsg::Stop).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    println!("\n=== Example Complete ===");
}
