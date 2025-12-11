//! Example: Creating and using a custom actor.
//!
//! This demonstrates:
//! - Defining a custom actor with its own message type
//! - Implementing the Actor trait
//! - Spawning the actor with a scheduler
//! - Sending messages to the actor
//!
//! Run with: cargo run --example custom_actor

use fitz::prelude::*;

/// Custom actor message type.
#[derive(Debug)]
enum CounterMsg {
    Increment,
    Decrement,
    GetValue { reply_to: ActorRef<CounterReply> },
    Reset,
    Shutdown,
}

/// Reply message for GetValue.
#[derive(Debug)]
struct CounterReply {
    value: i64,
}

/// A simple counter actor.
struct CounterActor {
    count: i64,
}

impl CounterActor {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl Actor for CounterActor {
    type Message = CounterMsg;

    fn on_message(&mut self, msg: Self::Message, ctx: &mut ActorContext<Self::Message>) {
        match msg {
            CounterMsg::Increment => {
                self.count += 1;
                println!("Counter incremented to {}", self.count);
            }
            CounterMsg::Decrement => {
                self.count -= 1;
                println!("Counter decremented to {}", self.count);
            }
            CounterMsg::GetValue { reply_to } => {
                println!("Sending counter value: {}", self.count);
                let _ = reply_to.tell(CounterReply { value: self.count });
            }
            CounterMsg::Reset => {
                self.count = 0;
                println!("Counter reset to 0");
            }
            CounterMsg::Shutdown => {
                println!("Counter shutting down...");
                ctx.stop();
            }
        }
    }

    fn on_start(&mut self, _ctx: &mut ActorContext<Self::Message>) {
        println!("CounterActor started!");
    }

    fn on_stop(&mut self) {
        println!("CounterActor stopped! Final count: {}", self.count);
    }
}

fn main() -> Result<(), String> {
    println!("═══════════════════════════════════════════");
    println!("  Fitz v2 - Custom Actor Example");
    println!("═══════════════════════════════════════════\n");

    // Create actor system and scheduler
    let system = ActorSystem::new("example");
    let scheduler = system.scheduler(2);

    // Spawn our custom actor
    let counter = scheduler.spawn(CounterActor::new(), "counter");
    println!("Spawned CounterActor: {}\n", counter.name());

    // Send some messages
    println!("Sending messages to counter...\n");

    counter.tell(CounterMsg::Increment).ok();
    counter.tell(CounterMsg::Increment).ok();
    counter.tell(CounterMsg::Increment).ok();
    counter.tell(CounterMsg::Decrement).ok();
    counter.tell(CounterMsg::Reset).ok();
    counter.tell(CounterMsg::Increment).ok();

    // Note: In a real system, we'd wait for replies or use synchronization
    // For this example, we just send messages and they'll process async

    println!("\n✅ Messages sent! (Actor processing in background)\n");
    println!("In a real system:");
    println!("  - Messages are processed asynchronously");
    println!("  - Use reply channels for responses");
    println!("  - Scheduler manages fair execution");

    // Give the actor a moment to process
    std::thread::sleep(std::time::Duration::from_millis(100));

    counter.tell(CounterMsg::Shutdown).ok();

    println!("\n✅ Custom actor example complete!");

    Ok(())
}
