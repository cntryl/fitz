# Fitz Actor Runtime

## Overview

The Fitz actor runtime provides a lightweight, synchronous actor system for message-based concurrency. Every subsystem in Fitz is an actor that processes messages sequentially without shared mutable state.

## Core Components

### Actor Trait

Every actor must implement the `Actor` trait:

```rust
use fitz::runtime::{Actor, Context};

struct MyActor {
    state: u32,
}

impl Actor for MyActor {
    type Message = MyMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        // Process message
    }

    fn started(&mut self, ctx: &mut Context<Self>) {
        // Called when actor starts
    }

    fn stopped(&mut self) {
        // Called when actor stops
    }

    fn on_error(&mut self, error: ActorError, ctx: &mut Context<Self>) {
        // Handle errors
    }
}
```

### Scheduler

The scheduler manages actor lifecycles and message processing:

```rust
use fitz::runtime::Scheduler;

let scheduler = Scheduler::new(4); // 4 worker threads
scheduler.start();

let actor_ref = scheduler.spawn(my_actor, 100); // mailbox capacity = 100
```

### Mailbox

Each actor has a bounded mailbox for message queuing:

- **Bounded capacity**: Prevents unbounded memory growth
- **Backpressure**: Send operations fail when mailbox is full
- **FIFO ordering**: Messages processed in order received

### Context

Actors receive a context during message processing:

```rust
fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
    // Get actor ID
    let id = ctx.actor_id();
    
    // Stop this actor
    ctx.stop();
    
    // Check if running
    if ctx.is_running() {
        // ...
    }
}
```

### Supervision

Supervision strategies define how to handle actor failures:

```rust
use fitz::runtime::SupervisorStrategy;
use std::time::Duration;

// Restart up to 3 times within 60 seconds
let strategy = SupervisorStrategy::restart(3, Duration::from_secs(60));

// Or use other strategies
let strategy = SupervisorStrategy::stop();
let strategy = SupervisorStrategy::escalate();
let strategy = SupervisorStrategy::resume();
```

### Timers

Schedule delayed and recurring messages:

```rust
use fitz::runtime::TimerManager;
use std::time::Duration;

let mut timers = TimerManager::new();

// One-time timer
let timer_id = timers.schedule_once(Duration::from_secs(5));

// Repeating timer
let timer_id = timers.schedule_repeat(
    Duration::from_secs(1),  // delay
    Duration::from_secs(10)  // interval
);

// Cancel timer
timers.cancel(timer_id);

// Get fired timers
let fired = timers.fired_timers();
```

## Message Passing

### Sending Messages

Use `ActorRef` to send messages to actors:

```rust
// Non-blocking send (fails if mailbox full)
actor_ref.send(MyMessage::DoWork)?;

// Get actor ID
let id = actor_ref.actor_id();
```

### Message Types

Messages must be `Send + 'static`:

```rust
#[derive(Debug)]
enum MyMessage {
    DoWork { data: String },
    GetStatus(crossbeam_channel::Sender<Status>),
    Stop,
}
```

## Design Principles

### 1. Synchronous Message Processing

Actors process messages **synchronously** - no async/await inside actors:

```rust
// ✅ CORRECT - Synchronous processing
fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
    match msg {
        MyMessage::Process(data) => {
            self.state.update(data);
        }
    }
}

// ❌ WRONG - No async in actors
async fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
    // Not allowed!
}
```

### 2. No Shared State

Actors own their state exclusively. No `Arc<Mutex<T>>` or shared references:

```rust
// ✅ CORRECT - Actor owns state
struct MyActor {
    state: HashMap<String, Value>,
}

// ❌ WRONG - No shared state
struct MyActor {
    state: Arc<Mutex<HashMap<String, Value>>>,
}
```

### 3. Message Passing Only

All coordination happens via messages:

```rust
// ✅ CORRECT - Send message to other actor
other_actor_ref.send(OtherMessage::Update(data))?;

// ❌ WRONG - No direct method calls
other_actor.update(data);
```

### 4. Bounded Mailboxes

Always use bounded mailboxes for backpressure:

```rust
// ✅ CORRECT - Bounded mailbox
let actor_ref = scheduler.spawn(actor, 100);

// Handle send failures
match actor_ref.send(msg) {
    Ok(()) => println!("Sent"),
    Err(SendError::MailboxFull) => {
        // Apply backpressure
    }
}
```

## Examples

### Counter Actor

See `examples/counter_actor.rs` for a complete example:

```bash
cargo run --example counter_actor
```

### Request-Reply Pattern

```rust
#[derive(Debug)]
enum QueryMsg {
    Get(String, crossbeam_channel::Sender<Option<String>>),
}

impl Actor for QueryActor {
    type Message = QueryMsg;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        match msg {
            QueryMsg::Get(key, reply) => {
                let value = self.data.get(&key).cloned();
                let _ = reply.send(value);
            }
        }
    }
}

// Usage
let (tx, rx) = crossbeam_channel::bounded(1);
actor_ref.send(QueryMsg::Get("key".to_string(), tx))?;
let value = rx.recv_timeout(Duration::from_secs(1))?;
```

### Actor Coordination

```rust
struct CoordinatorActor {
    workers: Vec<ActorRef<WorkerMsg>>,
}

impl Actor for CoordinatorActor {
    type Message = CoordinatorMsg;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        match msg {
            CoordinatorMsg::Distribute(work) => {
                // Send to all workers
                for worker in &self.workers {
                    let _ = worker.send(WorkerMsg::DoWork(work.clone()));
                }
            }
        }
    }
}
```

## Testing

The runtime includes comprehensive tests:

```bash
cargo test --lib runtime
```

Test actors follow the same patterns:

```rust
#[test]
fn should_process_messages() {
    let scheduler = Scheduler::new(1);
    scheduler.start();

    let actor = TestActor::new();
    let actor_ref = scheduler.spawn(actor, 10);

    actor_ref.send(TestMsg::DoWork).unwrap();
    
    // Verify behavior...
}
```

## Performance Characteristics

- **Message latency**: ~100-500ns for uncontended sends
- **Throughput**: Millions of messages per second per actor
- **Memory**: Bounded by mailbox capacity
- **Scheduling**: Cooperative (actors yield between messages)
- **Overhead**: Minimal (no async overhead in hot path)

## Comparison to Other Models

| Aspect | Fitz Runtime | Tokio | Actix |
|--------|--------------|-------|-------|
| Message Processing | Sync | Async | Async |
| Scheduling | Thread per actor | Task scheduler | Actor scheduler |
| Backpressure | Bounded mailbox | Manual | Manual |
| Overhead | Low | Medium | Low |
| Use Case | Deterministic, low-latency | High concurrency | Web services |

## Best Practices

1. **Keep actors focused** - Single responsibility per actor
2. **Use bounded mailboxes** - Always specify capacity
3. **Handle send failures** - Apply backpressure when mailbox is full
4. **Avoid blocking** - Don't block inside `receive()`
5. **No panics** - Use `Result` and error handling
6. **Test actors** - Unit test actor logic separately

## Future Enhancements

- [ ] Actor pools for load balancing
- [ ] Remote actors (network transparency)
- [ ] Persistent actors (event sourcing)
- [ ] Actor metrics and observability
- [ ] Hot code reloading

## References

- [ARCHITECTURE.md](../../specs/ARCHITECTURE.md) - Overall system architecture
- [Test Guidelines](test_guidelines.md) - Testing standards
- [examples/counter_actor.rs](../../examples/counter_actor.rs) - Example actor
