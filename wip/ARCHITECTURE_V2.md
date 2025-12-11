# Fitz v2 Architecture - Actor Model Implementation

## Overview

Fitz v2 is a complete rewrite using a pure **actor model** architecture with **clean durability boundaries**. Every subsystem is an actor. No shared mutable state. No locks on hot paths. All coordination via message passing.

## Core Principles

1. **Actor Model**: Every subsystem is an actor with its own mailbox
2. **Message Passing**: No shared state, all coordination via messages
3. **Clean Durability Boundary**: Only 4 things are durable (Streams, Queues, KV, Metrics)
4. **TLV Transport**: Simple [type:u16][len:u32][value:bytes] framing
5. **MidgeActor**: Single bridge to storage, all durability goes through it
6. **Synchronous Actors**: Actors are sync, only transport and Midge are async

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                        Transport                             │
│         (TCP, WebSocket, TLV Framing)                       │
└──────────────┬─────────────────────────────────────────────┘
               │ TLV Frames
               ↓
┌─────────────────────────────────────────────────────────────┐
│                     SessionActor                            │
│    (Per-connection: parsing, multiplexing, backpressure)   │
└──────────────┬─────────────────────────────────────────────┘
               │ Messages
               ↓
┌─────────────────────────────────────────────────────────────┐
│                    RouterActor                              │
│         (Global routing, wildcard matching)                 │
└──────────────┬─────────────────────────────────────────────┘
               │
               ├─→ RealmActor (pub/sub, ephemeral)
               ├─→ StreamActor (fanout, ephemeral) ─┐
               ├─→ QueueActor (scheduling, ephemeral) ─┤
               ├─→ RpcActor (correlation, ephemeral)   │
               ├─→ LeaseActor (timers, ephemeral)      │
               └─→ MetricsActor (counters, ephemeral)  │
                                                        │
                                                        ↓
                                         ┌────────────────────┐
                                         │    MidgeActor      │
                                         │ (ONLY durable I/O) │
                                         └────────────────────┘
                                                  │
                                                  ↓
                                            [ Midge Storage ]
                                              - Streams
                                              - Queues
                                              - KV
                                              - Metrics
```

## Module Structure

```
src/
├── actor/                # Core actor runtime
│   ├── mod.rs            # Actor trait + ActorContext
│   ├── actor_ref.rs      # ActorRef for message passing
│   ├── mailbox.rs        # Bounded message queues
│   ├── scheduler.rs      # Cooperative scheduler
│   ├── system.rs         # ActorSystem supervisor
│   ├── timers.rs         # Timer support
│   └── error.rs          # Actor errors
│
├── messages/             # Message definitions (one per actor)
│   ├── mod.rs
│   ├── session.rs        # SessionMsg
│   ├── routing.rs        # RouterMsg
│   ├── realm.rs          # RealmMsg
│   ├── stream.rs         # StreamMsg
│   ├── queue.rs          # QueueMsg
│   ├── rpc.rs            # RpcMsg
│   ├── lease.rs          # LeaseMsg
│   ├── metrics.rs        # MetricsMsg
│   └── midge.rs          # MidgeMsg (storage ops)
│
├── personas/             # Actor implementations
│   ├── mod.rs
│   ├── session_actor.rs  # Per-connection actor
│   ├── router_actor.rs   # Global routing table
│   ├── realm_actor.rs    # Pub/sub coordination
│   ├── stream_actor.rs   # Stream fanout
│   ├── queue_actor.rs    # Queue scheduling
│   ├── rpc_actor.rs      # RPC correlation
│   ├── lease_actor.rs    # Ephemeral leases
│   └── metrics_actor.rs  # Counters/histograms
│
├── transport/            # Network layer (async)
│   ├── mod.rs
│   ├── protocol.rs       # TLV framing
│   ├── tcp.rs            # TCP transport
│   ├── websocket.rs      # WebSocket transport
│   └── multiplexer.rs    # Connection multiplexing
│
├── storage/              # Durability bridge
│   ├── mod.rs
│   ├── midge_actor.rs    # ONLY actor that touches Midge
│   ├── types.rs          # Storage types
│   └── api.rs            # Storage API
│
├── routing/              # Route parsing/matching
│   ├── mod.rs
│   ├── matcher.rs        # Wildcard matching
│   ├── path.rs           # Route parsing
│   └── registry.rs       # Route registry
│
├── metrics/              # Observability
├── api/                  # Public SDK APIs
├── config/               # Configuration
├── util/                 # Utilities
├── bootstrap/            # System initialization
│   ├── mod.rs
│   └── system_init.rs    # FitzSystemBuilder
│
├── lib.rs                # Library root
└── prelude.rs            # Common imports
```

## Durability Boundary

**Only these are durable (via MidgeActor):**

- ✅ Streams (append-only logs)
- ✅ Queues (message queues with acks)
- ✅ KV (key-value storage)
- ✅ Metrics (optional persistence)

**Everything else is ephemeral:**

- ❌ Routing tables (RealmActor, RouterActor)
- ❌ RPC state (RpcActor)
- ❌ Leases (LeaseActor)
- ❌ Subscriptions (SessionActor, StreamActor)
- ❌ Consumer groups (RealmActor)

## Actor Message Flow Example

### Example: Publishing to a Stream

1. **Client** sends TLV frame → **SessionActor**
2. **SessionActor** parses frame → sends `StreamMsg::Append` to **StreamActor**
3. **StreamActor** sends `MidgeMsg::AppendStream` to **MidgeActor**
4. **MidgeActor** writes to Midge (async) → replies with `AppendStreamReply`
5. **StreamActor** receives reply → fanouts to subscribers
6. **StreamActor** sends `SessionMsg::StreamData` to subscriber **SessionActors**
7. **SessionActors** encode TLV frames → send to clients

## TLV Frame Format

All transport uses TLV (Type-Length-Value) framing:

```
┌─────────────┬──────────────┬───────────────────┐
│ Type (u16)  │ Len (u32)    │ Value (bytes)     │
│  2 bytes    │  4 bytes     │  variable         │
└─────────────┴──────────────┴───────────────────┘
```

Example frame types:
- `0x0100` - Stream append
- `0x0200` - Queue enqueue
- `0x0300` - RPC invoke
- `0x0400` - Lease acquire
- `0x0500` - KV put
- `0x0600` - Realm subscribe

## Building and Running

```bash
# Build
cargo build

# Run tests
cargo test

# Start server
cargo run --example server

# Connect client
cargo run --example client
```

## Example: Starting Fitz

```rust
use fitz::prelude::*;

fn main() -> Result<(), String> {
    // Build the system
    let system = FitzSystemBuilder::new()
        .with_name("fitz")
        .with_workers(4)
        .with_tcp("0.0.0.0:7070")
        .with_websocket("0.0.0.0:8080")
        .build()?;

    // Start (blocks)
    system.start()?;

    Ok(())
}
```

## Example: Creating a Custom Actor

```rust
use fitz::prelude::*;

#[derive(Debug)]
enum MyActorMsg {
    DoWork { data: String },
    Shutdown,
}

struct MyActor {
    state: Vec<String>,
}

impl Actor for MyActor {
    type Message = MyActorMsg;

    fn on_message(&mut self, msg: Self::Message, ctx: &mut ActorContext<Self::Message>) {
        match msg {
            MyActorMsg::DoWork { data } => {
                self.state.push(data);
                println!("Processed: {} items", self.state.len());
            }
            MyActorMsg::Shutdown => {
                ctx.stop();
            }
        }
    }

    fn on_start(&mut self, _ctx: &mut ActorContext<Self::Message>) {
        println!("MyActor started!");
    }

    fn on_stop(&mut self) {
        println!("MyActor stopped!");
    }
}
```

## Key Differences from Fitz v1

| Aspect | v1 (Old) | v2 (New) |
|--------|----------|----------|
| Architecture | Async handlers | Pure actor model |
| Concurrency | Tokio tasks + locks | Message passing only |
| State | Shared via Arc<RwLock> | Each actor owns its state |
| Durability | Mixed (unclear boundaries) | Clean (4 durable domains) |
| Transport | Multiple protocols | Unified TLV framing |
| Storage Access | Multiple actors touch storage | Only MidgeActor |
| Scheduling | Tokio async | Cooperative actor scheduler |

## Status

🚧 **Work in Progress** 🚧

This is a complete scaffolding of Fitz v2. Core structure is in place:

- ✅ Actor runtime (trait, mailbox, scheduler)
- ✅ All message definitions
- ✅ TLV transport protocol
- ✅ MidgeActor storage bridge
- ✅ Bootstrap system
- ⚠️ Persona actors (stubs, need implementation)
- ⚠️ Transport integration (TCP/WS incomplete)
- ⚠️ Real Midge integration (placeholder)

## Next Steps

1. Implement SessionActor with real TLV frame handling
2. Implement RouterActor with wildcard matching
3. Implement StreamActor with fanout logic
4. Implement QueueActor with visibility timers
5. Connect TCP/WebSocket transports to SessionActor
6. Integrate real Midge APIs into MidgeActor
7. Add comprehensive tests
8. Add benchmarks

## License

MIT
