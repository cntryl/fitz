// LAYER: RUNTIME (Core Synchronous Engine)
//! Actor runtime and execution model
//!
//! This is the authoritative layer. It contains:
//! - `routing`: Universal addressing model (`RouteFamily`, Route, `RouteAddress`)
//! - `actor`: Actor trait and lifecycle
//! - `mailbox`: Message queuing
//! - `scheduler`: Actor scheduling
//! - `envelope`: Message metadata and routing
//! - `router`: Message delivery infrastructure
//! - `matcher`: Wildcard pattern matching
//! - `subscriptions`: High-performance subscription indexing
//!
//! **CRITICAL INVARIANTS:**
//! - 100% synchronous (no async, no Tokio)
//! - No socket I/O
//! - No domain business logic
//! - Receives frames from Session
//! - Returns responses to Session
//!
//! This module is the core of Fitz and must remain pure and deterministic.

pub mod actor;
pub mod cf_validation;
pub mod client_frame;
pub mod clock;
pub mod context;
pub mod domain_event;
pub mod domain_manifest;
pub mod envelope;
pub mod mailbox;
pub mod managed_actor;
pub mod matcher;
pub mod router;
pub mod routing;
pub mod scheduler;
pub mod subscriptions;
pub mod supervision;

// Re-export commonly used types
pub use actor::{Actor, ActorError, ActorId, ActorRef, ActorState, Context, SendError};
pub use client_frame::{ClientChannel, ClientFrameMeta, EncodedClientFrame};
pub use clock::{
    epoch_ms_to_instant_with_reference, instant_to_epoch_ms_with_reference, Clock, SystemClock,
};
pub use context::{Timer, TimerId, TimerManager};
pub use domain_event::{DomainPublishEvent, SessionCleanup};
pub use domain_manifest::{DomainDescriptor, DomainKind, DomainRegistry};
pub use envelope::{Envelope, MessageId};
pub use mailbox::Mailbox;
pub use managed_actor::{ManagedActor, ManagedActorHealthSnapshot};
pub use matcher::{Pattern, PatternSegment};
pub use router::{DeliveryError, MailboxSink, RouteError, Router};
pub use scheduler::Scheduler;
pub use subscriptions::{SubscriptionId, SubscriptionIndex};
pub use supervision::{SupervisionAction, SupervisorStrategy};
