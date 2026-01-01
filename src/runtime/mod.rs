//! Actor runtime and execution model
//!
//! This module provides the core actor runtime for Fitz:
//! - `actor`: Actor trait and lifecycle
//! - `mailbox`: Message queuing with bounded capacity
//! - `scheduler`: Cooperative actor scheduling
//! - `supervision`: Fault tolerance and restart strategies
//! - `context`: Actor execution context with timers

pub mod actor;
pub mod context;
pub mod mailbox;
pub mod scheduler;
pub mod supervision;

// Re-export commonly used types
pub use actor::{Actor, ActorError, ActorId, ActorRef, ActorState, Context, SendError};
pub use context::{Timer, TimerId, TimerManager};
pub use mailbox::Mailbox;
pub use scheduler::Scheduler;
pub use supervision::{SupervisionAction, SupervisorStrategy};
