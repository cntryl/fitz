//! Actor scheduler and executor.

use super::actor_ref::ActorRef;
use super::error::{ActorError, ActorResult};
use super::mailbox::Mailbox;
use super::{Actor, ActorContext};
use std::sync::Arc;
use std::thread;

/// Cooperative scheduler that drives actors.
pub struct Scheduler {
    name: String,
    worker_count: usize,
}

impl Scheduler {
    /// Create a new scheduler.
    pub fn new(name: impl Into<String>, worker_count: usize) -> Self {
        Self {
            name: name.into(),
            worker_count,
        }
    }

    /// Spawn an actor and return its reference.
    pub fn spawn<A>(&self, actor: A, name: impl Into<String>) -> ActorRef<A::Message>
    where
        A: Actor,
    {
        let mailbox_capacity = 1024; // TODO: make configurable
        let mailbox = Arc::new(Mailbox::new(mailbox_capacity));
        let actor_name = name.into();
        let actor_ref = ActorRef::new(mailbox.clone(), actor_name.clone());

        // Spawn actor worker thread
        let actor_ref_clone = actor_ref.clone();
        thread::spawn(move || {
            run_actor_loop(actor, mailbox, actor_ref_clone);
        });

        actor_ref
    }

    /// Start the scheduler (blocks until shutdown).
    pub fn start(&self) -> ActorResult<()> {
        // TODO: implement main scheduler loop
        Ok(())
    }

    /// Shutdown the scheduler gracefully.
    pub fn shutdown(&self) {
        // TODO: implement graceful shutdown
    }
}

/// Run the actor message loop.
fn run_actor_loop<A>(mut actor: A, mailbox: Arc<Mailbox<A::Message>>, actor_ref: ActorRef<A::Message>)
where
    A: Actor,
{
    // TODO: get proper system reference
    let system = super::system::ActorSystem::new("fitz");
    let mut ctx = ActorContext::new(actor_ref.clone(), system);

    // Call on_start
    actor.on_start(&mut ctx);

    // Process messages
    loop {
        match mailbox.recv() {
            Some(msg) => {
                actor.on_message(msg, &mut ctx);
            }
            None => {
                // Mailbox closed, actor is stopping
                break;
            }
        }
    }

    // Call on_stop
    actor.on_stop();
}

