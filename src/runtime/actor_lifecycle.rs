//! Shared panic containment for actor lifecycle hooks.

use super::{Actor, ActorError, Context};
use crate::runtime::routing::RouteAddress;
use std::any::Any;

pub(super) fn actor_error_from_panic(error: &(dyn Any + Send)) -> ActorError {
    if let Some(message) = error.downcast_ref::<&'static str>() {
        ActorError::Panic((*message).to_string())
    } else if let Some(message) = error.downcast_ref::<String>() {
        ActorError::Panic(message.clone())
    } else {
        ActorError::Panic("non-string panic payload".to_string())
    }
}

pub(super) fn notify_actor_error<A: Actor>(
    actor: &mut A,
    error: ActorError,
    ctx: &mut Context<A>,
    address: &RouteAddress,
) {
    if let Err(error_hook_panic) =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.on_error(error, ctx)))
    {
        tracing::error!(
            actor = ?address,
            error = ?error_hook_panic,
            "Actor panicked while handling actor error"
        );
    }
}

pub(super) fn start_actor<A: Actor>(
    actor: &mut A,
    ctx: &mut Context<A>,
    address: &RouteAddress,
) -> bool {
    let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.started(ctx)))
    else {
        return true;
    };

    tracing::error!(actor = ?address, error = ?error, "Actor panicked during startup");
    ctx.metrics().record_panic();
    notify_actor_error(actor, actor_error_from_panic(error.as_ref()), ctx, address);
    false
}
