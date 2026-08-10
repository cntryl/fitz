// LAYER: RUNTIME
//! Lazily spawns one managed, fail-closed mailbox actor per active key.
//!
//! Unlike [`crate::runtime::family_actor_pool::FamilyActorPool`] (fixed,
//! family-keyed, statically provisioned at startup), this pool grows
//! dynamically as new keys are first seen, but is capped. Once the cap is
//! reached, existing actors remain intact and new keys fail closed.

use crate::runtime::actor::Actor;
use crate::runtime::managed_actor::ManagedActor;
use crate::runtime::router::Router;
use crate::runtime::routing::RouteAddress;
use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::RwLock;

pub struct KeyedActorPool<K, M: Send + 'static> {
    router: Arc<Router>,
    mailbox_capacity: usize,
    max_actors: usize,
    actors: RwLock<HashMap<K, ManagedActor<M>>>,
}

impl<K, M> KeyedActorPool<K, M>
where
    K: Clone + Eq + Hash,
    M: Any + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(router: Arc<Router>, mailbox_capacity: usize, max_actors: usize) -> Self {
        Self {
            router,
            mailbox_capacity,
            max_actors,
            actors: RwLock::new(HashMap::new()),
        }
    }

    /// Ensure an actor is running for `key`, spawning it (fail-closed) at
    /// `address` via `actor_factory` the first time `key` is seen.
    ///
    /// A no-op if an actor for `key` already exists, even if that actor has
    /// since failed closed after a panic — callers observe delivery failure
    /// through the normal `Router::route` error path, matching every other
    /// fail-closed actor in the runtime.
    pub fn ensure_spawned<A, F>(&self, key: K, address: RouteAddress, actor_factory: F) -> bool
    where
        A: Actor<Message = M>,
        F: Fn() -> A + Send + Sync + 'static,
    {
        if self.actors.read().contains_key(&key) {
            return true;
        }

        let mut actors = self.actors.write();
        if actors.contains_key(&key) {
            return true;
        }
        if self.max_actors == 0 || actors.len() >= self.max_actors {
            return false;
        }
        actors.entry(key).or_insert_with(|| {
            ManagedActor::spawn_fail_closed(
                self.router.clone(),
                address,
                actor_factory,
                self.mailbox_capacity,
            )
        });
        true
    }

    #[cfg(test)]
    fn actor_count(&self) -> usize {
        self.actors.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::actor::Context;
    use crate::runtime::envelope::Envelope;
    use crate::runtime::routing::{Route, RouteFamily};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct CountingActor {
        count: Arc<AtomicU64>,
    }

    impl Actor for CountingActor {
        type Message = u64;

        fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
            self.count.fetch_add(msg, Ordering::SeqCst);
        }
    }

    #[test]
    fn should_spawn_actor_once_per_key_plus_route_messages() {
        // Arrange
        let router = Arc::new(Router::new());
        let pool: KeyedActorPool<&'static str, u64> = KeyedActorPool::new(router.clone(), 16, 4);
        let count = Arc::new(AtomicU64::new(0));
        let address = RouteAddress::new(RouteFamily::new(1), Route::new("stream://bench/area/x"));

        // Act
        for _ in 0..3 {
            let count = count.clone();
            assert!(
                pool.ensure_spawned("bench/area", address.clone(), move || CountingActor {
                    count: count.clone(),
                })
            );
        }
        router
            .route(Envelope::new(address.clone(), 5_u64))
            .expect("route to spawned actor");
        router
            .route(Envelope::new(address, 7_u64))
            .expect("route to spawned actor");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while count.load(Ordering::SeqCst) != 12 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }

        // Assert
        assert_eq!(count.load(Ordering::SeqCst), 12);
    }

    #[test]
    fn should_preserve_existing_actors_when_capacity_is_reached() {
        // Arrange
        let router = Arc::new(Router::new());
        let pool: KeyedActorPool<u64, u64> = KeyedActorPool::new(router.clone(), 16, 2);
        let count = Arc::new(AtomicU64::new(0));

        // Act
        for key in 0..3 {
            let count = count.clone();
            let spawned = pool.ensure_spawned(
                key,
                RouteAddress::new(
                    RouteFamily::new(1),
                    Route::new(format!("stream://bench/area/{key}")),
                ),
                move || CountingActor {
                    count: count.clone(),
                },
            );
            assert_eq!(spawned, key < 2);
        }
        for key in 0..2 {
            router
                .route(Envelope::new(
                    RouteAddress::new(
                        RouteFamily::new(1),
                        Route::new(format!("stream://bench/area/{key}")),
                    ),
                    1_u64,
                ))
                .expect("route to preserved actor");
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while count.load(Ordering::SeqCst) != 2 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }

        // Assert
        assert_eq!(pool.actor_count(), 2);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert!(router
            .route(Envelope::new(
                RouteAddress::new(RouteFamily::new(1), Route::new("stream://bench/area/2"),),
                1_u64,
            ))
            .is_err());
    }
}
