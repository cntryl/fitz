//! Disconnect flags retained only while admitted session work references them.
//!
//! The routing/admission edge attaches flags to canonical session-inbox sources.
//! Cleanup marks existing flags; it never adds permanent history. Last-reference
//! removal checks pointer identity so it cannot erase a concurrently admitted
//! replacement. Registry guards must be released before dropping upgraded flags.

use super::routing::{session_inbox_address, RouteAddress, RouteFamily};
use dashmap::DashMap;
use rustc_hash::FxBuildHasher;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

type WorkEntries = DashMap<RouteAddress, Weak<SessionWork>, FxBuildHasher>;

#[derive(Clone, Default)]
pub(super) struct SessionWorkRegistry(Arc<WorkEntries>);

impl SessionWorkRegistry {
    pub(super) fn admit(&self, source: &RouteAddress) -> Option<Arc<SessionWork>> {
        let session_id = source
            .route()
            .as_str()
            .strip_prefix("inbox://session/")?
            .parse::<u64>()
            .ok()?;
        if *source != session_inbox_address(*source.family(), session_id) {
            return None;
        }
        let mut entry = self.0.entry(source.clone()).or_default();
        if let Some(work) = entry.upgrade() {
            return Some(work);
        }
        let work = Arc::new(SessionWork {
            session_id,
            source: source.clone(),
            closed: AtomicBool::new(false),
            registry: Arc::downgrade(&self.0),
        });
        *entry = Arc::downgrade(&work);
        Some(work)
    }

    pub(super) fn close(&self, family: RouteFamily, session_id: u64) {
        let work = self
            .0
            .get(&session_inbox_address(family, session_id))
            .and_then(|entry| entry.upgrade());
        if let Some(work) = work {
            work.closed.store(true, Ordering::Release);
        }
    }
}

pub(super) struct SessionWork {
    session_id: u64,
    source: RouteAddress,
    closed: AtomicBool,
    registry: Weak<WorkEntries>,
}

impl SessionWork {
    pub(super) fn is_closed(&self, session_id: u64) -> bool {
        self.session_id == session_id && self.closed.load(Ordering::Acquire)
    }
}

impl Drop for SessionWork {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.remove_if(&self.source, |_, entry| std::ptr::eq(entry.as_ptr(), self));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_keep_closed_work_invalid_after_unrelated_cleanup_churn() {
        // Arrange
        let registry = SessionWorkRegistry::default();
        let family = RouteFamily::new(1);
        let work = registry
            .admit(&session_inbox_address(family, 1))
            .expect("work");
        registry.close(family, 1);

        // Act
        for session_id in 2..20_000 {
            registry.close(family, session_id);
        }

        // Assert
        assert!(work.is_closed(1));
        assert_eq!(registry.0.len(), 1);
    }

    #[test]
    fn should_release_session_record_after_last_work_reference_drains() {
        // Arrange
        let registry = SessionWorkRegistry::default();
        let work = registry
            .admit(&session_inbox_address(RouteFamily::new(1), 1))
            .expect("work");
        let retained = work.clone();
        drop(work);

        // Act
        drop(retained);

        // Assert
        assert!(registry.0.is_empty());
    }

    #[test]
    fn should_preserve_live_older_session_during_unrelated_cleanup_churn() {
        // Arrange
        let registry = SessionWorkRegistry::default();
        let family = RouteFamily::new(1);
        let work = registry
            .admit(&session_inbox_address(family, 1))
            .expect("work");

        // Act
        for session_id in 2..20_000 {
            registry.close(family, session_id);
        }

        // Assert
        assert!(!work.is_closed(1));
    }

    #[test]
    fn should_isolate_cleanup_flags_between_route_families() {
        // Arrange
        let registry = SessionWorkRegistry::default();
        let work = registry
            .admit(&session_inbox_address(RouteFamily::new(2), 1))
            .expect("work");

        // Act
        registry.close(RouteFamily::new(1), 1);

        // Assert
        assert!(!work.is_closed(1));
    }
    #[test]
    fn should_share_closed_flag_with_concurrent_admissions() {
        // Arrange
        let registry = SessionWorkRegistry::default();
        let source = session_inbox_address(RouteFamily::new(1), 1);
        let retained = registry.admit(&source).expect("original work");
        registry.close(RouteFamily::new(1), 1);

        // Act
        let threads = (0..8)
            .map(|_| {
                let registry = registry.clone();
                let source = source.clone();
                std::thread::spawn(move || {
                    registry
                        .admit(&source)
                        .expect("concurrent work")
                        .is_closed(1)
                })
            })
            .collect::<Vec<_>>();
        let all_closed = threads
            .into_iter()
            .all(|thread| thread.join().expect("admission thread"));

        // Assert
        assert!(all_closed);
        assert!(retained.is_closed(1));
    }

    #[test]
    fn should_isolate_cleanup_between_independent_routers() {
        // Arrange
        let registry = SessionWorkRegistry::default();
        let independent = SessionWorkRegistry::default();
        let family = RouteFamily::new(1);
        let work = independent
            .admit(&session_inbox_address(family, 1))
            .expect("work");

        // Act
        registry.close(family, 1);

        // Assert
        assert!(!work.is_closed(1));
    }

    #[test]
    fn should_retain_disconnect_flag_for_deferred_reply_lifetime() {
        // Arrange
        let registry = SessionWorkRegistry::default();
        let family = RouteFamily::new(1);
        let source = session_inbox_address(family, 1);
        let mut envelope = crate::runtime::Envelope::from_route(source.clone(), source.clone(), ());
        envelope.retain_session_work(registry.admit(&source).expect("work"));
        let deferred = envelope.clone_for_deferred_reply();
        drop(envelope);

        // Act
        registry.close(family, 1);

        // Assert
        assert!(deferred.is_closed_session_work(1));
        assert_eq!(registry.0.len(), 1);
    }

    #[test]
    fn should_release_disconnect_record_while_independent_reply_remains() {
        // Arrange
        let registry = SessionWorkRegistry::default();
        let source = session_inbox_address(RouteFamily::new(1), 1);
        let mut envelope = crate::runtime::Envelope::from_route(source.clone(), source.clone(), ());
        envelope.retain_session_work(registry.admit(&source).expect("work"));
        let reply = envelope.reply_to(());

        // Act
        drop(envelope);

        // Assert
        assert!(!reply.is_closed_session_work(1));
        assert!(registry.0.is_empty());
    }
    #[test]
    fn should_keep_replacement_work_registered_during_concurrent_last_reference_drop() {
        // Arrange
        let registry = SessionWorkRegistry::default();
        let family = RouteFamily::new(1);
        let source = session_inbox_address(family, 1);

        // Act
        for _ in 0..100 {
            let previous = registry.admit(&source).expect("previous work");
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let release = barrier.clone();
            let thread = std::thread::spawn(move || {
                release.wait();
                drop(previous);
            });
            barrier.wait();
            let current = registry.admit(&source).expect("current work");
            thread.join().expect("release thread");
            registry.close(family, 1);
            assert!(
                current.is_closed(1),
                "replacement work must remain reachable by cleanup"
            );
            drop(current);
        }

        // Assert
        assert!(registry.0.is_empty());
    }
}
