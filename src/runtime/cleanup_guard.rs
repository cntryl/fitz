//! Runtime-owned disconnect-cleanup protocol for session-scoped domains.
//!
//! `SessionCleanup` is delivered on the high-priority/control-plane mailbox
//! lane, so it can pass an older, already-queued normal-lane request from
//! the same session. Remembering the cleaned-up session lets that stale
//! request fail instead of silently recreating actor/session state for a
//! session that is already gone and will never be cleaned up again.
//!
//! [`SessionScoped`] owns that protocol: the cleaned-up record, marking a
//! session *before* any state is released, and recognizing the cleanup
//! envelope. Each domain supplies only
//! [`SessionScoped::release_session_resources`].

use crate::runtime::{Envelope, SessionCleanup};
use std::collections::{HashSet, VecDeque};

/// Return the session being cleaned up when `envelope` carries
/// [`SessionCleanup`].
#[must_use]
pub fn session_cleanup_id(envelope: &Envelope) -> Option<u64> {
    envelope
        .payload::<SessionCleanup>()
        .map(|cleanup| cleanup.session_id)
}

/// Domain state that owns per-session resources released on disconnect.
///
/// Implementors provide the cleaned-up record and the release step; the
/// provided methods own ordering and must not be overridden.
pub trait SessionScoped {
    /// The bounded record of sessions cleanup has already run for.
    fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions;

    /// Release every resource this domain holds for `session_id`.
    ///
    /// Called only after the session is marked cleaned up, so a stale
    /// normal-lane request cannot recreate what this removes.
    fn release_session_resources(&mut self, session_id: u64);

    /// Whether disconnect cleanup already ran for `session_id`.
    fn is_cleaned_up_session(&mut self, session_id: u64) -> bool {
        self.cleaned_up_sessions().contains(session_id)
    }

    /// Mark `session_id` cleaned up, then release its resources.
    fn cleanup_session(&mut self, session_id: u64) {
        self.cleaned_up_sessions().mark(session_id);
        self.release_session_resources(session_id);
    }

    /// Run cleanup when `envelope` is a [`SessionCleanup`]; returns whether
    /// it was one.
    fn handle_cleanup_envelope(&mut self, envelope: &Envelope) -> bool {
        let Some(session_id) = session_cleanup_id(envelope) else {
            return false;
        };
        self.cleanup_session(session_id);
        true
    }
}

/// Bounded record of sessions that disconnect cleanup has already run for.
pub struct CleanedUpSessions {
    order: VecDeque<u64>,
    seen: HashSet<u64>,
    capacity: usize,
}

impl CleanedUpSessions {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            order: VecDeque::new(),
            seen: HashSet::new(),
            capacity: capacity.max(1),
        }
    }

    pub fn mark(&mut self, session_id: u64) {
        if self.seen.insert(session_id) {
            self.order.push_back(session_id);
            if self.order.len() > self.capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.seen.remove(&oldest);
                }
            }
        }
    }

    #[must_use]
    pub fn contains(&self, session_id: u64) -> bool {
        self.seen.contains(&session_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::runtime::{Envelope, SessionCleanup};

    struct RecordingScope {
        cleaned_up: CleanedUpSessions,
        released: Vec<(u64, bool)>,
    }

    impl RecordingScope {
        fn new() -> Self {
            Self {
                cleaned_up: CleanedUpSessions::new(4),
                released: Vec::new(),
            }
        }
    }

    impl SessionScoped for RecordingScope {
        fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions {
            &mut self.cleaned_up
        }

        fn release_session_resources(&mut self, session_id: u64) {
            let marked = self.cleaned_up.contains(session_id);
            self.released.push((session_id, marked));
        }
    }

    fn envelope<T: Send + Sync + 'static>(payload: T) -> Envelope {
        Envelope::new(
            RouteAddress::new(RouteFamily::new(0), Route::new("kv://cleanup")),
            payload,
        )
    }

    #[test]
    fn should_mark_session_before_releasing_resources() {
        // Arrange
        let mut scope = RecordingScope::new();

        // Act
        scope.cleanup_session(7);

        // Assert
        assert_eq!(scope.released, vec![(7, true)]);
        assert!(scope.is_cleaned_up_session(7));
    }

    #[test]
    fn should_intercept_session_cleanup_envelope() {
        // Arrange
        let mut scope = RecordingScope::new();

        // Act
        let handled = scope.handle_cleanup_envelope(&envelope(SessionCleanup { session_id: 9 }));

        // Assert
        assert!(handled);
        assert_eq!(scope.released, vec![(9, true)]);
    }

    #[test]
    fn should_ignore_non_cleanup_envelope() {
        // Arrange
        let mut scope = RecordingScope::new();

        // Act
        let handled = scope.handle_cleanup_envelope(&envelope(42_u64));

        // Assert
        assert!(!handled);
        assert!(scope.released.is_empty());
        assert!(!scope.is_cleaned_up_session(42));
    }

    #[test]
    fn should_extract_session_cleanup_id_from_envelope() {
        // Arrange
        let cleanup = envelope(SessionCleanup { session_id: 3 });
        let other = envelope(3_u64);

        // Act
        let ids = (session_cleanup_id(&cleanup), session_cleanup_id(&other));

        // Assert
        assert_eq!(ids, (Some(3), None));
    }
}
