//! Centralized session ID generation
//!
//! All session IDs are generated from a single monotonically-increasing counter
//! to ensure uniqueness across all connections and transports (TCP, WebSocket, etc.).
//!
//! This single source of truth prevents ID collisions and makes debugging easier.

use std::sync::atomic::{AtomicU64, Ordering};

/// Generate a unique session ID
///
/// Uses `Ordering::SeqCst` to ensure all writes are globally visible immediately,
/// which is critical for session ID uniqueness across all threads/connections.
pub fn generate() -> u64 {
    static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);
    SESSION_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_generate_unique_session_ids() {
        // Arrange

        // Act
        let id1 = generate();
        let id2 = generate();
        let id3 = generate();

        // Assert
        assert!(id1 < id2);
        assert!(id2 < id3);
    }

    #[test]
    fn should_never_repeat_session_ids() {
        // Arrange
        let count = 1000;

        // Act
        let ids: Vec<u64> = (0..count).map(|_| generate()).collect();

        // Assert
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                assert_ne!(ids[i], ids[j], "Session IDs must be unique");
            }
        }
    }
}
