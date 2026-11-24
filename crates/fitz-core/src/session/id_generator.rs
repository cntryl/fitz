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
    use std::collections::HashSet;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn should_generate_increasing_session_ids() {
        // Arrange
        let first = generate();

        // Act
        let next = generate();

        // Assert
        assert!(next > first);
    }

    #[test]
    fn should_generate_distinct_session_ids_given_concurrent_callers() {
        // Arrange
        const WORKERS: usize = 8;
        const IDS_PER_WORKER: usize = 128;
        let start = Barrier::new(WORKERS);

        // Act
        let ids = thread::scope(|scope| {
            let workers: Vec<_> = (0..WORKERS)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        (0..IDS_PER_WORKER).map(|_| generate()).collect::<Vec<_>>()
                    })
                })
                .collect();
            workers
                .into_iter()
                .flat_map(|worker| worker.join().expect("generate session IDs"))
                .collect::<HashSet<_>>()
        });

        // Assert
        assert_eq!(ids.len(), WORKERS * IDS_PER_WORKER);
    }
}
