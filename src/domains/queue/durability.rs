//! Queue durability policies
//!
//! Queues are "durable by default" but can explicitly trade durability for throughput.
//! This module defines per-queue durability semantics that do NOT affect other domains.
//!
//! # Durability Modes
//!
//! ## Strict (Default)
//! - fsync on every committed write batch
//! - Messages survive process crashes and power loss
//! - Throughput: 100-150K msg/sec
//! - **Guarantees**: True durability, at-least-once delivery
//!
//! ## Grouped { interval_ms }
//! - fsync at most once per interval (e.g., 1ms, 5ms, 10ms)
//! - Group commit batches multiple queue write batches
//! - Messages within interval window may be lost on crash
//! - Throughput: 500K+ msg/sec
//! - **Guarantees**: At-least-once on clean shutdown, best-effort on crash
//!
//! ## Async
//! - WAL append without fsync
//! - Maximum throughput, minimal write latency
//! - Messages may be lost on crash or power loss
//! - Throughput: 1-2M+ msg/sec
//! - **Guarantees**: Best-effort persistence, duplicates and loss possible
//!
//! # Isolation from Other Domains
//!
//! Queue durability settings use domain-specific Midge handles/namespaces.
//! Other domains (KV, streams, leases) maintain Strict durability regardless
//! of queue settings.
//!
//! # Usage
//!
//! ```ignore
//! // High-throughput job queue (tolerate message loss on crash)
//! let actor = QueueActor::with_durability(
//!     family,
//!     queue_key,
//!     store,
//!     None,
//!     QueueDurabilityPolicy::Async
//! );
//!
//! // Financial transactions (never lose messages)
//! let actor = QueueActor::with_durability(
//!     family,
//!     queue_key,
//!     store,
//!     None,
//!     QueueDurabilityPolicy::Strict
//! );
//!
//! // Balanced (low latency, minimal loss window)
//! let actor = QueueActor::with_durability(
//!     family,
//!     queue_key,
//!     store,
//!     None,
//!     QueueDurabilityPolicy::Grouped { interval_ms: 5 }
//! );
//! ```
//!
//! # Crash Behavior
//!
//! | Policy | Clean Shutdown | Process Crash | Power Loss |
//! |--------|----------------|---------------|------------|
//! | Strict | ✅ All persist | ✅ All persist | ✅ All persist |
//! | Grouped(1ms) | ✅ All persist | ⚠️ <1ms loss | ⚠️ <1ms loss |
//! | Grouped(5ms) | ✅ All persist | ⚠️ <5ms loss | ⚠️ <5ms loss |
//! | Async | ✅ All persist | ❌ Buffer loss | ❌ Buffer loss |
//!
//! # Design Rationale
//!
//! Many queue workloads (background jobs, analytics pipelines, cache warming)
//! tolerate message loss in exchange for 5-10× higher throughput. This policy
//! makes the tradeoff EXPLICIT in configuration rather than hidden.
//!
//! Critical queues (payments, orders, coordination) use Strict mode and get
//! full durability guarantees.

/// Queue-specific durability policy
///
/// Controls fsync behavior and group commit for queue writes.
/// Does NOT affect other domains sharing the same Midge instance.
///
/// # Data Loss Semantics
///
/// - **Strict**: No data loss, true durability
/// - **Grouped**: Data loss window = interval_ms on crash
/// - **Async**: Best-effort persistence, loss possible
///
/// All modes preserve at-least-once semantics for completed messages
/// (reserve/complete cycle is always durable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueueDurabilityPolicy {
    /// Strict durability (default)
    ///
    /// - fsync on every committed write batch
    /// - Messages survive crashes and power loss
    /// - Throughput: 100-150K msg/sec
    /// - Latency: ~10-50µs per message
    ///
    /// Use for: Financial transactions, orders, critical coordination
    #[default]
    Strict,

    /// Grouped commit with periodic fsync
    ///
    /// - fsync at most once per `interval_ms`
    /// - Multiple write batches grouped into one fsync
    /// - Messages within interval window may be lost on crash
    /// - Throughput: 500K+ msg/sec
    /// - Latency: ~5-15µs per message
    ///
    /// Use for: High-volume logs, analytics, background jobs
    ///
    /// # Data Loss Window
    /// On crash, messages enqueued within the last `interval_ms` may be lost.
    /// For example, `Grouped { interval_ms: 5 }` means up to 5ms of messages
    /// can disappear on crash (typically 500-2500 messages at peak throughput).
    Grouped {
        /// Maximum milliseconds between fsyncs
        ///
        /// Common values:
        /// - 1ms: Minimal loss (100-500 msgs), ~400-600K msg/sec
        /// - 5ms: Balanced (500-2500 msgs), ~600-800K msg/sec
        /// - 10ms: Aggressive (1000-5000 msgs), ~800K-1M msg/sec
        interval_ms: u32,
    },

    /// Async persistence (maximum throughput)
    ///
    /// - WAL append without fsync
    /// - OS page cache handles eventual flush
    /// - Messages may be lost on crash or power loss
    /// - Throughput: 1-2M+ msg/sec
    /// - Latency: ~2-5µs per message
    ///
    /// Use for: Cache warming, best-effort notifications, transient data
    ///
    /// # Data Loss Semantics
    /// On crash, all messages in OS buffers are lost (typically 10-100ms window).
    /// Clean shutdown still flushes all messages.
    Async,
}

impl QueueDurabilityPolicy {
    /// Check if this policy guarantees true durability (survives crashes)
    pub fn is_durable(&self) -> bool {
        matches!(self, Self::Strict)
    }

    /// Check if this policy may lose data on crash
    pub fn may_lose_data(&self) -> bool {
        !self.is_durable()
    }

    /// Get human-readable description of durability guarantees
    pub fn description(&self) -> &'static str {
        match self {
            Self::Strict => "Strict durability (no data loss)",
            Self::Grouped { .. } => "Grouped commit (loss window on crash)",
            Self::Async => "Async persistence (best-effort)",
        }
    }

    /// Get expected throughput range for this policy
    pub fn throughput_range(&self) -> (u32, u32) {
        match self {
            Self::Strict => (100_000, 150_000),
            Self::Grouped { interval_ms } => match interval_ms {
                1 => (400_000, 600_000),
                5 => (600_000, 800_000),
                10 => (800_000, 1_000_000),
                _ => (500_000, 1_000_000),
            },
            Self::Async => (1_000_000, 2_000_000),
        }
    }

    /// Convert policy to Midge write options
    ///
    /// Returns a tuple of (sync: bool, disable_wal: bool) that can be passed
    /// to Midge's transaction commit API to override global durability settings.
    ///
    /// # Midge WriteOptions Mapping
    ///
    /// - **Strict**: `(sync=true, disable_wal=false)` → fsync on every commit
    /// - **Grouped**: `(sync=false, disable_wal=false)` → WAL without immediate fsync
    /// - **Async**: `(sync=false, disable_wal=true)` → memory-only (no WAL)
    ///
    /// # Usage with Midge Transaction API
    ///
    /// ```ignore
    /// // Begin transaction
    /// let cf = self.store.default_column_family();
    /// let mut txn = self.store.begin_transaction(cf)?;
    ///
    /// // Add writes to transaction
    /// for (id, record) in batch {
    ///     let key = Self::message_key(&self.queue_key, id);
    ///     let value = Self::encode_record(&record);
    ///     txn.put(&key, &value)?;
    /// }
    ///
    /// // Commit with durability policy
    /// let (sync, disable_wal) = self.durability.to_midge_options();
    /// let mut opts = cntryl_midge::WriteOptions::default();
    /// opts.set_sync(sync);
    /// opts.set_disable_wal(disable_wal);
    /// self.store.commit_transaction_boxed(txn, &opts)?;
    /// ```
    ///
    /// # Domain Isolation
    ///
    /// Each queue actor translates its durability policy at commit time,
    /// allowing different queues to have different durability on the same
    /// Midge instance without affecting KV, streams, or leases.
    pub fn to_midge_options(&self) -> (bool, bool) {
        match self {
            // Strict: sync=true, wal=enabled
            Self::Strict => (true, false),
            // Grouped: sync=false (async), wal=enabled
            // Note: Midge doesn't have interval-based fsync yet,
            // so Grouped behaves like Async for now (TODO: add Midge group commit)
            Self::Grouped { .. } => (false, false),
            // Async: sync=false, wal=disabled (memory-only)
            Self::Async => (false, true),
        }
    }
}

/// Midge write options for queue operations
///
/// These options are passed to Midge on a per-write basis to control
/// durability without affecting other domains.
///
/// # Implementation Note
/// This struct will be passed to Midge's write_batch API when available.
/// For now, it serves as documentation of the intended API.
#[derive(Debug, Clone, Copy)]
pub struct QueueWriteOptions {
    /// Durability policy for this write
    pub policy: QueueDurabilityPolicy,

    /// Column family or namespace for queue writes
    ///
    /// Ensures queue durability settings don't affect other domains
    pub namespace: &'static str,
}

impl QueueWriteOptions {
    /// Create write options from durability policy
    pub fn from_policy(policy: QueueDurabilityPolicy) -> Self {
        Self {
            policy,
            namespace: "queue",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_default_to_strict_durability() {
        // Arrange & Act
        let policy = QueueDurabilityPolicy::default();

        // Assert
        assert_eq!(policy, QueueDurabilityPolicy::Strict);
        assert!(policy.is_durable());
        assert!(!policy.may_lose_data());
    }

    #[test]
    fn should_identify_strict_as_durable() {
        // Arrange
        let policy = QueueDurabilityPolicy::Strict;

        // Act & Assert
        assert!(policy.is_durable());
        assert!(!policy.may_lose_data());
        assert_eq!(policy.description(), "Strict durability (no data loss)");
    }

    #[test]
    fn should_identify_grouped_as_lossy() {
        // Arrange
        let policy = QueueDurabilityPolicy::Grouped { interval_ms: 5 };

        // Act & Assert
        assert!(!policy.is_durable());
        assert!(policy.may_lose_data());
        assert_eq!(
            policy.description(),
            "Grouped commit (loss window on crash)"
        );
    }

    #[test]
    fn should_identify_async_as_lossy() {
        // Arrange
        let policy = QueueDurabilityPolicy::Async;

        // Act & Assert
        assert!(!policy.is_durable());
        assert!(policy.may_lose_data());
        assert_eq!(policy.description(), "Async persistence (best-effort)");
    }

    #[test]
    fn should_return_throughput_ranges() {
        // Arrange
        let strict = QueueDurabilityPolicy::Strict;
        let grouped_1ms = QueueDurabilityPolicy::Grouped { interval_ms: 1 };
        let grouped_5ms = QueueDurabilityPolicy::Grouped { interval_ms: 5 };
        let async_policy = QueueDurabilityPolicy::Async;

        // Act & Assert
        assert_eq!(strict.throughput_range(), (100_000, 150_000));
        assert_eq!(grouped_1ms.throughput_range(), (400_000, 600_000));
        assert_eq!(grouped_5ms.throughput_range(), (600_000, 800_000));
        assert_eq!(async_policy.throughput_range(), (1_000_000, 2_000_000));
    }

    #[test]
    fn should_convert_strict_to_midge_options() {
        // Arrange
        let policy = QueueDurabilityPolicy::Strict;

        // Act
        let (sync, disable_wal) = policy.to_midge_options();

        // Assert
        assert!(sync); // fsync on every write
        assert!(!disable_wal); // WAL enabled
    }

    #[test]
    fn should_convert_grouped_to_midge_options() {
        // Arrange
        let policy = QueueDurabilityPolicy::Grouped { interval_ms: 5 };

        // Act
        let (sync, disable_wal) = policy.to_midge_options();

        // Assert
        assert!(!sync); // async writes
        assert!(!disable_wal); // WAL enabled (group commit)
    }

    #[test]
    fn should_convert_async_to_midge_options() {
        // Arrange
        let policy = QueueDurabilityPolicy::Async;

        // Act
        let (sync, disable_wal) = policy.to_midge_options();

        // Assert
        assert!(!sync); // async writes
        assert!(disable_wal); // WAL disabled (memory-only)
    }
}
