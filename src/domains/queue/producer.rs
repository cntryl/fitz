//! Producer-side batching for queue enqueue operations
//!
//! This module provides client-side batching to amortize the cost of enqueue operations.
//! Batching happens BEFORE messages reach the QueueActor, allowing producers to submit
//! large batches that map directly to Midge write-batch operations.
//!
//! # Batching Strategy
//!
//! - Messages are buffered in-memory by the producer
//! - Buffer flushes when:
//!   - Buffer size reaches `max_batch_size`
//!   - Flush timer expires
//! - Each flush sends ONE `enqueue_batch` request
//! - On failure, entire batch can be retried
//!
//! # Performance Characteristics
//!
//! - **Target throughput**: 1M+ msg/sec per queue
//! - **Batch sizes**: 100-1000 messages
//! - **Latency tradeoff**: Adds up to `flush_interval_ms` latency
//! - **CPU efficiency**: Amortizes serialization and network overhead

use crate::domains::queue::{QueueActor, QueueResponse};
use bytes::Bytes;
use std::time::{Duration, Instant};

/// Producer-side batching for queue enqueue operations
///
/// Aggregates messages on the producer side before submitting to the queue.
/// This pattern is CRITICAL for achieving multi-million msg/sec throughput.
///
/// # Design Philosophy
///
/// Producer batching exists at the CLIENT LAYER, not inside the actor.
/// The QueueActor receives only batch requests, never accumulates state.
/// This keeps the actor deterministic and the producer responsible for batching.
pub struct QueueProducer {
    /// Maximum batch size before automatic flush
    max_batch_size: usize,

    /// Maximum time to buffer before flush
    flush_interval: Duration,

    /// In-memory message buffer
    buffer: Vec<PendingMessage>,

    /// Time of last flush (for timer-based flushing)
    last_flush: Instant,
}

/// A message pending batch submission
#[derive(Debug, Clone)]
struct PendingMessage {
    body: Bytes,
    delay_seconds: Option<u64>,
}

impl QueueProducer {
    /// Create a new producer with batching enabled
    ///
    /// # Arguments
    /// * `max_batch_size` - Flush when buffer reaches this size (e.g., 100-1000)
    /// * `flush_interval` - Flush when this duration elapses (e.g., 1-5ms)
    ///
    /// # Recommendations
    /// - **High throughput**: max_batch_size=1000, flush_interval=5ms
    /// - **Low latency**: max_batch_size=10, flush_interval=1ms
    /// - **Balanced**: max_batch_size=100, flush_interval=2ms
    pub fn new(max_batch_size: usize, flush_interval: Duration) -> Self {
        Self {
            max_batch_size,
            flush_interval,
            buffer: Vec::with_capacity(max_batch_size),
            last_flush: Instant::now(),
        }
    }

    /// Enqueue a message (buffers until flush)
    ///
    /// Messages are NOT sent to the queue immediately. They are buffered
    /// and flushed when the buffer is full or the flush timer expires.
    ///
    /// # Arguments
    /// * `body` - Message payload
    /// * `delay_seconds` - Optional visibility delay
    ///
    /// # Returns
    /// `true` if buffer was auto-flushed, `false` if still buffering
    ///
    /// # Ordering Guarantees
    /// Messages are enqueued in FIFO order within each batch.
    /// Order between batches depends on when flushes occur.
    pub fn enqueue(&mut self, body: Bytes, delay_seconds: Option<u64>) -> bool {
        self.buffer.push(PendingMessage {
            body,
            delay_seconds,
        });

        // Check if we should auto-flush
        if self.buffer.len() >= self.max_batch_size {
            true // Caller should flush
        } else {
            false
        }
    }

    /// Check if flush timer has expired
    pub fn should_flush_timer(&self) -> bool {
        self.last_flush.elapsed() >= self.flush_interval
    }

    /// Flush buffered messages to the queue
    ///
    /// Drains the buffer and submits ONE enqueue_batch request to the actor.
    /// This is the critical operation that achieves batch efficiency.
    ///
    /// # Returns
    /// Number of messages flushed (0 if buffer was empty)
    ///
    /// # Semantics
    /// - All messages in batch succeed or all fail (atomic)
    /// - Message IDs assigned in FIFO order
    /// - Buffer is cleared after flush (even on error)
    pub fn flush(&mut self, actor: &mut QueueActor) -> Result<usize, String> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        // Group messages by delay_seconds (optimization: allows batch-level delay)
        // For MVP, we assume all messages in buffer have same delay
        let delay = self.buffer.first().and_then(|m| m.delay_seconds);

        // Extract message bodies
        let messages: Vec<Bytes> = self.buffer.drain(..).map(|m| m.body).collect();
        let count = messages.len();

        // Submit ONE batch request to actor
        match actor.handle_enqueue_batch(messages, delay) {
            QueueResponse::EnqueuedBatch { .. } => {
                self.last_flush = Instant::now();
                Ok(count)
            }
            QueueResponse::Error { message } => Err(message),
            QueueResponse::BadRequest { reason } => Err(reason),
            other => Err(format!("Unexpected response: {:?}", other)),
        }
    }

    /// Get current buffer size
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Get maximum batch size
    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_producer_with_batch_config() {
        // Arrange & Act
        let producer = QueueProducer::new(100, Duration::from_millis(5));

        // Assert
        assert_eq!(producer.max_batch_size(), 100);
        assert_eq!(producer.buffered_count(), 0);
    }

    #[test]
    fn should_buffer_messages_until_max_batch() {
        // Arrange
        let mut producer = QueueProducer::new(3, Duration::from_millis(100));

        // Act - Enqueue below threshold
        let flush1 = producer.enqueue(Bytes::from("msg1"), None);
        let flush2 = producer.enqueue(Bytes::from("msg2"), None);

        // Assert - No auto-flush yet
        assert!(!flush1);
        assert!(!flush2);
        assert_eq!(producer.buffered_count(), 2);

        // Act - Enqueue to reach threshold
        let flush3 = producer.enqueue(Bytes::from("msg3"), None);

        // Assert - Should signal flush
        assert!(flush3);
        assert_eq!(producer.buffered_count(), 3);
    }

    #[test]
    fn should_detect_timer_expiration() {
        // Arrange
        let producer = QueueProducer::new(100, Duration::from_millis(1));

        // Act - Wait for timer
        std::thread::sleep(Duration::from_millis(2));

        // Assert
        assert!(producer.should_flush_timer());
    }

    #[test]
    fn should_flush_batch_to_actor() {
        // Arrange
        use crate::benchkit::create_bench_queue_actor;
        let mut actor = create_bench_queue_actor("test", "queue", "jobs", None);
        let mut producer = QueueProducer::new(100, Duration::from_millis(5));

        // Act - Enqueue messages
        producer.enqueue(Bytes::from("msg1"), None);
        producer.enqueue(Bytes::from("msg2"), None);
        producer.enqueue(Bytes::from("msg3"), None);

        // Assert - Before flush
        assert_eq!(producer.buffered_count(), 3);

        // Act - Flush
        let result = producer.flush(&mut actor);

        // Assert - After flush
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);
        assert_eq!(producer.buffered_count(), 0);

        // Verify messages are in queue
        let reserve_response = actor.handle_reserve(30, Some(3));
        match reserve_response {
            crate::domains::queue::QueueResponse::Reserved { messages } => {
                assert_eq!(messages.len(), 3);
            }
            _ => panic!("Expected Reserved response"),
        }
    }
}
