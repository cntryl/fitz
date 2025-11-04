//! Storage Provider Traits (SPEC 5.1)

/// StreamStore provides append-only streams with ordered sequence ids and read/peek/consume APIs.
pub trait StreamStore {
    /// Append to a stream; returns assigned sequence id.
    fn append(&self, stream_id: &str, payload: Vec<u8>) -> Result<u64, String>;

    /// Read forward from `from_seq` inclusive up to `limit` records.
    fn read(
        &self,
        stream_id: &str,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, String>;

    /// Peek the last record in a fully-qualified stream; returns (seq, payload) if present.
    fn peek(&self, stream_id: &str) -> Result<Option<(u64, Vec<u8>)>, String>;

    /// Consume hierarchically over a prefix id by deterministic order (ts, route, seq).
    /// Returns tuples of (route, seq, payload).
    fn consume_prefix(
        &self,
        prefix_id: &str,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(String, u64, Vec<u8>)>, String>;
}

/// QueueStore supports at-least-once delivery via leases.
pub trait QueueStore {
    /// Enqueue a message; if `dedupe_key` is provided, the backend SHOULD enforce idempotency.
    /// Returns msg id.
    fn enqueue(
        &self,
        queue_id: &str,
        message: Vec<u8>,
        dedupe_key: Option<&str>,
    ) -> Result<String, String>;

    /// Acquire a lease for up to `max_batch` messages (backend may return fewer). Returns tuples (id, payload, lease_token).
    fn lease(
        &self,
        queue_id: &str,
        visibility_secs: u32,
        max_batch: usize,
    ) -> Result<Vec<(String, Vec<u8>, String)>, String>;

    /// Complete (ack) a message lease using its id and token.
    fn complete(&self, queue_id: &str, id: &str, lease_token: &str) -> Result<(), String>;
}
