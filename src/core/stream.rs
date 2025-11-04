use crate::core::engine::EngineHandle;
use crate::storage::mem::ExpectedRevision;

/// Stream API: append-only ordered logs with peek and live notifications (via Notice subscribe route).
#[derive(Clone, Debug)]
pub struct Stream {
    engine: EngineHandle,
}

impl Stream {
    pub fn new(engine: EngineHandle) -> Self {
        Self { engine }
    }

    /// Append an event with optimistic concurrency check; returns assigned seq.
    pub async fn append(
        &self,
        route: String,
        id: Option<String>,
        body: Vec<u8>,
        metadata: Option<Vec<u8>>,
        expected: ExpectedRevision,
    ) -> Result<u64, String> {
        self.engine
            .stream_append_old(route, id, body, metadata, expected)
            .await
    }

    /// Peek N events starting from a given sequence (inclusive). Returns (seq, body).
    pub async fn peek(
        &self,
        route: String,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, String> {
        self.engine.stream_peek_old(route, from_seq, limit).await
    }

    /// Consume hierarchically over a prefix route; returns (route, seq, body) records.
    pub async fn consume_prefix(
        &self,
        prefix: String,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(String, u64, Vec<u8>)>, String> {
        self.engine
            .stream_consume_prefix(prefix, from_seq, limit)
            .await
    }
}

pub use crate::storage::mem::ExpectedRevision as StreamExpectedRevision;
