//! MidgeMsg messages.
//!
//! MidgeActor is the ONLY bridge to durable storage.
//! Handles streams, queues, KV, and optionally metrics.

use crate::actor::ActorRef;

/// Messages for MidgeActor.
#[derive(Debug)]
pub enum MidgeMsg {
    // ===== STREAM OPERATIONS =====
    /// Append to a stream.
    AppendStream {
        realm: String,
        area: String,
        stream_name: String,
        payload: Vec<u8>,
        reply_to: Option<ActorRef<AppendStreamReply>>,
    },

    /// Read from a stream.
    ReadStream {
        realm: String,
        area: String,
        stream_name: String,
        from_offset: u64,
        max_count: usize,
        reply_to: ActorRef<ReadStreamReply>,
    },

    // ===== QUEUE OPERATIONS =====
    /// Enqueue a message.
    Enqueue {
        realm: String,
        area: String,
        queue_name: String,
        payload: Vec<u8>,
        reply_to: Option<ActorRef<EnqueueReply>>,
    },

    /// Dequeue a message.
    Dequeue {
        realm: String,
        area: String,
        queue_name: String,
        reply_to: ActorRef<DequeueReply>,
    },

    /// Acknowledge (delete) a message.
    Ack {
        realm: String,
        area: String,
        queue_name: String,
        message_id: String,
        reply_to: Option<ActorRef<AckReply>>,
    },

    // ===== KV OPERATIONS =====
    /// Put a key-value pair.
    KvPut {
        realm: String,
        area: String,
        key: Vec<u8>,
        value: Vec<u8>,
        reply_to: Option<ActorRef<KvPutReply>>,
    },

    /// Get a value by key.
    KvGet {
        realm: String,
        area: String,
        key: Vec<u8>,
        reply_to: ActorRef<KvGetReply>,
    },

    /// Delete a key.
    KvDelete {
        realm: String,
        area: String,
        key: Vec<u8>,
        reply_to: Option<ActorRef<KvDeleteReply>>,
    },

    // ===== METRICS (OPTIONAL) =====
    /// Flush metrics to storage.
    FlushMetrics {
        realm: String,
        metrics: Vec<MetricSnapshot>,
    },
}

// ===== REPLY TYPES =====

#[derive(Debug)]
pub struct AppendStreamReply {
    pub offset: u64,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct ReadStreamReply {
    pub records: Vec<StreamRecord>,
}

#[derive(Debug)]
pub struct StreamRecord {
    pub offset: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct EnqueueReply {
    pub message_id: String,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct DequeueReply {
    pub message_id: Option<String>,
    pub payload: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct AckReply {
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct KvPutReply {
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct KvGetReply {
    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct KvDeleteReply {
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MetricSnapshot {
    pub name: String,
    pub metric_type: MetricType,
    pub value: MetricValue,
}

#[derive(Debug, Clone)]
pub enum MetricType {
    Counter,
    Histogram,
}

#[derive(Debug, Clone)]
pub enum MetricValue {
    Counter(i64),
    Histogram {
        count: usize,
        min: f64,
        max: f64,
        sum: f64,
    },
}

