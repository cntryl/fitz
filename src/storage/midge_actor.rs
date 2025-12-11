//! The sole actor responsible for Midge integration.
//!
//! All durable operations (streams, queues, KV) are mediated through this actor.
//! This ensures:
//! - No blocking on I/O in hot-path actors
//! - Clean separation of concerns
//! - Async persistence without async domain logic

use crate::actor::{Actor, ActorContext};
use crate::messages::midge::{
    MidgeMsg, AppendStreamReply, ReadStreamReply, StreamRecord,
    EnqueueReply, DequeueReply, AckReply,
    KvPutReply, KvGetReply, KvDeleteReply, MetricSnapshot,
};
use std::collections::HashMap;

/// MidgeActor is the ONLY bridge to durable storage.
///
/// Durability boundary:
/// - Streams (durable)
/// - Queues (durable)
/// - KV (durable)
/// - Metrics (optionally durable)
///
/// Everything else is ephemeral.
pub struct MidgeActor {
    /// TODO: Replace with real Midge connection
    _placeholder: HashMap<String, Vec<u8>>,
}

impl MidgeActor {
    /// Create a new MidgeActor.
    pub fn new() -> Self {
        Self {
            _placeholder: HashMap::new(),
        }
    }

    /// Handle stream append operation.
    fn handle_append_stream(
        &mut self,
        realm: String,
        area: String,
        stream_name: String,
        payload: Vec<u8>,
        reply_to: Option<crate::actor::ActorRef<AppendStreamReply>>,
    ) {
        // TODO: Implement real Midge append
        let offset = 0u64; // placeholder
        if let Some(reply) = reply_to {
            let _ = reply.tell(AppendStreamReply {
                offset,
                success: true,
                error: None,
            });
        }
    }

    /// Handle stream read operation.
    fn handle_read_stream(
        &mut self,
        realm: String,
        area: String,
        stream_name: String,
        from_offset: u64,
        max_count: usize,
        reply_to: crate::actor::ActorRef<ReadStreamReply>,
    ) {
        // TODO: Implement real Midge read
        let _ = reply_to.tell(ReadStreamReply { records: vec![] });
    }

    /// Handle queue enqueue operation.
    fn handle_enqueue(
        &mut self,
        realm: String,
        area: String,
        queue_name: String,
        payload: Vec<u8>,
        reply_to: Option<crate::actor::ActorRef<EnqueueReply>>,
    ) {
        // TODO: Implement real Midge enqueue
        if let Some(reply) = reply_to {
            let _ = reply.tell(EnqueueReply {
                message_id: "msg-placeholder".to_string(),
                success: true,
                error: None,
            });
        }
    }

    /// Handle queue dequeue operation.
    fn handle_dequeue(
        &mut self,
        realm: String,
        area: String,
        queue_name: String,
        reply_to: crate::actor::ActorRef<DequeueReply>,
    ) {
        // TODO: Implement real Midge dequeue
        let _ = reply_to.tell(DequeueReply {
            message_id: None,
            payload: None,
        });
    }

    /// Handle queue ack operation.
    fn handle_ack(
        &mut self,
        realm: String,
        area: String,
        queue_name: String,
        message_id: String,
        reply_to: Option<crate::actor::ActorRef<AckReply>>,
    ) {
        // TODO: Implement real Midge ack
        if let Some(reply) = reply_to {
            let _ = reply.tell(AckReply {
                success: true,
                error: None,
            });
        }
    }

    /// Handle KV put operation.
    fn handle_kv_put(
        &mut self,
        realm: String,
        area: String,
        key: Vec<u8>,
        value: Vec<u8>,
        reply_to: Option<crate::actor::ActorRef<KvPutReply>>,
    ) {
        // TODO: Implement real Midge KV put
        if let Some(reply) = reply_to {
            let _ = reply.tell(KvPutReply {
                success: true,
                error: None,
            });
        }
    }

    /// Handle KV get operation.
    fn handle_kv_get(
        &mut self,
        realm: String,
        area: String,
        key: Vec<u8>,
        reply_to: crate::actor::ActorRef<KvGetReply>,
    ) {
        // TODO: Implement real Midge KV get
        let _ = reply_to.tell(KvGetReply { value: None });
    }

    /// Handle KV delete operation.
    fn handle_kv_delete(
        &mut self,
        realm: String,
        area: String,
        key: Vec<u8>,
        reply_to: Option<crate::actor::ActorRef<KvDeleteReply>>,
    ) {
        // TODO: Implement real Midge KV delete
        if let Some(reply) = reply_to {
            let _ = reply.tell(KvDeleteReply {
                success: true,
                error: None,
            });
        }
    }

    /// Handle metrics flush operation.
    fn handle_flush_metrics(&mut self, realm: String, metrics: Vec<MetricSnapshot>) {
        // TODO: Implement metrics flush to Midge
    }
}

impl Actor for MidgeActor {
    type Message = MidgeMsg;

    fn on_message(&mut self, msg: Self::Message, _ctx: &mut ActorContext<Self::Message>) {
        match msg {
            MidgeMsg::AppendStream {
                realm,
                area,
                stream_name,
                payload,
                reply_to,
            } => self.handle_append_stream(realm, area, stream_name, payload, reply_to),

            MidgeMsg::ReadStream {
                realm,
                area,
                stream_name,
                from_offset,
                max_count,
                reply_to,
            } => self.handle_read_stream(realm, area, stream_name, from_offset, max_count, reply_to),

            MidgeMsg::Enqueue {
                realm,
                area,
                queue_name,
                payload,
                reply_to,
            } => self.handle_enqueue(realm, area, queue_name, payload, reply_to),

            MidgeMsg::Dequeue {
                realm,
                area,
                queue_name,
                reply_to,
            } => self.handle_dequeue(realm, area, queue_name, reply_to),

            MidgeMsg::Ack {
                realm,
                area,
                queue_name,
                message_id,
                reply_to,
            } => self.handle_ack(realm, area, queue_name, message_id, reply_to),

            MidgeMsg::KvPut {
                realm,
                area,
                key,
                value,
                reply_to,
            } => self.handle_kv_put(realm, area, key, value, reply_to),

            MidgeMsg::KvGet {
                realm,
                area,
                key,
                reply_to,
            } => self.handle_kv_get(realm, area, key, reply_to),

            MidgeMsg::KvDelete {
                realm,
                area,
                key,
                reply_to,
            } => self.handle_kv_delete(realm, area, key, reply_to),

            MidgeMsg::FlushMetrics { realm, metrics } => {
                self.handle_flush_metrics(realm, metrics)
            }
        }
    }

    fn on_start(&mut self, _ctx: &mut ActorContext<Self::Message>) {
        // TODO: Initialize Midge connection
    }

    fn on_stop(&mut self) {
        // TODO: Close Midge connection gracefully
    }
}

impl Default for MidgeActor {
    fn default() -> Self {
        Self::new()
    }
}
