// Engine: single task that serializes store access and manages subscriptions
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::core::router::Router;
use crate::storage::mem::{
    ExpectedRevision as StreamExpectedRevision, MemStore, QueueConfig, QueueScope,
};
use tokio::task::JoinHandle;

/// Notification sender type used by transports to receive (route, id, body, reply_to, seq, end)
pub type SubSender = mpsc::Sender<(
    String,
    Option<String>,
    Vec<u8>,
    Option<String>,
    Option<u32>,
    bool,
)>;

// Response channel type aliases
type RespUnit = oneshot::Sender<Result<(), String>>;
type RespStr = oneshot::Sender<Result<String, String>>;
type RespVecStr = oneshot::Sender<Result<Vec<String>, String>>;
type RespOptIdBody = oneshot::Sender<Result<Option<(String, Vec<u8>)>, String>>;
type RespReserve = oneshot::Sender<Result<(String, Vec<u8>, String), String>>;
type RespU32 = oneshot::Sender<Result<u32, String>>;
type RespStreamPeek = oneshot::Sender<Result<Vec<(u64, Vec<u8>)>, String>>;
type RespStreamConsume = oneshot::Sender<Result<Vec<(String, u64, Vec<u8>)>, String>>;

#[derive(Debug)]
pub enum EngineCommand {
    Publish {
        route: String,
        id: String,
        body: Vec<u8>,
        reply_to: Option<String>,
        seq: Option<u32>,
        end: bool,
        ttl_secs: Option<u64>,
        resp: RespUnit,
    },
    Reserve {
        route: String,
        lease_secs: u32,
        resp: RespReserve,
    },
    ExtendLease {
        route: String,
        id: String,
        token: String,
        add_secs: u32,
        resp: RespU32,
    },
    Peek {
        route: String,
        resp: RespOptIdBody,
    },
    Consume {
        route: String,
        id: String,
        token: String,
        resp: RespUnit,
    },
    ListResources {
        route: String,
        resp: RespVecStr,
    },
    ListAreas {
        resp: RespVecStr,
    },
    FetchStatus {
        resp: RespStr,
    },
    FetchResourceStatus {
        resource: String,
        resp: RespStr,
    },
    Subscribe {
        route: String,
        sender: SubSender,
        channel_id: u32,
        resp: oneshot::Sender<Result<u64, String>>,
    },
    Unsubscribe {
        id: u64,
        resp: oneshot::Sender<Result<(), String>>,
    },
    CleanupChannel {
        channel_id: u32,
        resp: oneshot::Sender<Result<(), String>>,
    },
    StreamAppend {
        route: String,
        id: Option<String>,
        body: Vec<u8>,
        metadata: Option<Vec<u8>>,
        expected: StreamExpectedRevision,
        resp: oneshot::Sender<Result<u64, String>>,
    },
    StreamPeek {
        route: String,
        from_seq: u64,
        limit: usize,
        resp: RespStreamPeek,
    },
    StreamConsume {
        prefix: String,
        from_seq: u64,
        limit: usize,
        resp: RespStreamConsume,
    },
    SetQueueConfig {
        scope: QueueScope,
        cfg: QueueConfig,
        resp: RespUnit,
    },
    KvPut {
        route: String,
        key: String,
        value: Vec<u8>,
        resp: RespUnit,
    },
    KvGet {
        route: String,
        key: String,
        resp: oneshot::Sender<Result<Option<Vec<u8>>, String>>,
    },
    KvDelete {
        route: String,
        key: String,
        resp: RespUnit,
    },
    KvScanGe {
        route: String,
        start_key: String,
        limit: usize,
        resp: oneshot::Sender<Result<Vec<(String, Vec<u8>)>, String>>,
    },
    KvPutBatch {
        route: String,
        items: Vec<(String, Vec<u8>)>,
        resp: RespUnit,
    },
    KvGetBatch {
        route: String,
        keys: Vec<String>,
        resp: oneshot::Sender<Result<Vec<(String, Option<Vec<u8>>)>, String>>,
    },
    KvDeleteRange {
        route: String,
        start_key: String,
        end_key: String,
        resp: oneshot::Sender<Result<u64, String>>,
    },
}

#[derive(Clone, Debug)]
pub struct EngineHandle {
    tx: mpsc::Sender<EngineCommand>,
}

impl EngineHandle {
    pub fn new(tx: mpsc::Sender<EngineCommand>) -> Self {
        Self { tx }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn publish(
        &self,
        route: String,
        id: String,
        body: Vec<u8>,
        reply_to: Option<String>,
        seq: Option<u32>,
        end: bool,
        ttl_secs: Option<u64>,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::Publish {
            route,
            id,
            body,
            reply_to,
            seq,
            end,
            ttl_secs,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn reserve(
        &self,
        route: String,
        lease_secs: u32,
    ) -> Result<(String, Vec<u8>, String), String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::Reserve {
            route,
            lease_secs,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn extend_lease(
        &self,
        route: String,
        id: String,
        token: String,
        add_secs: u32,
    ) -> Result<u32, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::ExtendLease {
            route,
            id,
            token,
            add_secs,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn peek(&self, route: String) -> Result<Option<(String, Vec<u8>)>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::Peek { route, resp: tx };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn consume(&self, route: String, id: String, token: String) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::Consume {
            route,
            id,
            token,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn subscribe(
        &self,
        route: String,
        sender: SubSender,
        channel_id: u32,
    ) -> Result<u64, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::Subscribe {
            route,
            sender,
            channel_id,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn unsubscribe(&self, id: u64) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::Unsubscribe { id, resp: tx };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn cleanup_channel(&self, channel_id: u32) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::CleanupChannel {
            channel_id,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn set_queue_config(
        &self,
        scope: QueueScope,
        cfg: QueueConfig,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::SetQueueConfig {
            scope,
            cfg,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn stream_append(
        &self,
        route: String,
        id: Option<String>,
        body: Vec<u8>,
        metadata: Option<Vec<u8>>,
        expected: StreamExpectedRevision,
    ) -> Result<u64, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::StreamAppend {
            route,
            id,
            body,
            metadata,
            expected,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn stream_peek(
        &self,
        route: String,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::StreamPeek {
            route,
            from_seq,
            limit,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn stream_consume_prefix(
        &self,
        prefix: String,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(String, u64, Vec<u8>)>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::StreamConsume {
            prefix,
            from_seq,
            limit,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    // Admin / introspection helpers
    pub async fn list_resources(&self, route: String) -> Result<Vec<String>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::ListResources { route, resp: tx };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn list_areas(&self) -> Result<Vec<String>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::ListAreas { resp: tx };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn fetch_status(&self) -> Result<String, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::FetchStatus { resp: tx };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn fetch_resource_status(&self, resource: String) -> Result<String, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::FetchResourceStatus { resource, resp: tx };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    // KV operations
    pub async fn kv_put(&self, route: String, key: String, value: Vec<u8>) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::KvPut {
            route,
            key,
            value,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn kv_get(&self, route: String, key: String) -> Result<Option<Vec<u8>>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::KvGet {
            route,
            key,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn kv_delete(&self, route: String, key: String) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::KvDelete {
            route,
            key,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn kv_scan_ge(
        &self,
        route: String,
        start_key: String,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::KvScanGe {
            route,
            start_key,
            limit,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn kv_put_batch(
        &self,
        route: String,
        items: Vec<(String, Vec<u8>)>,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::KvPutBatch {
            route,
            items,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn kv_get_batch(
        &self,
        route: String,
        keys: Vec<String>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::KvGetBatch {
            route,
            keys,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }

    pub async fn kv_delete_range(
        &self,
        route: String,
        start_key: String,
        end_key: String,
    ) -> Result<u64, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::KvDeleteRange {
            route,
            start_key,
            end_key,
            resp: tx,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }
}

pub fn start_engine(store: Arc<Mutex<MemStore>>) -> EngineHandle {
    // Delegate to the joinable variant and drop the JoinHandle
    let (handle, _jh) = start_engine_with_join(store);
    handle
}

/// Variant of `start_engine` that returns both an `EngineHandle` and the `JoinHandle` of the
/// spawned engine task so tests can await shutdown.
pub fn start_engine_with_join(store: Arc<Mutex<MemStore>>) -> (EngineHandle, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<EngineCommand>(1024);
    let handle = EngineHandle::new(tx.clone());

    let jh = tokio::spawn(async move {
        let mut router = Router::new();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                EngineCommand::Publish {
                    route,
                    id,
                    body,
                    reply_to,
                    seq,
                    end,
                    ttl_secs: _ttl,
                    resp,
                } => {
                    let mut s = store.lock().await;
                    let _ = s.append(route.clone(), id.clone(), body.clone()).await;
                    let (_delivered, _removed) =
                        router.dispatch(&route, Some(&id), &body, reply_to.as_deref(), seq, end);
                    let _ = resp.send(Ok(()));
                }

                EngineCommand::Reserve {
                    route,
                    lease_secs,
                    resp,
                } => {
                    let mut s = store.lock().await;
                    let _ = resp.send(s.reserve_next(&route, lease_secs).await);
                }

                EngineCommand::ExtendLease {
                    route,
                    id,
                    token,
                    add_secs,
                    resp,
                } => {
                    let mut s = store.lock().await;
                    let _ = resp.send(s.extend_lease(&route, &id, &token, add_secs).await);
                }

                EngineCommand::Peek { route, resp } => {
                    let s = store.lock().await;
                    let res = s.peek_next(&route).await;
                    let _ = resp.send(Ok(res));
                }

                EngineCommand::Consume {
                    route,
                    id,
                    token,
                    resp,
                } => {
                    let mut s = store.lock().await;
                    let _ = resp.send(s.consume(&route, &id, &token).await.map(|_| ()));
                }

                EngineCommand::ListResources {
                    route: _route,
                    resp,
                } => {
                    let _ = resp.send(Err("not implemented".to_string()));
                }

                EngineCommand::ListAreas { resp } => {
                    let _ = resp.send(Err("not implemented".to_string()));
                }

                EngineCommand::FetchStatus { resp } => {
                    let _ = resp.send(Ok("ok".to_string()));
                }

                EngineCommand::FetchResourceStatus { resource: _, resp } => {
                    let _ = resp.send(Err("not implemented".to_string()));
                }

                EngineCommand::Subscribe {
                    route,
                    sender,
                    channel_id,
                    resp,
                } => {
                    let id = router.subscribe(route, channel_id, sender);
                    let _ = resp.send(Ok(id));
                }

                EngineCommand::Unsubscribe { id, resp } => {
                    // reply unit regardless; router.unsubscribe performs best-effort cleanup
                    let _ = router.unsubscribe(id);
                    let _ = resp.send(Ok(()));
                }

                EngineCommand::CleanupChannel { channel_id, resp } => {
                    router.cleanup_channel(channel_id);
                    let _ = resp.send(Ok(()));
                }

                EngineCommand::StreamAppend {
                    route,
                    id,
                    body,
                    metadata,
                    expected,
                    resp,
                } => {
                    let s = store.lock().await;
                    match s
                        .stream_append_with_expected(&route, id, body, metadata, expected)
                        .await
                    {
                        Ok(seq) => {
                            let _ = resp.send(Ok(seq));
                        }
                        Err(e) => {
                            let _ = resp.send(Err(format!("stream error: {:?}", e)));
                        }
                    }
                }

                EngineCommand::StreamPeek {
                    route,
                    from_seq,
                    limit,
                    resp,
                } => {
                    let s = store.lock().await;
                    let events = s.stream_peek(&route, from_seq, limit).await;
                    let out = events.into_iter().map(|e| (e.seq, e.body)).collect();
                    let _ = resp.send(Ok(out));
                }

                EngineCommand::StreamConsume {
                    prefix,
                    from_seq,
                    limit,
                    resp,
                } => {
                    let s = store.lock().await;
                    let out = s.stream_consume_prefix(&prefix, from_seq, limit).await;
                    let _ = resp.send(Ok(out));
                }

                EngineCommand::SetQueueConfig { scope, cfg, resp } => {
                    store.lock().await.set_queue_config(scope, cfg).await;
                    let _ = resp.send(Ok(()));
                }

                EngineCommand::KvPut {
                    route,
                    key,
                    value,
                    resp,
                } => {
                    let s = store.lock().await;
                    let res = s.kv_put(&route, &key, value).await;
                    let _ = resp.send(res);
                }

                EngineCommand::KvGet { route, key, resp } => {
                    let s = store.lock().await;
                    let res = s.kv_get(&route, &key).await;
                    let _ = resp.send(res);
                }

                EngineCommand::KvDelete { route, key, resp } => {
                    let s = store.lock().await;
                    let res = s.kv_delete(&route, &key).await;
                    let _ = resp.send(res);
                }

                EngineCommand::KvScanGe {
                    route,
                    start_key,
                    limit,
                    resp,
                } => {
                    let s = store.lock().await;
                    let res = s.kv_scan_ge(&route, &start_key, limit).await;
                    let _ = resp.send(res);
                }

                EngineCommand::KvPutBatch { route, items, resp } => {
                    let s = store.lock().await;
                    let res = s.kv_put_batch(&route, items).await;
                    let _ = resp.send(res);
                }

                EngineCommand::KvGetBatch { route, keys, resp } => {
                    let s = store.lock().await;
                    let res = s.kv_get_batch(&route, keys).await;
                    let _ = resp.send(res);
                }

                EngineCommand::KvDeleteRange {
                    route,
                    start_key,
                    end_key,
                    resp,
                } => {
                    let s = store.lock().await;
                    let res = s.kv_delete_range(&route, &start_key, &end_key).await;
                    let _ = resp.send(res);
                }
            }
        }
    });

    (handle, jh)
}

// Inline unit tests for EngineHandle and the engine task
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn should_publish_and_notify_subscriber() {
        // Arrange
        let store = Arc::new(Mutex::new(MemStore::new()));
        let handle = start_engine(store.clone());
        let (tx, mut rx) = mpsc::channel::<(
            String,
            Option<String>,
            Vec<u8>,
            Option<String>,
            Option<u32>,
            bool,
        )>(4);
        // subscribe is setup for the publish behaviour
        let sub_id = handle
            .subscribe("route/x".to_string(), tx, 1)
            .await
            .expect("subscribe failed");

        // Act
        let _ = handle
            .publish(
                "route/x".to_string(),
                "mid-1".to_string(),
                b"payload".to_vec(),
                None,
                Some(1),
                false,
                None,
            )
            .await
            .expect("publish failed");

        // Assert
        // receive with timeout to avoid hanging the test
        let got = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("recv timed out")
            .expect("channel closed");
        let (route, mid, body, _reply, seq, end) = got;
        assert_eq!(route, "route/x");
        assert_eq!(mid.unwrap(), "mid-1");
        assert_eq!(body, b"payload".to_vec());
        assert_eq!(seq, Some(1));
        assert!(!end);

        // cleanup: unsubscribe should succeed (cleanup belongs with Assert/teardown)
        let _ = handle
            .unsubscribe(sub_id)
            .await
            .expect("unsubscribe failed");
    }

    #[tokio::test]
    async fn should_reserve_returns_message() {
        // Arrange
        let store = Arc::new(Mutex::new(MemStore::new()));
        let handle = start_engine(store.clone());
        let route = "queue://r/a/res".to_string();

        // seed a message directly into the store
        {
            let mut s = store.lock().await;
            s.append(route.clone(), "id-1".to_string(), b"hello".to_vec())
                .await
                .expect("append failed");
        }

        // Act
        let (id, body, token) = handle
            .reserve(route.clone(), 30)
            .await
            .expect("reserve failed");

        // Assert
        assert_eq!(id, "id-1");
        assert_eq!(body, b"hello".to_vec());
        assert!(!token.is_empty());
    }

    #[tokio::test]
    async fn should_extend_lease_updates_remaining() {
        // Arrange
        let store = Arc::new(Mutex::new(MemStore::new()));
        let handle = start_engine(store.clone());
        let route = "queue://r/a/res".to_string();

        // seed a message directly into the store
        {
            let mut s = store.lock().await;
            s.append(route.clone(), "id-2".to_string(), b"x".to_vec())
                .await
                .expect("append failed");
        }

        // Arrange (reserve is part of setup for extend)
        let (id, _body, token) = handle
            .reserve(route.clone(), 30)
            .await
            .expect("reserve failed");

        // Act
        let remaining = handle
            .extend_lease(route.clone(), id.clone(), token.clone(), 10)
            .await
            .expect("extend lease failed");

        // Assert
        assert!(remaining > 0);
    }

    #[tokio::test]
    async fn should_consume_removes_message() {
        // Arrange
        let store = Arc::new(Mutex::new(MemStore::new()));
        let handle = start_engine(store.clone());
        let route = "queue://r/a/res".to_string();

        // seed a message directly into the store
        {
            let mut s = store.lock().await;
            s.append(route.clone(), "id-3".to_string(), b"bye".to_vec())
                .await
                .expect("append failed");
        }

        // Arrange (reserve the message so we can exercise consume)
        let (id, _body, token) = handle
            .reserve(route.clone(), 30)
            .await
            .expect("reserve failed");

        // Act
        handle
            .consume(route.clone(), id.clone(), token.clone())
            .await
            .expect("consume failed");

        // Assert
        let remaining = {
            let s = store.lock().await;
            s.read_all(&route).await
        };
        assert!(remaining.is_empty());
    }
}
