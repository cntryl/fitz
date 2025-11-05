// Refactored Engine: simple dispatcher that routes to domain handlers
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::core::domain::{Domain, DomainRequest, DomainResponse};
use crate::core::router::Router;
use crate::protocol::route::parse_route;

// Keep the subscription sender type for compatibility
pub type SubSender = mpsc::Sender<(
    String,
    Option<String>,
    Vec<u8>,
    Option<String>,
    Option<u32>,
    bool,
)>;

/// Simplified engine command - just dispatch to domain
#[derive(Debug)]
pub enum EngineCommand {
    /// Dispatch a request to the appropriate domain handler
    Dispatch {
        route: String,
        payload: Vec<u8>,
        channel_id: u32,
        resp: oneshot::Sender<Result<Vec<u8>, String>>,
    },
    
    /// Subscribe to a route (for pub/sub) - routed to domain
    Subscribe {
        route: String,
        sender: SubSender,
        channel_id: u32,
        resp: oneshot::Sender<Result<u64, String>>,
    },
    
    /// Unsubscribe from a route - routed to domain
    Unsubscribe {
        id: u64,
        resp: oneshot::Sender<Result<(), String>>,
    },
    
    /// Cleanup channel subscriptions - routed to domain
    CleanupChannel {
        channel_id: u32,
        resp: oneshot::Sender<Result<(), String>>,
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
    
    /// Dispatch a request to the appropriate domain
    pub async fn dispatch(
        &self,
        route: String,
        payload: Vec<u8>,
        channel_id: u32,
    ) -> Result<Vec<u8>, String> {
        let (tx, rx) = oneshot::channel();
        let cmd = EngineCommand::Dispatch {
            route,
            payload,
            channel_id,
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
        let cmd = EngineCommand::CleanupChannel { channel_id, resp: tx };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| "engine stopped".to_string())?;
        rx.await.map_err(|_| "no response".to_string())?
    }
    
    // ========================================================================
    // BACKWARD COMPATIBILITY METHODS
    // These methods build TLV payloads and call dispatch()
    // Eventually these will be removed when all callers use dispatch directly
    // ========================================================================
    
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
        use crate::protocol::frame::build_tlv;
        use crate::protocol::tags::*;
        
        let mut payload = Vec::new();
        build_tlv(TAG_ID, id.as_bytes(), &mut payload);
        build_tlv(TAG_BODY, &body, &mut payload);
        
        if let Some(reply) = reply_to {
            build_tlv(TAG_ROUTE_REPLY, reply.as_bytes(), &mut payload);
        }
        if let Some(s) = seq {
            build_tlv(TAG_SEQ, &s.to_be_bytes(), &mut payload);
        }
        if end {
            build_tlv(TAG_STREAM_END, &[], &mut payload);
        }
        if let Some(ttl) = ttl_secs {
            build_tlv(TAG_TTL_SECS, &ttl.to_be_bytes(), &mut payload);
        }
        
        self.dispatch(route, payload, 0).await?;
        Ok(())
    }
    
    pub async fn reserve(
        &self,
        route: String,
        lease_secs: u32,
    ) -> Result<(String, Vec<u8>, String), String> {
        use crate::protocol::frame::{build_tlv, find_tlv};
        use crate::protocol::tags::*;
        
        let mut payload = Vec::new();
        build_tlv(TAG_LEASE, &lease_secs.to_be_bytes(), &mut payload);
        
        let response = self.dispatch(route, payload, 0).await?;
        
        // Parse response TLVs
        let id = find_tlv(&response, TAG_ID)
            .and_then(|b| std::str::from_utf8(b).ok())
            .ok_or("missing TAG_ID in response")?
            .to_string();
        let body = find_tlv(&response, TAG_BODY)
            .ok_or("missing TAG_BODY in response")?
            .to_vec();
        let token = find_tlv(&response, TAG_DELIVERY_TOKEN)
            .and_then(|b| std::str::from_utf8(b).ok())
            .ok_or("missing TAG_DELIVERY_TOKEN in response")?
            .to_string();
        
        Ok((id, body, token))
    }
    
    pub async fn extend_lease(
        &self,
        route: String,
        id: String,
        token: String,
        add_secs: u32,
    ) -> Result<u32, String> {
        use crate::protocol::frame::{build_tlv, find_tlv};
        use crate::protocol::tags::*;
        
        let mut payload = Vec::new();
        build_tlv(TAG_ID, id.as_bytes(), &mut payload);
        build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut payload);
        build_tlv(TAG_LEASE, &add_secs.to_be_bytes(), &mut payload);
        
        let response = self.dispatch(route, payload, 0).await?;
        
        // Parse remaining seconds from response
        let remaining = find_tlv(&response, TAG_LEASE)
            .and_then(|b| {
                if b.len() == 4 {
                    Some(u32::from_be_bytes(b.try_into().ok()?))
                } else {
                    None
                }
            })
            .ok_or("missing TAG_LEASE in response")?;
        
        Ok(remaining)
    }
    
    pub async fn peek(&self, route: String) -> Result<Option<(String, Vec<u8>)>, String> {
        use crate::protocol::frame::find_tlv;
        use crate::protocol::tags::*;
        
        let payload = Vec::new(); // Empty payload for peek
        let response = self.dispatch(route, payload, 0).await?;
        
        if response.is_empty() {
            return Ok(None);
        }
        
        let id = find_tlv(&response, TAG_ID)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.to_string());
        let body = find_tlv(&response, TAG_BODY).map(|b| b.to_vec());
        
        match (id, body) {
            (Some(id), Some(body)) => Ok(Some((id, body))),
            _ => Ok(None),
        }
    }
    
    pub async fn consume(&self, route: String, id: String, token: String) -> Result<(), String> {
        use crate::protocol::frame::build_tlv;
        use crate::protocol::tags::*;
        
        let mut payload = Vec::new();
        build_tlv(TAG_ID, id.as_bytes(), &mut payload);
        build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut payload);
        
        self.dispatch(route, payload, 0).await?;
        Ok(())
    }
    
    pub async fn stream_append_old(
        &self,
        route: String,
        id: Option<String>,
        body: Vec<u8>,
        _metadata: Option<Vec<u8>>,
        _expected: crate::core::stream::ExpectedRevision,
    ) -> Result<u64, String> {
        use crate::protocol::frame::{build_tlv, find_tlv};
        use crate::protocol::tags::*;
        
        let mut payload = Vec::new();
        if let Some(id) = id {
            build_tlv(TAG_ID, id.as_bytes(), &mut payload);
        }
        build_tlv(TAG_BODY, &body, &mut payload);
        // TODO: Add metadata and expected revision support
        
        let response = self.dispatch(route, payload, 0).await?;
        
        // Parse sequence number from response
        let seq = find_tlv(&response, TAG_SEQ)
            .and_then(|b| {
                if b.len() == 8 {
                    Some(u64::from_be_bytes(b.try_into().ok()?))
                } else {
                    None
                }
            })
            .ok_or("missing TAG_SEQ in response")?;
        
        Ok(seq)
    }
}

/// Start the engine task with domain handlers
pub fn start_engine() -> EngineHandle {
    let (_jh, handle) = start_engine_with_join();
    handle
}

pub fn start_engine_with_join() -> (JoinHandle<()>, EngineHandle) {
    let (tx, mut rx) = mpsc::channel::<EngineCommand>(1024);
    let handle = EngineHandle::new(tx.clone());

    // Create domain handlers as Arc for shared ownership
    let mut domains: HashMap<&'static str, Arc<dyn Domain>> = HashMap::new();
    
    // Create a mock KV store for domains that need storage
    // TODO: Replace with proper storage backend
    use crate::storage::traits::{KvStore, KvTransaction};
    use bytes::Bytes;
    
    #[derive(Clone)]
    struct MockStore;
    impl KvStore for MockStore {
        fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), String> { Ok(()) }
        fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, String> { Ok(None) }
        fn delete(&self, _key: &[u8]) -> Result<(), String> { Ok(()) }
        fn put_batch(&self, _writes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> { Ok(()) }
        fn delete_batch(&self, _keys: Vec<Vec<u8>>) -> Result<(), String> { Ok(()) }
        fn scan(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<(Bytes, Bytes)>, String> { Ok(vec![]) }
        fn flush(&self) -> Result<(), String> { Ok(()) }
        fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>, String> {
            Err("Transactions not supported in mock".to_string())
        }
    }
    let kv_store = Arc::new(MockStore) as Arc<dyn KvStore>;
    
    // Register all domains
    use crate::core::{control::ControlDomain, kv::KvDomain, lease::LeaseDomain, 
                      notice::NoticeDomain, queue::QueueDomain, rpc::RpcDomain, 
                      stream::StreamDomain};
    
    // Queue domain
    domains.insert("queue", Arc::new(QueueDomain::new()));
    
    // KV domain - needs storage
    domains.insert("kv", Arc::new(KvDomain::new(Arc::clone(&kv_store))));
    
    // Stream domain - needs storage
    domains.insert("stream", Arc::new(StreamDomain::new(Arc::clone(&kv_store))));
    
    // Lease domain
    domains.insert("lease", Arc::new(LeaseDomain::new()));
    
    // Notice domain - keep separate typed reference for subscription routing
    let notice_domain = Arc::new(NoticeDomain::new());
    domains.insert("notice", Arc::clone(&notice_domain) as Arc<dyn Domain>);
    
    // Control domain - shares notice service for pub/sub
    let control_domain = Arc::new(ControlDomain::with_notice_service(notice_domain.get_service()));
    domains.insert("control", Arc::clone(&control_domain) as Arc<dyn Domain>);
    
    // RPC domain
    domains.insert("rpc", Arc::new(RpcDomain::new()));

    let jh = tokio::spawn(async move {
        let mut router = Router::new();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                EngineCommand::Dispatch {
                    route,
                    payload,
                    channel_id,
                    resp,
                } => {
                    // Parse route to determine domain
                    let parsed = match parse_route(&route) {
                        Ok(r) => r,
                        Err(e) => {
                            let _ = resp.send(Err(format!("invalid route: {}", e)));
                            continue;
                        }
                    };
                    
                    // Get scheme string
                    let scheme_str = parsed.scheme.as_str();
                    
                    // Find domain handler
                    let domain = match domains.get(scheme_str) {
                        Some(d) => d,
                        None => {
                            let _ = resp.send(Err(format!("unsupported scheme: {}", scheme_str)));
                            continue;
                        }
                    };
                    
                    // Create domain request
                    let request = DomainRequest {
                        route: parsed.clone(),
                        route_str: route.clone(),
                        payload: payload.clone(),
                        channel_id,
                    };
                    
                    // Dispatch to domain
                    let response = domain.handle(request).await;
                    
                    // Convert domain response to bytes and send response
                    match response {
                        DomainResponse::Ok => {
                            let _ = resp.send(Ok(Vec::new()));
                        }
                        DomainResponse::Frame(data) => {
                            let _ = resp.send(Ok(data));
                        }
                        DomainResponse::Error(e) => {
                            let _ = resp.send(Err(e));
                        }
                    }
                }

                EngineCommand::Subscribe {
                    route,
                    sender,
                    channel_id,
                    resp,
                } => {
                    // Parse route to determine which domain handles it
                    let scheme_str = if let Some(scheme_end) = route.find("://") {
                        &route[..scheme_end]
                    } else {
                        "notice" // default to notice for bare routes
                    };
                    
                    // Control routes use notice domain for pub/sub
                    let lookup_scheme = if scheme_str == "control" {
                        "notice"
                    } else {
                        scheme_str
                    };
                    
                    // Find domain and delegate subscription
                    if let Some(domain) = domains.get(lookup_scheme) {
                        let result = domain.subscribe(route, channel_id, sender).await;
                        let _ = resp.send(result);
                    } else {
                        // Fall back to legacy router for unknown schemes
                        let id = router.subscribe(route, channel_id, sender);
                        let _ = resp.send(Ok(id));
                    }
                }

                EngineCommand::Unsubscribe { id, resp } => {
                    // Try each domain until one successfully unsubscribes
                    // Domains that don't support subscriptions will return false
                    let mut removed = false;
                    for (_, domain) in &domains {
                        if domain.unsubscribe(id).await {
                            removed = true;
                            break;
                        }
                    }
                    
                    // Fall back to legacy router if not handled by any domain
                    if !removed {
                        router.unsubscribe(id);
                    }
                    let _ = resp.send(Ok(()));
                }

                EngineCommand::CleanupChannel { channel_id, resp } => {
                    // Cleanup in all domains that support it
                    for (_, domain) in &domains {
                        domain.cleanup_channel(channel_id).await;
                    }
                    
                    // Also cleanup legacy router
                    router.cleanup_channel(channel_id);
                    let _ = resp.send(Ok(()));
                }
            }
        }
    });

    (jh, handle)
}
