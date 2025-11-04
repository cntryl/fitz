use crate::protocol::frame as fr;
use futures_util::{SinkExt, StreamExt};
use once_cell::sync::OnceCell;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tungstenite::Message;
use url::Url;

#[derive(Debug, Clone)]
pub enum ControlMode {
    SelfNode,
    Route(String),
}

#[derive(Debug)]
pub struct ControlConfig {
    pub mode: ControlMode,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
}

static CONFIG: OnceCell<Arc<ControlConfig>> = OnceCell::new();
static JWKS_CACHE: OnceCell<Arc<Mutex<HashMap<String, Value>>>> = OnceCell::new();
static ACCESS_TOKEN: OnceCell<Arc<Mutex<Option<String>>>> = OnceCell::new();

pub fn get() -> Option<Arc<ControlConfig>> {
    CONFIG.get().cloned()
}

pub fn init() {
    let ccfg = crate::config::load().control;
    let control_route = ccfg.route.clone();
    let client_id = ccfg.client_id.clone();
    let client_secret = ccfg.client_secret.clone();

    let mode = if control_route.to_lowercase() == "self" {
        ControlMode::SelfNode
    } else {
        ControlMode::Route(control_route.clone())
    };
    let cfg = ControlConfig {
        mode,
        client_id,
        client_secret,
    };
    CONFIG.set(Arc::new(cfg)).ok();
    JWKS_CACHE.set(Arc::new(Mutex::new(HashMap::new()))).ok();
    ACCESS_TOKEN.set(Arc::new(Mutex::new(None))).ok();

    if let Some(cfg) = get() {
        match &cfg.mode {
            ControlMode::Route(route) => {
                // Token acquisition task via broker-native FTZ over WebSocket
                let route_clone = route.clone();
                let cid = cfg.client_id.clone();
                let secret = cfg.client_secret.clone();
                let token_cell = ACCESS_TOKEN.get().unwrap().clone();
                tokio::spawn(async move {
                    loop {
                        // Expect route to be a ws:// or wss:// URL
                        let url = match Url::parse(&route_clone) {
                            Ok(u) => u,
                            Err(_) => {
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                continue;
                            }
                        };
                        if let Ok((mut ws, _)) = tokio_tungstenite::connect_async(url.clone()).await
                        {
                            // Create a temporary reply route (avoid rpc:// to preserve body delivery)
                            let ts = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis();
                            let reply_route = format!("control://client/token/reply/{}", ts);

                            // Send REG subscribe for reply_route
                            let mut reg_payload = Vec::new();
                            fr::build_tlv(fr::TAG_ROUTE, reply_route.as_bytes(), &mut reg_payload);
                            fr::build_tlv(fr::TAG_SUBSCRIBE, &[], &mut reg_payload);
                            let reg = fr::build_frame(fr::FRAME_REG, 0, 1, &reg_payload);
                            if ws.send(Message::Binary(reg)).await.is_err() {
                                let _ = ws.close(None).await;
                                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                                continue;
                            }

                            // Build PUB to control://sys/token/issue with client creds and reply route
                            let body = serde_json::json!({
                                "client_id": cid.clone().unwrap_or_default(),
                                "client_secret": secret.clone().unwrap_or_default(),
                            })
                            .to_string();
                            let mut pub_payload = Vec::new();
                            fr::build_tlv(
                                fr::TAG_ROUTE,
                                b"control://sys/token/issue",
                                &mut pub_payload,
                            );
                            fr::build_tlv(fr::TAG_ID, b"1", &mut pub_payload);
                            fr::build_tlv(fr::TAG_BODY, body.as_bytes(), &mut pub_payload);
                            fr::build_tlv(
                                fr::TAG_ROUTE_REPLY,
                                reply_route.as_bytes(),
                                &mut pub_payload,
                            );
                            let pub_frame = fr::build_frame(fr::FRAME_PUB, 0, 1, &pub_payload);
                            if ws.send(Message::Binary(pub_frame)).await.is_err() {
                                let _ = ws.close(None).await;
                                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                                continue;
                            }

                            // Await response on the reply route
                            let token_opt = loop {
                                match ws.next().await {
                                    Some(Ok(Message::Binary(bin))) => {
                                        if let Ok(parsed) = fr::parse_frame(&bin) {
                                            if parsed.header.frame_type == fr::FRAME_DAT {
                                                // Look for notification body for our reply route
                                                let route_b =
                                                    fr::find_tlv(parsed.payload, fr::TAG_ROUTE);
                                                if let Some(r) = route_b {
                                                    if String::from_utf8_lossy(r) != reply_route {
                                                        continue;
                                                    }
                                                }
                                                let body_b =
                                                    fr::find_tlv(parsed.payload, fr::TAG_BODY);
                                                if let Some(b) = body_b {
                                                    if let Ok(v) =
                                                        serde_json::from_slice::<serde_json::Value>(
                                                            b,
                                                        )
                                                    {
                                                        if let Some(tok) = v
                                                            .get("access_token")
                                                            .and_then(|t| t.as_str())
                                                        {
                                                            break Some(tok.to_string());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(_)) => { /* ignore */ }
                                    Some(Err(_)) | None => {
                                        break None;
                                    }
                                }
                            };

                            if let Some(tok) = token_opt {
                                let mut g = token_cell.lock().await;
                                *g = Some(tok);
                                let _ = ws.close(None).await;
                                break;
                            }
                            let _ = ws.close(None).await;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                });

                // Connect loop (health)
                let route_clone2 = route.clone();
                let cid2 = cfg.client_id.clone();
                let secret2 = cfg.client_secret.clone();
                tokio::spawn(async move {
                    let client = reqwest::Client::new();
                    loop {
                        let mut req = client.get(format!("{}/health", route_clone2));
                        if let Some(tc) = ACCESS_TOKEN.get() {
                            if let Some(tok) = tc.lock().await.clone() {
                                req = req.bearer_auth(tok);
                            } else if let Some(id) = &cid2 {
                                req = req.basic_auth(id.clone(), secret2.clone());
                            }
                        }
                        match req.send().await {
                            Ok(r) if r.status().is_success() => {
                                break;
                            }
                            _ => {}
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                });

                // Heartbeat loop
                let route_clone3 = route.clone();
                let cid3 = cfg.client_id.clone();
                let secret3 = cfg.client_secret.clone();
                tokio::spawn(async move {
                    let client = reqwest::Client::new();
                    loop {
                        let metrics = serde_json::json!({
                            "nodeId": "node-1",
                            "activeConnections": crate::transport::get_active_connections(),
                            "ts": chrono::Utc::now().to_rfc3339(),
                        });
                        let mut req = client
                            .post(format!("{}/heartbeat", route_clone3))
                            .json(&metrics);
                        if let Some(tc) = ACCESS_TOKEN.get() {
                            if let Some(tok) = tc.lock().await.clone() {
                                req = req.bearer_auth(tok);
                            } else if let (Some(id), Some(secret)) = (&cid3, &secret3) {
                                req = req.basic_auth(id.clone(), Some(secret.clone()));
                            }
                        }
                        let _ = req.send().await;
                        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    }
                });
            }
            ControlMode::SelfNode => {
                // No separate HTTP server here; control plane routes will be provided via built-in transports.
                println!("starting as control plane (self)");
            }
        }
    }
}

/// Get JWKS for a tenant: check local cache, else fetch from control plane (Route mode)
pub async fn get_jwks(tenant: &str) -> Option<Value> {
    let cache = JWKS_CACHE
        .get()
        .expect("jwks cache not initialized")
        .clone();
    // check cache
    {
        let guard = cache.lock().await;
        if let Some(v) = guard.get(tenant) {
            return Some(v.clone());
        }
    }
    if let Some(cfg) = get() {
        if let ControlMode::Route(route) = &cfg.mode {
            let client = reqwest::Client::new();
            let url = format!("{}/rpc/sys/tenant/jwks/get", route);
            let body = serde_json::json!({"tenant": tenant});
            let req = client.post(&url).json(&body);
            let req = if let (Some(id), Some(secret)) = (&cfg.client_id, &cfg.client_secret) {
                req.basic_auth(id.clone(), Some(secret.clone()))
            } else {
                req
            };
            if let Ok(resp) = req.send().await {
                if resp.status().is_success() {
                    if let Ok(jwks) = resp.json::<Value>().await {
                        let mut guard = cache.lock().await;
                        guard.insert(tenant.to_string(), jwks.clone());
                        return Some(jwks);
                    }
                }
            }
        }
    }
    None
}
