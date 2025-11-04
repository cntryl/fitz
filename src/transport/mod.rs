//! Transport module

pub mod http;
pub mod mux;
pub mod session;
pub mod tcp;
pub mod ws;

use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc as StdArc;

// Shared active connection counter for all transports
static ACTIVE_CONN: OnceCell<StdArc<AtomicUsize>> = OnceCell::new();

// Ports now come from config::TransportConfig (env-driven)

/// Initialize transports (stub)
pub fn init() {
    // Start a minimal websocket transport in background
    let store = std::sync::Arc::new(tokio::sync::Mutex::new(crate::storage::mem::MemStore::new()));
    // start engine with the store and obtain a handle
    let engine = crate::core::engine::start_engine(store.clone());
    // initialize active connections counter (no-op if already set)
    ACTIVE_CONN.set(StdArc::new(AtomicUsize::new(0))).ok();
    let cfg = crate::config::load();
    let ws_port = cfg.transport.ws_port;
    let tcp_port = cfg.transport.tcp_port;
    let addr: std::net::SocketAddr = ([0, 0, 0, 0], ws_port).into();
    let ws_engine = engine.clone();
    let transport = ws::WsTransport::new(addr, store, ws_engine);
    tokio::spawn(async move {
        if let Err(e) = transport.run().await {
            eprintln!("ws transport failed: {}", e);
        }
    });

    // Start TCP transport
    let tcp_addr: std::net::SocketAddr = ([0, 0, 0, 0], tcp_port).into();
    let tcp = tcp::TcpTransport::new(tcp_addr, engine.clone());
    tokio::spawn(async move {
        if let Err(e) = tcp.run().await {
            eprintln!("tcp transport failed: {}", e);
        }
    });

    // Emit control-plane frames over FTZ (baseline): register once, heartbeat periodically
    let engine_ctrl = engine.clone();
    tokio::spawn(async move {
        // Register
        let payload = serde_json::json!({
            "brokerId": "node-1",
            "version": env!("CARGO_PKG_VERSION"),
            "realmSpan": ["*"],
            "endpoints": { "ws_port": ws_port, "tcp_port": tcp_port },
            "capabilities": { "stream_backend": "kv", "queue_backend": "kv", "supports_peek": true, "supports_consume_prefix": true }
        }).to_string().into_bytes();
        let _ = engine_ctrl
            .publish(
                "control://broker/register".to_string(),
                format!("reg-{}", chrono::Utc::now().timestamp_millis()),
                payload,
                None,
                None,
                true,
                None,
            )
            .await;

        // Heartbeat loop (every 30s)
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            ticker.tick().await;
            let metrics = serde_json::json!({
                "nodeId": "node-1",
                "clients": crate::transport::get_active_connections(),
                "ts": chrono::Utc::now().to_rfc3339(),
            })
            .to_string()
            .into_bytes();
            let _ = engine_ctrl
                .publish(
                    "control://broker/heartbeat".to_string(),
                    format!("hb-{}", chrono::Utc::now().timestamp_millis()),
                    metrics,
                    None,
                    None,
                    true,
                    None,
                )
                .await;
        }
    });
}

// Transport lifecycle state shared across transports

struct TransportState {
    pub started: AtomicBool,
    pub ready: AtomicBool,
    pub live: AtomicBool,
}

static STATE: OnceCell<TransportState> = OnceCell::new();

fn ensure_state() -> &'static TransportState {
    STATE.get_or_init(|| TransportState {
        started: AtomicBool::new(false),
        ready: AtomicBool::new(false),
        live: AtomicBool::new(false),
    })
}

pub fn mark_started() {
    ensure_state().started.store(true, Ordering::SeqCst);
}
pub fn mark_ready() {
    ensure_state().ready.store(true, Ordering::SeqCst);
}
pub fn mark_live() {
    ensure_state().live.store(true, Ordering::SeqCst);
}
pub fn is_started() -> bool {
    ensure_state().started.load(Ordering::SeqCst)
}
pub fn is_ready() -> bool {
    ensure_state().ready.load(Ordering::SeqCst)
}
pub fn is_live() -> bool {
    ensure_state().live.load(Ordering::SeqCst)
}

// TLS is now loaded via crate::config::load().transport.tls

/// Increment active connections count (used by transport implementations)
pub fn inc_active_connections() {
    if let Some(arc) = ACTIVE_CONN.get() {
        arc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Decrement active connections count
pub fn dec_active_connections() {
    if let Some(arc) = ACTIVE_CONN.get() {
        arc.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Read current active connections
pub fn get_active_connections() -> usize {
    if let Some(arc) = ACTIVE_CONN.get() {
        arc.load(std::sync::atomic::Ordering::SeqCst)
    } else {
        0
    }
}
