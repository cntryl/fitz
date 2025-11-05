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
    // TODO: This is a temporary stub. In production, the application
    // should create a KvStore implementation (e.g., ShaleStore) and
    // start the engine before initializing transports.
    
    // For now, panic to indicate this needs proper setup
    panic!("transport::init() is a stub - application must provide KvStore and start engine");
    
    // Example of how this should be called:
    // let store: Arc<dyn KvStore> = Arc::new(MyKvStoreImpl::new());
    // let engine = crate::core::engine::start_engine(store);
    // let ws_transport = ws::WsTransport::new(addr, engine.clone());
    // tokio::spawn(async move { ws_transport.run().await });
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
