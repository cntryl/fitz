use std::collections::HashMap;
use std::sync::Arc;

use crate::core::engine::EngineHandle;
use crate::transport::mux::Muxer;

type SubEntry = (u64, tokio::task::JoinHandle<()>);

#[derive(Clone)]
pub struct SessionState {
    pub mux: Arc<Muxer>,
    pub engine: EngineHandle,
    pub channel_id: u32,
    pub auth_state: Arc<tokio::sync::Mutex<Option<String>>>,
    pub inflight: Arc<tokio::sync::Mutex<usize>>, // retained for incidental metrics; not used for gating
    pub permits: Arc<tokio::sync::Semaphore>,
    pub ack_delay_ms: u64,
    pub subs: Arc<tokio::sync::Mutex<HashMap<String, SubEntry>>>,
}

impl SessionState {
    pub fn new(mux: Arc<Muxer>, engine: EngineHandle, channel_id: u32) -> Self {
        let broker_cfg = crate::config::load().broker;
        Self {
            mux,
            engine,
            channel_id,
            auth_state: Arc::new(tokio::sync::Mutex::new(None)),
            inflight: Arc::new(tokio::sync::Mutex::new(0)),
            permits: Arc::new(tokio::sync::Semaphore::new(broker_cfg.ack_window)),
            ack_delay_ms: broker_cfg.test_ack_delay_ms,
            subs: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }
}
