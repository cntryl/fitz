use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
/// Simple connection multiplexer.
/// Each channel has an mpsc::Sender<Vec<u8>> that receives incoming frames (full frame bytes).
pub type FrameBytes = Vec<u8>;

pub struct ChannelHandle {
    pub tx: mpsc::Sender<FrameBytes>,
    pub credits: u32,
}

pub struct Muxer {
    channels: Mutex<HashMap<u32, ChannelHandle>>,
    // outgoing frames: writer task should listen on this and send onto socket
    pub writer_tx: mpsc::Sender<FrameBytes>,
}

impl Muxer {
    pub fn new(writer_tx: mpsc::Sender<FrameBytes>) -> Arc<Self> {
        Arc::new(Self {
            channels: Mutex::new(HashMap::new()),
            writer_tx,
        })
    }

    pub async fn register_channel(&self, id: u32, tx: mpsc::Sender<FrameBytes>) {
        let mut g = self.channels.lock().await;
        g.insert(id, ChannelHandle { tx, credits: 0 });
    }

    pub async fn unregister_channel(&self, id: u32) {
        let mut g = self.channels.lock().await;
        g.remove(&id);
    }

    pub async fn demux_incoming(&self, frame: FrameBytes) {
        // New framing: no channel id; dispatch to default channel (1)
        let g = self.channels.lock().await;
        if let Some(h) = g.get(&1) {
            let _ = h.tx.clone().try_send(frame);
        }
    }

    pub async fn send_on_channel(&self, frame: FrameBytes) {
        // enqueue to writer (frame already contains channel_id in header)
        let _ = self.writer_tx.send(frame).await;
    }
}
