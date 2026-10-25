//! Connection admission and the `CONNECT_REJECTED` write barrier.

use crate::protocol::mux::ChannelMessage;
use crate::protocol::wire2::header::decode_message;
use parking_lot::Mutex;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub(crate) const MAX_IN_FLIGHT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Request {
    Header(u16, Option<NonZeroU64>),
    Rpc([u8; 16]),
    Cancel([u8; 16]),
}

#[derive(Default)]
pub(crate) struct ConnectionState {
    requests: Mutex<Vec<Request>>,
    connect_seen: AtomicBool,
    hello_sent: AtomicBool,
    rejection_queued: AtomicBool,
    rejection_flushed: AtomicBool,
    rejection_written: tokio::sync::Notify,
}

fn uuid(bytes: &[u8]) -> Option<[u8; 16]> {
    bytes.get(..16)?.try_into().ok()
}

impl ConnectionState {
    pub(super) fn hello_sent(&self) -> bool {
        self.hello_sent.load(Ordering::Acquire)
    }
    pub(crate) fn validate_request(&self, message: &ChannelMessage) -> Result<(), &'static str> {
        let kind = message.msg_type.as_u16();
        if kind == 1 {
            self.connect_seen.store(true, Ordering::Release);
            return Ok(());
        }
        if !self.connect_seen.load(Ordering::Acquire) {
            return Err("CONNECT must be the first message");
        }
        // Worker RESPONSE and lifecycle acknowledgements are addressed by RPC
        // UUID, rather than being new connection-level requests.
        let rpc_worker_reply = kind == 303 || (kind == 304 && message.payload.first() != Some(&1));
        let unacknowledged_publish = kind == 500 && !message.flags.is_ack_requested();
        if !rpc_worker_reply && !unacknowledged_publish && message.correlation.is_none() {
            return Err("request correlation must be non-zero");
        }
        Ok(())
    }

    pub(crate) fn admit(&self, message: &ChannelMessage) -> bool {
        let request = match message.msg_type.as_u16() {
            500 if !message.flags.is_ack_requested() => return true,
            302 => uuid(&message.payload).map(Request::Rpc),
            304 if message.payload.first() == Some(&1) => {
                uuid(&message.payload[1..]).map(Request::Cancel)
            }
            1 | 303 | 304 => return true,
            kind => Some(Request::Header(kind, message.correlation)),
        };
        let Some(request) = request else { return true };
        let mut requests = self.requests.lock();
        if requests.len() >= MAX_IN_FLIGHT {
            return false;
        }
        requests.push(request);
        true
    }

    /// Called only after a response was accepted by the transport queue.
    pub(super) fn sent(&self, frame: &[u8]) {
        let Ok((header, payload)) = decode_message(frame, u32::MAX) else {
            return;
        };
        let request = match header.message_type {
            2 => {
                self.hello_sent.store(true, Ordering::Release);
                return;
            }
            3 => {
                self.rejection_queued.store(true, Ordering::Release);
                return;
            }
            // A queued acquire retains its slot until the later grant/error.
            400 if payload.starts_with(&[0, 2]) => return,
            303 if payload.get(24).is_some_and(|flags| flags & 1 != 0) => {
                uuid(payload).map(Request::Rpc)
            }
            305 if payload.first() == Some(&4) => {
                if payload.get(17)
                    == Some(
                        &(crate::protocol::rpc_codec::RpcCancellationResult::QueuedRemoved as u8),
                    )
                {
                    if let Some(id) = uuid(&payload[1..]) {
                        let mut requests = self.requests.lock();
                        for request in [Request::Cancel(id), Request::Rpc(id)] {
                            if let Some(index) =
                                requests.iter().position(|pending| *pending == request)
                            {
                                requests.swap_remove(index);
                            }
                        }
                        return;
                    }
                }
                uuid(&payload[1..]).map(Request::Cancel)
            }
            302 | 303 | 305 => return,
            kind => Some(Request::Header(kind, header.correlation)),
        };
        let Some(request) = request else { return };
        let mut requests = self.requests.lock();
        if let Some(index) = requests.iter().position(|pending| *pending == request) {
            requests.swap_remove(index);
        }
    }

    pub(crate) fn mark_rejection_flushed(&self) {
        self.rejection_flushed.store(true, Ordering::Release);
        self.rejection_written.notify_one();
    }

    pub(crate) async fn flush_rejection(&self) {
        if !self.rejection_queued.load(Ordering::Acquire) {
            return;
        }
        let written = self.rejection_written.notified();
        if !self.rejection_flushed.load(Ordering::Acquire) {
            let _ = tokio::time::timeout(Duration::from_secs(1), written).await;
        }
    }
}

#[cfg(test)]
mod tests;
