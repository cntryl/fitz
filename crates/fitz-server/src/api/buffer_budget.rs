//! Byte admission for transport-owned frames, shared across both listeners.
//!
//! Reservations follow `Bytes` clones and slices until their final owner drops.
//! This bounds queued/in-flight frame bytes, not storage or parser workspaces.

use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) const SESSION_BUFFER_BYTES: usize = 8 * 1024 * 1024;
pub(super) const TRANSPORT_BUFFER_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct BufferBudget {
    shared: Arc<Semaphore>,
    session: Arc<Semaphore>,
}

impl BufferBudget {
    pub(crate) fn new(shared: Arc<Semaphore>, session_bytes: usize) -> Self {
        Self {
            shared,
            session: Arc::new(Semaphore::new(session_bytes)),
        }
    }

    pub(crate) fn wrap(&self, bytes: Bytes) -> Result<Bytes, &'static str> {
        let count = u32::try_from(bytes.len()).map_err(|_| "frame byte count exceeds u32")?;
        let session = self
            .session
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| "session transport byte budget exhausted")?;
        let shared = self
            .shared
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| "shared transport byte budget exhausted")?;
        Ok(Bytes::from_owner(BudgetedFrame {
            bytes,
            _session: session,
            _shared: shared,
        }))
    }
}

impl Default for BufferBudget {
    fn default() -> Self {
        Self::new(
            Arc::new(Semaphore::new(TRANSPORT_BUFFER_BYTES)),
            SESSION_BUFFER_BYTES,
        )
    }
}

struct BudgetedFrame {
    bytes: Bytes,
    _session: OwnedSemaphorePermit,
    _shared: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for BudgetedFrame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests;
