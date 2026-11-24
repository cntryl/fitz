//! Family-local coordination of DROP with read/write admin estimate refreshes.

use super::{KvError, KvStore};
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Instant;

#[derive(Default)]
pub(super) struct InventoryGates {
    /// Weak references retain no gate after its last active operation completes.
    by_family: Mutex<HashMap<u32, Weak<InventoryGate>>>,
}

#[derive(Default)]
pub(crate) struct InventoryGate {
    mutex: Mutex<()>,
}

/// Keeps family inventory coordination active for the caller's operation.
#[must_use = "keep the inventory guard alive for the coordinated operation"]
pub(crate) struct InventoryGuard<'a> {
    _held: MutexGuard<'a, ()>,
}

impl InventoryGate {
    pub(crate) fn acquire_until(&self, deadline: Instant) -> Result<InventoryGuard<'_>, KvError> {
        if Instant::now() >= deadline {
            return Err(coordination_timeout());
        }
        self.mutex
            .try_lock_until(deadline)
            .map(|held| InventoryGuard { _held: held })
            .ok_or_else(coordination_timeout)
    }
}

impl KvStore {
    pub(crate) fn inventory_gate_for(
        &self,
        column_family: u32,
        deadline: Instant,
    ) -> Result<Arc<InventoryGate>, KvError> {
        if Instant::now() >= deadline {
            return Err(coordination_timeout());
        }
        let mut gates = self
            .inventory_gates
            .by_family
            .try_lock_until(deadline)
            .ok_or_else(coordination_timeout)?;
        // Only registry lookup is shared across families. Never hold this lock
        // while waiting on a family gate or on storage.
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(&column_family).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        let gate = Arc::new(InventoryGate::default());
        gates.insert(column_family, Arc::downgrade(&gate));
        Ok(gate)
    }
}

fn coordination_timeout() -> KvError {
    KvError::BackendUnavailable(
        "KV inventory coordination exhausted its admission budget".to_string(),
    )
}
