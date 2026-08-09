use std::collections::HashMap;

#[derive(Clone, Copy, Debug)]
pub(super) struct KvKeyInventoryChange {
    pub(super) before_bytes: Option<usize>,
    pub(super) after_bytes: Option<usize>,
}

#[derive(Default)]
pub(super) struct KvInventoryDelta {
    pub(super) key_changes: HashMap<Vec<u8>, KvKeyInventoryChange>,
    pub(super) estimate_incomplete: bool,
}

impl KvInventoryDelta {
    pub(super) fn is_empty(&self) -> bool {
        self.key_changes.is_empty() && !self.estimate_incomplete
    }

    pub(super) fn mark_incomplete(&mut self) {
        self.estimate_incomplete = true;
    }

    pub(super) fn record_key_change(
        &mut self,
        user_key: &[u8],
        before_bytes: Option<usize>,
        after_bytes: Option<usize>,
    ) {
        self.key_changes
            .entry(user_key.to_vec())
            .and_modify(|change| change.after_bytes = after_bytes)
            .or_insert(KvKeyInventoryChange {
                before_bytes,
                after_bytes,
            });
    }
}
