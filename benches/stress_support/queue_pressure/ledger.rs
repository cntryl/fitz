use serde::Serialize;
use std::collections::HashSet;

#[derive(Default)]
struct Entry {
    accepted: Option<u64>,
    acknowledged: Option<u64>,
    rejected: bool,
}

#[derive(Clone, Copy, Default, Serialize)]
pub struct Counts {
    pub sent: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub acknowledged: u64,
    pub accepted_unacknowledged: u64,
}

/// One bounded entry per sent request. Replies may arrive in either order.
#[derive(Default)]
pub struct Ledger {
    pub counts: Counts,
    entries: Vec<Entry>,
    accepted_ids: HashSet<u64>,
}

impl Ledger {
    pub fn dispatch(&mut self) -> usize {
        let sequence = self.entries.len();
        self.entries.push(Entry::default());
        self.counts.sent += 1;
        sequence
    }

    pub fn accepted(&mut self, sequence: usize, id: u64) -> Result<(), String> {
        let entry = self
            .entries
            .get_mut(sequence)
            .ok_or("unknown enqueue sequence")?;
        if entry.accepted.is_some()
            || entry.rejected
            || entry.acknowledged.is_some_and(|actual| actual != id)
            || !self.accepted_ids.insert(id)
        {
            return Err("duplicate or mismatched enqueue message ID".into());
        }
        entry.accepted = Some(id);
        self.counts.accepted += 1;
        if entry.acknowledged.is_none() {
            self.counts.accepted_unacknowledged += 1;
        }
        Ok(())
    }

    pub fn rejected(&mut self, sequence: usize) -> Result<(), String> {
        let entry = self
            .entries
            .get_mut(sequence)
            .ok_or("unknown rejected sequence")?;
        if entry.accepted.is_some() || entry.acknowledged.is_some() || entry.rejected {
            return Err("rejected work was accepted, delivered, or rejected twice".into());
        }
        entry.rejected = true;
        self.counts.rejected += 1;
        Ok(())
    }

    pub fn acknowledged(&mut self, sequence: usize, id: u64) -> Result<(), String> {
        let entry = self
            .entries
            .get_mut(sequence)
            .ok_or("delivery has unknown sequence")?;
        if entry.rejected
            || entry.acknowledged.is_some()
            || entry.accepted.is_some_and(|actual| actual != id)
        {
            return Err("duplicate, rejected, or changed delivery".into());
        }
        entry.acknowledged = Some(id);
        self.counts.acknowledged += 1;
        if entry.accepted.is_some() {
            self.counts.accepted_unacknowledged -= 1;
        }
        Ok(())
    }

    pub fn verify_drained(&self) -> Result<(), String> {
        if self.counts.sent != self.counts.accepted + self.counts.rejected
            || self.counts.accepted != self.counts.acknowledged
            || self.entries.iter().any(|entry| {
                if entry.rejected {
                    entry.accepted.is_some() || entry.acknowledged.is_some()
                } else {
                    entry.accepted.is_none() || entry.accepted != entry.acknowledged
                }
            })
        {
            return Err("accepted work is missing or enqueue outcomes remain indeterminate".into());
        }
        Ok(())
    }
}
