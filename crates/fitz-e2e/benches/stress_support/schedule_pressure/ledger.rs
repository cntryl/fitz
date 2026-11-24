//! Bounded observed-delivery accounting, not downstream work acknowledgement.
pub struct Ledger {
    masks: Vec<u8>,
    broadcast: bool,
    pub completed: u64,
    pub received: [u64; 2],
}

impl Ledger {
    pub fn new(count: usize, broadcast: bool) -> Self {
        Self {
            masks: vec![0; count],
            broadcast,
            completed: 0,
            received: [0; 2],
        }
    }

    pub fn receive(&mut self, index: usize, receiver: usize) -> Result<bool, String> {
        if receiver >= 2 {
            return Err("unknown receiver".into());
        }
        let mask = self.masks.get_mut(index).ok_or("unknown occurrence")?;
        let bit = 1 << receiver;
        if (*mask & bit) != 0 || (!self.broadcast && *mask != 0) {
            return Err("duplicate occurrence delivery".into());
        }
        *mask |= bit;
        self.received[receiver] += 1;
        let complete = !self.broadcast || *mask == 3;
        if complete {
            self.completed += 1;
        }
        Ok(complete)
    }

    pub fn verify(&self) -> Result<(), String> {
        if self.masks.iter().all(|mask| {
            if self.broadcast {
                *mask == 3
            } else {
                *mask == 1 || *mask == 2
            }
        }) {
            Ok(())
        } else {
            Err("missing occurrence delivery within declared healthy receiver window".into())
        }
    }
}
