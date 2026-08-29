mod claim_and_ack;
mod definitions_and_listing;
mod init_and_create;
mod model;
mod scan_helpers_and_handle;
#[cfg(test)]
mod test_actor_harness;

pub(super) const SCAN_DEDUP_WINDOW: std::time::Duration = std::time::Duration::from_millis(10);
/// Keeps each synchronous Schedule persistence transaction short enough to
/// yield the actor lock between due-storm batches.
pub(super) const MAX_DUE_CLAIMS_PER_SCAN: usize = 32;

fn retry_persistence<T>(
    mut operation: impl FnMut() -> Result<T, model::SchedulePersistenceError>,
) -> Result<T, String> {
    const MAX_ATTEMPTS: usize = 4;
    let mut delay = std::time::Duration::from_millis(1);

    for attempt in 0..MAX_ATTEMPTS {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if error.is_write_stall() && attempt + 1 < MAX_ATTEMPTS => {
                std::thread::sleep(delay);
                delay = delay.saturating_mul(2);
            }
            Err(error) => return Err(error.into()),
        }
    }

    unreachable!("bounded Schedule persistence retry loop must return")
}

pub use model::ScheduleActor;

#[cfg(test)]
use model::*;

#[cfg(test)]
mod tests;
