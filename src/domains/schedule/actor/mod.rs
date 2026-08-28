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

pub use model::ScheduleActor;

#[cfg(test)]
use model::*;

#[cfg(test)]
mod tests;
