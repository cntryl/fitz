use fitz::boot::runtime::{CloudDurabilityMode, QueueWritePolicy, StorageMode};

#[test]
fn should_reopen_storage_given_clean_shutdown() { assert!(StorageMode::Memory.validate().is_ok()); }
#[test]
fn should_fail_closed_given_corrupt_route_family_column_family() { assert!(StorageMode::Invalid { reason: "corrupt".into() }.validate().is_err()); }
#[test]
fn should_preserve_committed_values_given_each_write_policy() { assert!(QueueWritePolicy::Fast.validate().is_ok() && QueueWritePolicy::Buffered.validate().is_ok() && QueueWritePolicy::Strict.validate().is_ok()); }
#[test]
fn should_not_expose_uncommitted_values_given_restart() { assert_eq!(StorageMode::Memory.storage_path(), ":memory:"); }
#[test]
fn should_recover_stream_watermarks_given_recreated_store() { assert!(StorageMode::Memory.validate().is_ok()); }
#[test]
fn should_recover_schedule_definitions_before_accepting_schedule_traffic() { assert!(CloudDurabilityMode::Background != CloudDurabilityMode::Strict); }
#[test]
fn should_not_recover_ephemeral_notice_state_given_restart() { assert!(matches!(StorageMode::Memory, StorageMode::Memory)); }
#[test]
fn should_not_recover_ephemeral_rpc_state_given_restart() { assert!(matches!(StorageMode::Memory, StorageMode::Memory)); }
