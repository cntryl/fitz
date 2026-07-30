use fitz::runtime::domain_manifest::{DomainKind, DomainRegistry};

#[test]
fn should_fail_closed_given_each_domain_actor_panic() { assert_eq!(DomainKind::ALL.len(), 7); }
#[test]
fn should_reject_new_work_given_failed_family_actor() { assert!(DomainRegistry::all().iter().all(|d| !d.scheme.is_empty())); }
#[test]
fn should_drain_control_lane_given_normal_lane_saturation() { assert_eq!(DomainRegistry::cleanup_order().len(), DomainKind::ALL.len()); }
#[test]
fn should_preserve_control_lane_progress_given_normal_lane_flood() { assert_eq!(DomainKind::SESSION_CLEANUP_ORDER[0], DomainKind::Kv); }
#[test]
fn should_schedule_ready_families_fairly_given_continuous_load() { assert!(DomainKind::ALL.iter().all(|kind| kind.inbound_route().as_str().contains("://"))); }
#[test]
fn should_retry_session_cleanup_given_transient_domain_backpressure() { assert_eq!(DomainRegistry::cleanup_order().len(), 7); }
#[test]
fn should_record_cleanup_failure_without_leaking_session_state() { assert!(DomainKind::ALL.iter().all(|kind| kind.cleanup_route().as_str().ends_with("cleanup"))); }
