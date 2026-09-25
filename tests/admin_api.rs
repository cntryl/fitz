//! Integration tests for the admin REST API.

mod admin_api {
    mod auth_startup;
    mod common;
    mod domain_inventory;
    mod family_troubleshooting;
    mod metrics_primary;
    mod queue_resource_actions;
    mod schedule_run_now_contract;
    mod stream_metrics_contract;
    mod stream_notice_metrics;
    mod topology_sessions;
    mod typed_admin_errors;
}
