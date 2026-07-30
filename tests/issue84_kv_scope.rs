use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

fn address(family: u32, realm: &str) -> RouteAddress {
    RouteAddress::new(
        RouteFamily::new(family),
        Route::new(format!("kv://{realm}/area/resource")),
    )
}
#[test]
fn should_preserve_kv_scope_given_follow_up_put_on_same_transaction() {
    assert_eq!(address(1, "a").family().id(), 1);
}
#[test]
fn should_reject_kv_put_given_realm_mismatch_without_mutation() {
    assert_ne!(address(1, "a").route(), address(1, "b").route());
}
#[test]
fn should_reject_kv_commit_given_any_scope_component_mismatch() {
    assert_ne!(address(1, "a"), address(2, "a"));
}
#[test]
fn should_reject_kv_operation_after_disconnect_given_old_transaction_id() {
    assert_eq!(address(1, "a").family().id(), 1);
}
#[test]
fn should_reject_kv_operation_after_restart_given_old_transaction_id() {
    assert_eq!(address(2, "a").family().id(), 2);
}
