use super::*;

#[test]
fn should_remove_watch_session_subscriptions_on_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut registry = KvWatchRegistry::new(family);
    let route = RouteAddress::new(family, Route::new("inbox://session/7"));
    registry
        .subscribe(7, Pattern::new("kv://acme/app/users"), route.clone())
        .expect("subscribe users");
    registry
        .subscribe(7, Pattern::new("kv://acme/app/orders"), route)
        .expect("subscribe orders");

    // Act
    let removed = registry.remove_session(7);

    // Assert
    assert_eq!(removed, 2);
    assert!(registry.is_empty());
}
