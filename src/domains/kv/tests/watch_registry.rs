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

#[test]
fn should_reject_exact_watch_over_total_session_limit_and_reclaim_slots_on_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let subscriber = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mut registry = KvWatchRegistry::new(family);
    for index in 0..crate::domains::subscription_state::MAX_TOTAL_REGISTRATIONS_PER_SESSION {
        let route = Pattern::new(&format!("kv://acme/app/resource-{index}"));
        registry
            .subscribe(session_id, route, subscriber.clone())
            .expect("register exact watch within limit");
    }
    let overflow = Pattern::new("kv://acme/app/overflow");

    // Act
    let rejected = registry.subscribe(session_id, overflow.clone(), subscriber.clone());
    let removed = registry.remove_session(session_id);
    let accepted_after_cleanup = registry.subscribe(session_id, overflow, subscriber);

    // Assert
    assert!(matches!(
        rejected,
        Err(crate::domains::kv::KvError::SubscriptionLimit)
    ));
    assert_eq!(
        removed,
        crate::domains::subscription_state::MAX_TOTAL_REGISTRATIONS_PER_SESSION
    );
    assert!(accepted_after_cleanup.is_ok());
}

#[test]
fn should_reject_wildcard_watch_over_session_limit() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let subscriber = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mut registry = KvWatchRegistry::new(family);
    for index in 0..crate::domains::subscription_state::MAX_WILDCARD_REGISTRATIONS_PER_SESSION {
        let pattern = Pattern::new(&format!("kv://acme/area{index}/*"));
        registry
            .subscribe(session_id, pattern, subscriber.clone())
            .expect("register wildcard watch within limit");
    }
    let overflow = Pattern::new("kv://acme/overflow/*");

    // Act
    let rejected = registry.subscribe(session_id, overflow, subscriber);

    // Assert
    assert!(matches!(
        rejected,
        Err(crate::domains::kv::KvError::SubscriptionLimit)
    ));
}
