use super::*;
use crate::runtime::{DeliveryError, SessionCleanup};

#[test]
fn should_accept_control_lane_cleanup_for_every_registered_domain() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
    let router = Arc::new(Router::new());
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    let _domains =
        setup(&router, &store, &read_model, &domain_setup_options()).expect("set up all domains");

    // Act
    let outcomes = DomainKind::ALL.map(|domain| {
        let target = RouteAddress::new(RouteFamily::new(1), domain.cleanup_route());
        let outcome =
            router.route_high_priority(Envelope::new(target, SessionCleanup { session_id: 42 }));
        (domain.as_str(), outcome)
    });

    // Assert
    for (domain, outcome) in outcomes {
        assert!(
            outcome.is_ok(),
            "{domain} rejected control cleanup: {outcome:?}"
        );
    }
}

#[test]
fn should_reject_both_delivery_lanes_for_every_stopped_domain_sink() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
    let router = Arc::new(Router::new());
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    let domains =
        setup(&router, &store, &read_model, &domain_setup_options()).expect("set up all domains");
    let sinks = DomainKind::ALL.map(|domain| {
        let sink = router
            .resolve_domain_sink_for_tests(domain.as_str())
            .expect("registered domain sink");
        (domain, sink)
    });
    domains.stop();

    // Act
    let outcomes = sinks
        .iter()
        .flat_map(|(domain, sink)| {
            [false, true].map(|control| {
                let target = RouteAddress::new(RouteFamily::new(1), domain.cleanup_route());
                let envelope = Envelope::new(target, SessionCleanup { session_id: 42 });
                let outcome = if control {
                    sink.deliver_high_priority(envelope)
                } else {
                    sink.deliver(envelope)
                };
                (domain.as_str(), control, outcome)
            })
        })
        .collect::<Vec<_>>();

    // Assert
    for (domain, control, outcome) in outcomes {
        assert_eq!(
            outcome,
            Err(DeliveryError::ActorStopped),
            "{domain}, control={control}"
        );
    }
}
