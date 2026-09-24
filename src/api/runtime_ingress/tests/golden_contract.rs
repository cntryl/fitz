//! Golden contracts for ingress authorization routing.
//!
//! Pins which message IDs register patterns and how each domain canonicalizes
//! auth routes, so moving these rules into domains cannot change behavior.

use super::*;
use crate::testkit::golden::assert_golden;
use std::fmt::Write as _;

const ROUTES: &[&str] = &[
    "acme/app/users",
    "acme/app/users/extra",
    "acme/app/users/get",
    "acme/app",
    "acme",
    "*",
    "**",
    "acme/**",
    "acme/*/*",
    "acme/app/*",
    "acme/**/orders",
    "*/*/*",
    "acme/lock*/db",
    "acme/locks/db-migration/trailing",
    "",
];

#[test]
fn should_keep_pattern_registration_message_table_stable() {
    // Arrange
    let mut actual = String::new();

    // Act
    for domain in DispatchDomain::ALL {
        for id in 0..=999_u16 {
            let registration = is_subscription_registration_message(domain, id);
            let pattern = is_pattern_authorization_target(domain, id, "acme/*/x");
            if registration || pattern {
                let _ = writeln!(
                    actual,
                    "{} {id} registration={registration} pattern={pattern}",
                    domain.as_str()
                );
            }
        }
    }

    // Assert
    assert_golden("ingress_pattern_messages", &actual);
}

#[test]
fn should_keep_auth_route_canonicalization_stable() {
    // Arrange
    let mut actual = String::new();

    // Act
    for domain in DispatchDomain::ALL {
        for route in ROUTES {
            for candidate in [
                (*route).to_string(),
                format!("{}://{route}", domain.as_str()),
            ] {
                let result = canonicalize_dispatch_route_str(domain, &candidate);
                let _ = writeln!(actual, "{} {candidate:?} => {result:?}", domain.as_str());
            }
        }
    }

    // Assert
    assert_golden("ingress_auth_canonicalization", &actual);
}
