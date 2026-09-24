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
    "acme/*/users",
    "*/app/users",
    "*/app/*",
    "*/*/users",
    "acme/app/users#read",
    "acme/app/users#*",
    "acme/**#read",
    "ACME/App/Users",
    "acme/app/us%2Fers",
    "acme/app/users/",
    "acme//users",
    "acme/app/users ",
    "",
];

const FOREIGN_PREFIXES: &[&str] = &["queue://", "kv://", "kv:/", "KV://", "://"];

#[test]
fn should_keep_pattern_registration_message_table_stable() {
    // Arrange
    let mut actual = String::new();

    // Act
    for domain in DispatchDomain::ALL {
        for id in 0..=999_u16 {
            let registration = is_subscription_registration_message(domain, id);
            let pattern = is_pattern_authorization_target(domain, id, "acme/*/x");
            let literal = is_pattern_authorization_target(domain, id, "acme/app/x");
            if registration || pattern || literal {
                let _ = writeln!(
                    actual,
                    "{} {id} registration={registration} pattern={pattern} literal={literal}",
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
        for prefix in FOREIGN_PREFIXES {
            let candidate = format!("{prefix}acme/app/users");
            let result = canonicalize_dispatch_route_str(domain, &candidate);
            let _ = writeln!(actual, "{} {candidate:?} => {result:?}", domain.as_str());
        }
    }

    // Assert
    assert_golden("ingress_auth_canonicalization", &actual);
}
