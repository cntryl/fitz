use super::{safe_return, tokens::grants, Store, Url};

fn callback_url() -> Url {
    Url::parse("https://admin.fitz.test/api/v1/session/oidc/callback").expect("callback URL")
}

#[test]
fn should_scope_oidc_admin_to_the_granted_route_family() {
    // Arrange
    let permissions = ["fitz:admin", "route_family:1", "org:2"];

    // Act
    let access = grants(&permissions).expect("explicit admin grants");

    // Assert
    assert_eq!(access.route_families(), ["1"]);
    assert!(!access.is_wildcard());
    assert!(!access.allows("2"));
}

#[test]
fn should_allow_all_families_given_the_literal_wildcard() {
    // Arrange
    let permissions = ["fitz:admin", "route_family:*"];

    // Act
    let access = grants(&permissions).expect("wildcard admin grant");

    // Assert
    assert!(access.is_wildcard());
}

#[test]
fn should_reject_route_family_grants_given_no_admin_permission() {
    // Arrange
    let permissions = ["route_family:*"];

    // Act
    let result = grants(&permissions);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_admin_permission_given_no_route_family_grant() {
    // Arrange
    let permissions = ["fitz:admin", "org:1"];

    // Act
    let result = grants(&permissions);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_a_route_family_with_leading_zeroes() {
    // Arrange
    let permissions = ["fitz:admin", "route_family:01"];

    // Act
    let result = grants(&permissions);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_preserve_the_complete_local_return_path() {
    // Arrange
    let target = "/queue?group=cold#events";

    // Act
    let returned = safe_return(&callback_url(), target);

    // Assert
    assert_eq!(returned, target);
}

#[test]
fn should_replace_an_off_origin_return_path() {
    assert_eq!(safe_return(&callback_url(), "//idp.example/redirect"), "/");
}

#[test]
fn should_reject_callback_state_bound_to_a_different_browser() {
    // Arrange
    let mut store = Store::default();
    let (state, _, _, _) = store.begin("/", 100).expect("challenge");
    let (_, browser, _, _) = store.begin("/", 100).expect("other challenge");

    // Act
    let result = store.take(&state, &browser, 101);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_replayed_callback_state() {
    // Arrange
    let mut store = Store::default();
    let (state, browser, _, _) = store.begin("/", 100).expect("challenge");
    store.take(&state, &browser, 101).expect("first use");

    // Act
    let result = store.take(&state, &browser, 101);

    // Assert
    assert!(result.is_err());
}

mod issuer;

#[test]
fn should_reject_oidc_configuration_in_open_mode() {
    // Arrange
    let lookup = |name: &str| match name {
        "FITZ_ADMIN_OIDC_ISSUER" => Some("https://provider.fitz.test/".to_owned()),
        "FITZ_ADMIN_AUTH_MODE" => Some("open".to_owned()),
        _ => None,
    };

    // Act
    let result = super::Settings::from_lookup(lookup);

    // Assert
    assert_eq!(
        result.err().as_deref(),
        Some("Admin OIDC requires FITZ_ADMIN_AUTH_MODE=protected"),
    );
}

#[test]
fn should_reject_an_oidc_session_at_its_expiry() {
    // Arrange
    let mut store = Store::default();
    let session = super::Session {
        principal: super::AdminPrincipal {
            username: "operator".to_owned(),
            route_family_access: super::super::AdminRouteFamilyAccess::wildcard(),
        },
        expires_at: 101,
    };
    let session = store.issue(session, 100).expect("session");

    // Act
    let principal = store.principal(&session, 101);

    // Assert
    assert!(principal.is_none());
}

#[test]
fn should_fall_back_for_a_local_return_normalized_to_login() {
    // Arrange
    let target = "/sessions/%2e%2e/login";

    // Act
    let returned = safe_return(&callback_url(), target);

    // Assert
    assert_eq!(returned, "/");
}
