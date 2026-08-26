use super::{authorize_owned_lease, LeaseAuthorization};
use crate::domains::lease::sink::model::{Instant, SinkLeaseState};

fn state(owner_id: &str, fencing_token: u64, expiry: Instant) -> SinkLeaseState {
    SinkLeaseState {
        owner_id: owner_id.to_string(),
        owner_session_id: 7,
        fencing_token,
        expiry,
        acquired_at: String::new(),
        renewals: 0,
    }
}

#[test]
fn should_classify_expired_lease_before_owner_plus_token_checks() {
    // Arrange
    let now = Instant::now();
    let lease = state("another-owner", 11, now);

    // Act
    let authorization = authorize_owned_lease(Some(&lease), "owner", 9, now);

    // Assert
    assert_eq!(authorization, LeaseAuthorization::Expired);
}

#[test]
fn should_report_current_token_for_fenced_owner() {
    // Arrange
    let now = Instant::now();
    let lease = state("owner", 11, now + std::time::Duration::from_secs(1));

    // Act
    let authorization = authorize_owned_lease(Some(&lease), "owner", 9, now);

    // Assert
    assert_eq!(authorization, LeaseAuthorization::Fenced(11));
}
