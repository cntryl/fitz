#[path = "../benches/stress_support/lease_pressure_oracle.rs"]
mod oracle;
use fitz::protocol::payload_codec::PayloadEncoder;
fn response(kind: u8, token: u64) -> Vec<u8> {
    let mut encoder = PayloadEncoder::new();
    encoder.put_u8(0);
    encoder.put_u8(kind);
    encoder.put_u64(token);
    encoder.finish()
}
#[test]
fn should_reject_provisional_token_as_ownership_grant() {
    // Arrange
    let body = response(2, 42);
    // Act
    let result = oracle::token(&body, 0);
    // Assert
    assert!(result.is_err());
}
#[test]
fn should_reject_zero_fencing_token() {
    // Arrange
    let body = response(0, 0);
    // Act
    let result = oracle::token(&body, 0);
    // Assert
    assert!(result.is_err());
}
#[test]
fn should_reject_trailing_grant_bytes() {
    // Arrange
    let mut body = response(0, 42);
    body.push(0);
    // Act
    let result = oracle::token(&body, 0);
    // Assert
    assert!(result.is_err());
}
#[test]
fn should_validate_complete_ownership_grant() {
    // Arrange
    let body = response(0, 42);
    // Act
    let result = oracle::token(&body, 0);
    // Assert
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn should_poll_documented_expired_query_before_cleanup() {
    // Arrange
    let body = fitz::protocol::error_codes::encode_error_body(5003, "Expired");
    // Act
    let result = oracle::pending_expiry(&body);
    // Assert
    assert!(result.unwrap());
}

#[test]
fn should_not_poll_away_unexpected_query_error() {
    // Arrange
    let body = fitz::protocol::error_codes::encode_error_body(5009, "Unauthorized");
    // Act
    let result = oracle::pending_expiry(&body);
    // Assert
    assert!(!result.unwrap());
}
