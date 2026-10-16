#[path = "../benches/stress_support/kv_pressure_oracle.rs"]
mod oracle;
use fitz::protocol::payload_codec::PayloadEncoder;
fn body(found: u8, value: &[u8]) -> Vec<u8> {
    let mut encoder = PayloadEncoder::new();
    encoder.put_u8(0);
    encoder.put_u8(found);
    encoder.put_bytes(value);
    encoder.finish()
}
#[test]
fn should_reject_changed_committed_value() {
    // Arrange
    let response = body(1, b"altered");
    // Act
    let result = oracle::verify_value(&response, b"committed");
    // Assert
    assert!(result.is_err());
}
#[test]
fn should_reject_missing_committed_value() {
    // Arrange
    let response = body(0, b"committed");
    // Act
    let result = oracle::verify_value(&response, b"committed");
    // Assert
    assert!(result.is_err());
}
#[test]
fn should_reject_trailing_committed_response_bytes() {
    // Arrange
    let mut response = body(1, b"committed");
    response.push(0);
    // Act
    let result = oracle::verify_value(&response, b"committed");
    // Assert
    assert!(result.is_err());
}
#[test]
fn should_validate_exact_committed_value() {
    // Arrange
    let response = body(1, b"committed");
    // Act
    let result = oracle::verify_value(&response, b"committed");
    // Assert
    assert!(result.is_ok());
}
