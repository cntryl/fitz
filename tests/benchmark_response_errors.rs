#[path = "../benches/stress_support/response.rs"]
#[allow(dead_code)]
mod response;
#[path = "../benches/stress_support/types.rs"]
#[allow(dead_code)]
mod types;

#[test]
fn should_classify_a_wire2_stream_error_as_a_domain_failure() {
    // Arrange
    let body = fitz::protocol::error_codes::encode_error_body(2006, "storage unavailable");
    // Act
    let failure = response::require_ok(&body, "Stream COMMIT").unwrap_err();
    // Assert
    assert_eq!(failure.kind, types::FailureKind::DomainError);
    assert_eq!(
        failure.detail,
        "Stream COMMIT failed: code 2006: storage unavailable"
    );
}

#[test]
fn should_preserve_the_numeric_code_of_a_wire2_busy_error() {
    // Arrange
    let body = fitz::protocol::error_codes::encode_error_body(4005, "at capacity");
    // Act
    let code = response::error_code(&body, "Queue RESERVE").unwrap();
    // Assert
    assert_eq!(code, 4005);
}

#[test]
fn should_reject_a_wire1_error_envelope_as_malformed() {
    // Arrange
    let body = [2, 0, 0, 7, 220, 0, 0, 0, 0];
    // Act
    let failure = response::require_ok(&body, "Stream APPEND").unwrap_err();
    // Assert
    assert_eq!(failure.kind, types::FailureKind::InvalidResponse);
}
