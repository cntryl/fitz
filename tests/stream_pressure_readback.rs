#[path = "stream_pressure_support/readback.rs"]
mod readback;
use fitz::protocol::payload_codec::PayloadEncoder;

const ROUTE: &str = "stream://stress-bench/history/growing";

#[test]
fn should_preserve_coded_stream_read_failure_detail() {
    // Arrange
    let mut body = PayloadEncoder::new();
    body.put_u8(1);
    body.put_u32(2012);
    body.put_string("no free L0 slot");
    // Act
    let result = readback::verify_page(&body.finish(), ROUTE, 0, 1, 1, 32);
    // Assert
    assert_eq!(
        result,
        Err("Stream READ error code 2012: no free L0 slot".into())
    );
}

#[test]
fn should_accept_status_only_stream_rollback_success() {
    assert!(admission::rollback_success(&[0]).is_ok());
}

#[test]
fn should_reject_extended_stream_rollback_success() {
    assert!(admission::rollback_success(&[0, 0, 0, 0, 0]).is_err());
}

#[test]
fn should_classify_exact_actor_batch_limit_response() {
    // Arrange
    let error = "Stream error code 2012: batch too large";
    // Act
    let result = admission::batch_limit(error);
    // Assert
    assert!(result);
}

#[test]
fn should_reject_incidental_batch_words_in_backend_failure() {
    // Arrange
    let error = "Stream error code 2012: write stall while flushing batch too large";
    // Act
    let result = admission::batch_limit(error);
    // Assert
    assert!(!result);
}

fn page(route: &str, offsets: &[u64], corrupt: bool, more: u8) -> Vec<u8> {
    let mut data = PayloadEncoder::new();
    data.put_u32(u32::try_from(offsets.len()).unwrap());
    for offset in offsets {
        data.put_string(route);
        data.put_u8(0);
        data.put_u64(*offset);
        data.put_optional_u64(None);
        data.put_optional_u64(None);
        let mut body = readback::payload(*offset, 32);
        if corrupt {
            body[31] ^= 1;
        }
        data.put_bytes(&body);
        data.put_u8(0);
        data.put_u64(0);
    }
    data.put_u64(offsets.last().copied().unwrap_or(0));
    data.put_optional_u64(None);
    data.put_optional_u64(None);
    data.put_u8(more);
    let mut outer = PayloadEncoder::new();
    outer.put_u8(0);
    outer.put_optional_u64(None);
    outer.put_bytes(&data.finish());
    outer.finish()
}

#[test]
fn should_validate_partial_replay_page() {
    // Arrange
    let body = page(ROUTE, &[4, 5], false, 1);
    // Act
    let result = readback::verify_page(&body, ROUTE, 4, 7, 2, 32);
    // Assert
    assert_eq!(result.unwrap(), 2);
}

#[test]
fn should_reject_missing_committed_events() {
    // Arrange
    let body = page(ROUTE, &[], false, 0);
    // Act
    let result = readback::verify_page(&body, ROUTE, 4, 7, 2, 32);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_reordered_replay_offsets() {
    // Arrange
    let body = page(ROUTE, &[5, 4], false, 0);
    // Act
    let result = readback::verify_page(&body, ROUTE, 4, 6, 2, 32);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_changed_replay_payload() {
    // Arrange
    let body = page(ROUTE, &[4, 5], true, 0);
    // Act
    let result = readback::verify_page(&body, ROUTE, 4, 6, 2, 32);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_wrong_replay_route() {
    // Arrange
    let body = page("stream://other/history/growing", &[4], false, 0);
    // Act
    let result = readback::verify_page(&body, ROUTE, 4, 5, 2, 32);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_history_beyond_commit_ledger() {
    // Arrange
    let body = page(ROUTE, &[4, 5, 6], false, 0);
    // Act
    let result = readback::verify_page(&body, ROUTE, 4, 6, 4, 32);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_false_end_of_history_cursor() {
    // Arrange
    let body = page(ROUTE, &[4, 5], false, 0);
    // Act
    let result = readback::verify_page(&body, ROUTE, 4, 7, 2, 32);
    // Assert
    assert!(result.is_err());
}
#[path = "stream_pressure_support/admission.rs"]
mod admission;
