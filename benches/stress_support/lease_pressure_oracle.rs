//! A queued provisional token is not a live ownership grant.
use fitz::protocol::payload_codec::PayloadDecoder;
pub(crate) fn token(body: &[u8], expected_kind: u8) -> Result<u64, String> {
    let mut decoder = PayloadDecoder::new(body);
    let status = decoder.get_u8()?;
    let kind = decoder.get_u8()?;
    let token = decoder.get_u64()?;
    if status != 0 || kind != expected_kind || token == 0 || !decoder.is_complete() {
        return Err("unexpected acquire status/kind/token or trailing bytes".into());
    }
    Ok(token)
}

/// QUERY can report Expired before the sweep removes the ephemeral record.
/// This is a polling transition, never proof of completed cleanup.
pub(crate) fn pending_expiry(body: &[u8]) -> Result<bool, String> {
    if body.first() == Some(&0) {
        return Ok(false);
    }
    let (code, _) = fitz::protocol::error_codes::decode_error_body(body)?;
    Ok(code == fitz::protocol::error_codes::lease::ERR_LEASE_EXPIRED)
}
