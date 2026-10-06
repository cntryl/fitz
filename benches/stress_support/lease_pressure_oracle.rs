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
