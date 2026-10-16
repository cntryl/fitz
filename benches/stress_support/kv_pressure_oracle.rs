//! Exact current-state oracle; a legal status alone never proves a committed value.
use fitz::protocol::payload_codec::PayloadDecoder;
pub(crate) fn verify_value(body: &[u8], expected: &[u8]) -> Result<(), String> {
    let mut decoder = PayloadDecoder::new(body);
    if decoder.get_u8()? != 0 || decoder.get_u8()? != 1 {
        return Err("committed value was not found".into());
    }
    let value = decoder.get_bytes()?;
    if !decoder.is_complete() || value.as_ref() != expected {
        return Err("committed value changed or response has trailing bytes".into());
    }
    Ok(())
}
