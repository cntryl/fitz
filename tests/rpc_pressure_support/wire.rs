use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::testkit::transport::{TlvFrameBuilder, TlvFrameParser};
use uuid::Uuid;

pub const ROUTE: &str = "rpc://pressure/service/work";

pub fn frame(kind: u16, payload: &[u8]) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(kind, payload);
    builder.build()
}

pub fn subscribe() -> Vec<u8> {
    let mut payload = PayloadEncoder::new();
    payload.put_string(ROUTE);
    payload.put_u32(1);
    payload.put_u8(1);
    payload.put_u8(1);
    frame(300, &payload.finish())
}

pub fn body(sequence: u64) -> Vec<u8> {
    let mut body = vec![0xA5; 1024];
    body[..8].copy_from_slice(&sequence.to_be_bytes());
    body
}

pub fn request(id: Uuid, sequence: u64, budget: u32) -> Vec<u8> {
    let mut payload = PayloadEncoder::new();
    payload.put_raw(id.as_bytes());
    payload.put_string(ROUTE);
    payload.put_bytes(&body(sequence));
    payload.put_u8(1);
    payload.put_u8(1);
    payload.put_u32(budget);
    frame(302, &payload.finish())
}

pub fn control(kind: u8, id: Uuid, reason: Option<u8>) -> Vec<u8> {
    let mut payload = vec![kind];
    payload.extend_from_slice(id.as_bytes());
    if let Some(reason) = reason {
        payload.push(reason);
    }
    frame(304, &payload)
}

pub fn lifecycle(bytes: &[u8], kind: u8, id: Uuid, reason: u8) -> Result<(), String> {
    let mut parser = TlvFrameParser::new(bytes);
    let (message, payload) = parser.next_field().ok_or("missing lifecycle")?;
    let mut expected = vec![kind];
    expected.extend_from_slice(id.as_bytes());
    expected.push(reason);
    if message != 305 || payload != expected || parser.next_field().is_some() {
        return Err("unexpected cancellation lifecycle identity/kind/reason".into());
    }
    Ok(())
}

pub fn registration(bytes: &[u8]) -> Result<(), String> {
    let mut parser = TlvFrameParser::new(bytes);
    let (message, payload) = parser.next_field().ok_or("missing registration")?;
    let mut decoder = PayloadDecoder::new(&payload);
    if message != 300
        || decoder.get_u8()? != 0
        || !decoder.get_bytes()?.is_empty()
        || !decoder.is_complete()
        || parser.next_field().is_some()
    {
        return Err("worker registration rejected or malformed".into());
    }
    Ok(())
}

pub fn terminal(
    bytes: &[u8],
    pending: &mut std::collections::HashMap<Uuid, u64>,
) -> Result<bool, String> {
    let (id, response) = terminal_envelope(bytes)?;
    let sequence = pending
        .remove(&id)
        .ok_or("unknown or duplicate terminal UUID")?;
    if response.as_ref() == body(sequence) {
        return Ok(true);
    }
    let (code, message) = fitz::protocol::error_codes::decode_error_body(&response)?;
    if code != fitz::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE {
        return Err(format!("unexpected terminal {code}: {message}"));
    }
    Ok(false)
}

fn terminal_envelope(bytes: &[u8]) -> Result<(Uuid, bytes::Bytes), String> {
    let mut parser = TlvFrameParser::new(bytes);
    let (kind, payload) = parser.next_field().ok_or("missing terminal reply")?;
    if kind != 303 || parser.next_field().is_some() {
        return Err("invalid terminal frame".into());
    }
    let mut decoder = PayloadDecoder::new(&payload);
    let mut id = [0; 16];
    id[..8].copy_from_slice(&decoder.get_u64()?.to_be_bytes());
    id[8..].copy_from_slice(&decoder.get_u64()?.to_be_bytes());
    let id = Uuid::from_bytes(id);
    let reply_sequence = decoder.get_u64()?;
    let flags = decoder.get_u8()?;
    let response = decoder.get_bytes()?;
    if reply_sequence != 0 || flags != 1 || !decoder.is_complete() {
        return Err("invalid terminal sequence, flags, or trailing data".into());
    }
    Ok((id, response))
}

pub fn terminal_error(bytes: &[u8], expected_id: Uuid, expected_code: u16) -> Result<(), String> {
    let (id, response) = terminal_envelope(bytes)?;
    let (code, _) = fitz::protocol::error_codes::decode_error_body(&response)?;
    if id != expected_id || code != expected_code {
        return Err("unexpected terminal error identity/code".into());
    }
    Ok(())
}
