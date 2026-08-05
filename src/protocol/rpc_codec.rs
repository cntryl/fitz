//! RPC domain codec - request/response operations
//!
//! Encodes/decodes TLV messages for the RPC domain.
//! Supports Subscribe, Unsubscribe, Request, Response operations.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.

use crate::dispatch::wire::rpc::{
    RpcClientResponseBody, RpcDecodeError, RpcMessage, RpcRequest, RpcResponse,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::protocol::tlv::MessageType;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

const UUID_BYTES_LEN: usize = 16;
const U8_LEN: usize = 1;
const U32_LEN: usize = 4;
const U64_LEN: usize = 8;
const MAX_WORKER_CONCURRENCY: u32 = 1024;
const RPC_RESPONSE_FLAG_STREAM_END: u8 = 0x01;
const RPC_RESPONSE_FLAGS_SUPPORTED: u8 = RPC_RESPONSE_FLAG_STREAM_END;

fn encoded_bytes_len(len: usize) -> usize {
    U32_LEN + len
}

fn encoded_string_len(value: &str) -> usize {
    encoded_bytes_len(value.len())
}

fn put_u32_len(buf: &mut BytesMut, len: usize) {
    buf.put_u32(u32::try_from(len).unwrap_or(u32::MAX));
}

fn put_payload_bytes(buf: &mut BytesMut, value: &[u8]) {
    put_u32_len(buf, value.len());
    buf.put_slice(value);
}

fn put_payload_string(buf: &mut BytesMut, value: &str) {
    put_u32_len(buf, value.len());
    buf.put_slice(value.as_bytes());
}

fn put_uuid(buf: &mut BytesMut, correlation_id: &Uuid) {
    buf.put_slice(correlation_id.as_bytes());
}

fn put_uuid_encoder(enc: &mut PayloadEncoder, correlation_id: &Uuid) {
    let bytes = correlation_id.as_bytes();
    let mut high = [0u8; 8];
    let mut low = [0u8; 8];
    high.copy_from_slice(&bytes[..8]);
    low.copy_from_slice(&bytes[8..]);
    enc.put_u64(u64::from_be_bytes(high));
    enc.put_u64(u64::from_be_bytes(low));
}

fn get_uuid(dec: &mut PayloadDecoder<'_>) -> Result<Uuid, String> {
    let high = dec.get_u64()?.to_be_bytes();
    let low = dec.get_u64()?.to_be_bytes();
    let mut uuid_bytes = [0u8; UUID_BYTES_LEN];
    uuid_bytes[..8].copy_from_slice(&high);
    uuid_bytes[8..].copy_from_slice(&low);
    Ok(Uuid::from_bytes(uuid_bytes))
}

fn skip_uuid(dec: &mut PayloadDecoder<'_>) -> Result<(), String> {
    dec.get_u64()?;
    dec.get_u64()?;
    Ok(())
}

fn put_error_body(buf: &mut BytesMut, code: u16, message: &str) {
    buf.put_u8(1);
    buf.put_u32(u32::from(code));
    put_payload_string(buf, message);
}

fn put_tlv_header(buf: &mut BytesMut, msg_type: MessageType, payload_len: usize) {
    assert!(
        u16::try_from(payload_len).is_ok(),
        "TLV value too large: {} bytes (max {})",
        payload_len,
        u16::MAX
    );

    if msg_type.is_single_byte() {
        buf.put_u8(u8::try_from(msg_type.as_u16()).unwrap_or(u8::MAX));
    } else {
        buf.put_u8(MessageType::ESCAPE_MARKER);
        buf.put_slice(&msg_type.as_u16().to_be_bytes());
    }
    buf.put_slice(&u16::try_from(payload_len).unwrap_or(u16::MAX).to_be_bytes());
}

fn tlv_frame_buffer(msg_type: MessageType, payload_len: usize) -> BytesMut {
    let mut buf = BytesMut::with_capacity(msg_type.encoded_size(payload_len));
    put_tlv_header(&mut buf, msg_type, payload_len);
    buf
}

/// Return the exact payload size for a standard RPC client response body.
#[must_use]
pub fn response_body_capacity(response: &RpcClientResponseBody) -> usize {
    match response {
        RpcClientResponseBody::Ok { data } => U8_LEN + encoded_bytes_len(data.len()),
        RpcClientResponseBody::CodeError { message, .. }
        | RpcClientResponseBody::Error(message) => error_body_capacity(message),
    }
}

/// Return the exact payload size for a standard Fitz error body.
#[must_use]
pub fn error_body_capacity(message: &str) -> usize {
    U8_LEN + U32_LEN + encoded_string_len(message)
}

/// Return the exact payload size for an RPC request delivery payload.
#[must_use]
pub fn request_payload_capacity(request: &RpcRequest) -> usize {
    UUID_BYTES_LEN
        + encoded_string_len(request.route.as_str())
        + encoded_bytes_len(request.body.len())
}

/// Return the exact payload size for an RPC response message payload.
#[must_use]
pub fn response_message_capacity(response: &RpcResponse) -> usize {
    UUID_BYTES_LEN + U64_LEN + U8_LEN + encoded_bytes_len(response.body.len())
}

/// Return the exact payload size for an RPC terminal error response payload.
#[must_use]
pub fn terminal_error_response_message_capacity(message: &str) -> usize {
    UUID_BYTES_LEN + U64_LEN + U8_LEN + encoded_bytes_len(error_body_capacity(message))
}

/// Return the exact frame size for a standard RPC client response body.
#[must_use]
pub fn client_response_frame_capacity(
    msg_type: MessageType,
    response: &RpcClientResponseBody,
) -> usize {
    msg_type.encoded_size(response_body_capacity(response))
}

/// Return the exact frame size for an RPC worker request delivery frame.
#[must_use]
pub fn worker_request_frame_capacity(request: &RpcRequest) -> usize {
    MessageType::new(302).encoded_size(request_payload_capacity(request))
}

/// Return the exact frame size for an RPC response message frame.
#[must_use]
pub fn response_message_frame_capacity(response: &RpcResponse) -> usize {
    MessageType::new(303).encoded_size(response_message_capacity(response))
}

/// Return the exact frame size for an RPC terminal error response frame.
#[must_use]
pub fn terminal_error_response_message_frame_capacity(message: &str) -> usize {
    MessageType::new(303).encoded_size(terminal_error_response_message_capacity(message))
}

/// Parse incoming message from TLV-encoded bytes.
///
/// `route_family` is injected by the session layer — it is never read
/// from the wire payload.
///
/// # Errors
///
/// Returns an error when the RPC message type is unsupported or the payload
/// cannot be decoded as the requested RPC operation.
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
) -> Result<RpcMessage, RpcDecodeError> {
    let mut dec = PayloadDecoder::new(payload);

    match ctx.msg_type.0 {
        300 => parse_subscribe(&mut dec, route_family),
        301 => parse_unsubscribe(&mut dec, route_family),
        302 => parse_rpc_request(&mut dec, route_family),
        303 => parse_rpc_response(&mut dec),
        304 => Err(RpcDecodeError::from(
            "Unsupported RPC operation: 304".to_string(),
        )),
        _ => Err(RpcDecodeError::from(format!(
            "Unknown operation: {}",
            ctx.msg_type.0
        ))),
    }
}

/// Extract the request route or worker address needed for authorization.
///
/// # Errors
///
/// Returns an error when the payload is malformed, has trailing data, or the
/// message type is unsupported for RPC authorization extraction.
pub fn extract_auth_route(msg_type: u16, payload: &[u8]) -> Result<Option<&str>, String> {
    let mut dec = PayloadDecoder::new(payload);

    match msg_type {
        300 => {
            let route = dec.get_string_ref()?;
            let max_concurrent = dec.get_u32()?;
            validate_max_concurrent(max_concurrent)?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        301 => {
            let route = dec.get_string_ref()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        302 => {
            skip_uuid(&mut dec)?;
            let route = dec.get_string_ref()?;
            dec.skip_bytes()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        303 => Ok(None),
        304 => Err("Unsupported RPC operation: 304".to_string()),
        _ => Err(format!("Unknown operation: {msg_type}")),
    }
}

/// Encode domain response to TLV-encoded bytes
#[must_use]
pub fn encode_response(response: &RpcClientResponseBody) -> Vec<u8> {
    let mut enc = PayloadEncoder::with_capacity(response_body_capacity(response));
    encode_response_into(response, &mut enc)
}

/// Encode domain response into a reusable payload encoder.
pub fn encode_response_into(response: &RpcClientResponseBody, enc: &mut PayloadEncoder) -> Vec<u8> {
    enc.clear();

    match response {
        RpcClientResponseBody::Ok { data } => {
            enc.put_u8(0); // success flag
            enc.put_bytes(data);
        }
        RpcClientResponseBody::CodeError { code, message } => {
            return encode_error_body_into(*code, message, enc);
        }
        RpcClientResponseBody::Error(e) => {
            return encode_error_body_into(
                crate::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
                e,
                enc,
            );
        }
    }

    enc.finish()
}

/// Encode a standard RPC error body with numeric code and message.
#[must_use]
pub fn encode_error_body(code: u16, message: &str) -> Vec<u8> {
    let mut enc = PayloadEncoder::with_capacity(error_body_capacity(message));
    encode_error_body_into(code, message, &mut enc)
}

/// Encode a standard RPC error body into a reusable payload encoder.
pub fn encode_error_body_into(code: u16, message: &str, enc: &mut PayloadEncoder) -> Vec<u8> {
    crate::protocol::error_codes::encode_error_body_into(code, message, enc)
}

/// Decode a standard RPC error body with numeric code and message.
///
/// # Errors
///
/// Returns an error when the error payload is truncated or malformed.
pub fn decode_error_body(payload: &[u8]) -> Result<(u16, String), String> {
    crate::protocol::error_codes::decode_error_body(payload)
}

/// Extract only the fixed-width request correlation ID from an RPC REQUEST
/// payload.
///
/// This is intentionally narrower than full request parsing so submit-time
/// failures can still be correlated when later fields are malformed.
///
/// # Errors
///
/// Returns an error when the payload is too short to contain the UUID.
pub fn extract_request_correlation_id(payload: &[u8]) -> Result<Uuid, String> {
    if payload.len() < UUID_BYTES_LEN {
        return Err("RPC request payload too short for correlation_id".to_string());
    }

    let mut uuid_bytes = [0u8; UUID_BYTES_LEN];
    uuid_bytes.copy_from_slice(&payload[..UUID_BYTES_LEN]);
    Ok(Uuid::from_bytes(uuid_bytes))
}

// ===== Helper Parsers =====

fn validate_max_concurrent(max_concurrent: u32) -> Result<usize, String> {
    if max_concurrent == 0 {
        return Err("RPC worker max_concurrent must be greater than zero".to_string());
    }
    if max_concurrent > MAX_WORKER_CONCURRENCY {
        return Err(format!(
            "RPC worker max_concurrent must be <= {MAX_WORKER_CONCURRENCY}"
        ));
    }

    Ok(max_concurrent as usize)
}

/// Wire format: `[string worker_addr][u32 max_concurrent]`
fn parse_subscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<RpcMessage, RpcDecodeError> {
    let pattern = dec.get_string_ref()?;
    crate::runtime::DomainKind::Rpc
        .descriptor()
        .compile_registration_pattern(pattern)
        .map_err(RpcDecodeError::invalid_registration_pattern)?;
    let worker_addr = RouteAddress::new(route_family, Route::new(pattern));
    let max_concurrent = validate_max_concurrent(dec.get_u32()?)?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string().into());
    }

    Ok(RpcMessage::RegisterWorker {
        worker_addr,
        max_concurrent,
    })
}

/// Wire format: `[string worker_addr]`
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<RpcMessage, RpcDecodeError> {
    let pattern = dec.get_string_ref()?;
    crate::runtime::DomainKind::Rpc
        .descriptor()
        .compile_registration_pattern(pattern)
        .map_err(RpcDecodeError::invalid_registration_pattern)?;
    let worker_addr = RouteAddress::new(route_family, Route::new(pattern));

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string().into());
    }

    Ok(RpcMessage::UnregisterWorker { worker_addr })
}

/// Wire format: `[uuid16 correlation_id][string route][bytes body]`
fn parse_rpc_request(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<RpcMessage, RpcDecodeError> {
    let correlation_id = get_uuid(dec)?;
    let route_value = dec.get_string_ref()?;
    let compiled = crate::runtime::DomainKind::Rpc
        .descriptor()
        .compile_registration_pattern(route_value)
        .map_err(RpcDecodeError::invalid_call_route)?;
    if compiled.is_wildcard() {
        return Err(RpcDecodeError::invalid_call_route(
            "RPC call route must be concrete",
        ));
    }
    let route = Route::new(route_value);
    let body = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string().into());
    }

    Ok(RpcMessage::Request(RpcRequest::new(
        route_family,
        correlation_id,
        route,
        body,
    )))
}

fn parse_rpc_response(dec: &mut PayloadDecoder) -> Result<RpcMessage, RpcDecodeError> {
    let correlation_id = get_uuid(dec)?;
    let seq = dec.get_u64()?;
    let flags = dec.get_u8()?;
    if flags & !RPC_RESPONSE_FLAGS_SUPPORTED != 0 {
        return Err(format!("Unsupported RPC response flags: {flags:#04x}").into());
    }
    let body = dec.get_bytes()?;
    let stream_end = flags & RPC_RESPONSE_FLAG_STREAM_END != 0;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string().into());
    }

    Ok(RpcMessage::Response(RpcResponse {
        correlation_id,
        seq,
        body,
        stream_end,
    }))
}

// ===== Encoders for Outbound Messages =====

/// Encode RPC REQUEST delivery to worker (message type 302)
///
/// Wire format: `[uuid16 correlation_id][string route][bytes body]`
///
/// This encodes the `RpcWorkItem` to be sent from route actor to worker session actor.
pub fn encode_request_delivery(work_item: &crate::dispatch::wire::rpc::RpcWorkItem) -> Vec<u8> {
    let capacity = UUID_BYTES_LEN
        + encoded_string_len(work_item.route.as_str())
        + encoded_bytes_len(work_item.body.len());
    let mut enc = PayloadEncoder::with_capacity(capacity);
    encode_request_delivery_into(work_item, &mut enc)
}

/// Encode an RPC request payload directly from `RpcRequest` for dispatch to a worker.
pub fn encode_request_into(request: &RpcRequest, enc: &mut PayloadEncoder) -> Vec<u8> {
    encode_request_fields_into(&request.correlation_id, &request.route, &request.body, enc)
}

/// Encode RPC REQUEST delivery using a reusable payload encoder.
pub fn encode_request_delivery_into(
    work_item: &crate::dispatch::wire::rpc::RpcWorkItem,
    enc: &mut PayloadEncoder,
) -> Vec<u8> {
    encode_request_fields_into(
        &work_item.correlation_id,
        &work_item.route,
        &work_item.body,
        enc,
    )
}

fn encode_request_fields_into(
    correlation_id: &Uuid,
    route: &Route,
    body: &[u8],
    enc: &mut PayloadEncoder,
) -> Vec<u8> {
    enc.clear();
    put_uuid_encoder(enc, correlation_id);
    enc.put_string(route.as_str());
    enc.put_bytes(body);
    enc.finish()
}

/// Encode RPC RESPONSE from worker to route (message type 303)
///
/// Wire format: `[uuid16 correlation_id][u64 seq][u8 flags][bytes body]`
pub fn encode_response_message(response: &RpcResponse) -> Vec<u8> {
    let mut enc = PayloadEncoder::with_capacity(response_message_capacity(response));
    encode_response_message_into(response, &mut enc)
}

/// Encode RPC RESPONSE from worker to route using a reusable payload encoder.
pub fn encode_response_message_into(response: &RpcResponse, enc: &mut PayloadEncoder) -> Vec<u8> {
    enc.clear();
    put_uuid_encoder(enc, &response.correlation_id);
    enc.put_u64(response.seq);
    enc.put_u8(u8::from(response.stream_end) & RPC_RESPONSE_FLAG_STREAM_END);
    enc.put_bytes(&response.body);
    enc.finish()
}

/// Encode an RPC terminal error response message using reusable encoders.
pub fn encode_terminal_error_response_message_into(
    correlation_id: &Uuid,
    code: u16,
    message: &str,
    response_enc: &mut PayloadEncoder,
    error_enc: &mut PayloadEncoder,
) -> Vec<u8> {
    let error_body = encode_error_body_into(code, message, error_enc);
    response_enc.clear();
    put_uuid_encoder(response_enc, correlation_id);
    response_enc.put_u64(0);
    response_enc.put_u8(RPC_RESPONSE_FLAG_STREAM_END);
    response_enc.put_bytes(&error_body);
    response_enc.finish()
}

/// Encode a complete RPC client response TLV frame directly into the final wire buffer.
///
/// # Panics
///
/// Panics if the encoded RPC payload exceeds the TLV `u16` value-length limit.
#[must_use]
pub fn encode_client_response_tlv_frame(
    msg_type: MessageType,
    response: &RpcClientResponseBody,
) -> Bytes {
    let payload_len = response_body_capacity(response);
    let mut buf = tlv_frame_buffer(msg_type, payload_len);

    match response {
        RpcClientResponseBody::Ok { data } => {
            buf.put_u8(0);
            put_payload_bytes(&mut buf, data);
        }
        RpcClientResponseBody::CodeError { code, message } => {
            put_error_body(&mut buf, *code, message);
        }
        RpcClientResponseBody::Error(message) => {
            put_error_body(
                &mut buf,
                crate::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
                message,
            );
        }
    }

    debug_assert_eq!(
        buf.len(),
        client_response_frame_capacity(msg_type, response)
    );
    buf.freeze()
}

/// Encode a complete RPC worker request delivery (`302`) TLV frame directly into
/// the final wire buffer.
///
/// # Panics
///
/// Panics if the encoded RPC payload exceeds the TLV `u16` value-length limit.
#[must_use]
pub fn encode_worker_request_tlv_frame(request: &RpcRequest) -> Bytes {
    let msg_type = MessageType::new(302);
    let payload_len = request_payload_capacity(request);
    let mut buf = tlv_frame_buffer(msg_type, payload_len);

    put_uuid(&mut buf, &request.correlation_id);
    put_payload_string(&mut buf, request.route.as_str());
    put_payload_bytes(&mut buf, &request.body);

    debug_assert_eq!(buf.len(), worker_request_frame_capacity(request));
    buf.freeze()
}

/// Encode a complete RPC response message (`303`) TLV frame directly into the
/// final wire buffer.
///
/// # Panics
///
/// Panics if the encoded RPC payload exceeds the TLV `u16` value-length limit.
#[must_use]
pub fn encode_response_message_tlv_frame(response: &RpcResponse) -> Bytes {
    let msg_type = MessageType::new(303);
    let payload_len = response_message_capacity(response);
    let mut buf = tlv_frame_buffer(msg_type, payload_len);

    put_uuid(&mut buf, &response.correlation_id);
    buf.put_u64(response.seq);
    buf.put_u8(u8::from(response.stream_end) & RPC_RESPONSE_FLAG_STREAM_END);
    put_payload_bytes(&mut buf, &response.body);

    debug_assert_eq!(buf.len(), response_message_frame_capacity(response));
    buf.freeze()
}

/// Encode a complete RPC terminal error response (`303`) TLV frame directly into
/// the final wire buffer.
///
/// # Panics
///
/// Panics if the encoded RPC payload exceeds the TLV `u16` value-length limit.
#[must_use]
pub fn encode_terminal_error_response_message_tlv_frame(
    correlation_id: &Uuid,
    code: u16,
    message: &str,
) -> Bytes {
    let msg_type = MessageType::new(303);
    let payload_len = terminal_error_response_message_capacity(message);
    let error_body_len = error_body_capacity(message);
    let mut buf = tlv_frame_buffer(msg_type, payload_len);

    put_uuid(&mut buf, correlation_id);
    buf.put_u64(0);
    buf.put_u8(RPC_RESPONSE_FLAG_STREAM_END);
    put_u32_len(&mut buf, error_body_len);
    put_error_body(&mut buf, code, message);

    debug_assert_eq!(
        buf.len(),
        terminal_error_response_message_frame_capacity(message)
    );
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::tlv::TlvDecoder;

    fn decode_single_frame(frame: &[u8]) -> (MessageType, Bytes) {
        let decoder = TlvDecoder::new();
        let (record, consumed) = decoder.decode_one(frame).expect("decode frame");
        assert_eq!(consumed, frame.len());
        (record.msg_type, record.value)
    }

    #[test]
    fn should_encode_client_response_tlv_frame() {
        // Arrange
        let response = RpcClientResponseBody::Ok {
            data: b"accepted".to_vec(),
        };
        let expected_payload = encode_response(&response);

        // Act
        let frame = encode_client_response_tlv_frame(MessageType::new(302), &response);
        let (msg_type, payload) = decode_single_frame(&frame);

        // Assert
        assert_eq!(msg_type.as_u16(), 302);
        assert_eq!(payload.as_ref(), expected_payload.as_slice());
    }

    #[test]
    fn should_encode_worker_request_tlv_frame() {
        // Arrange
        let request = RpcRequest::new(
            RouteFamily::new(1),
            Uuid::new_v4(),
            Route::new("rpc://bench/service"),
            Bytes::from_static(b"ping"),
        );
        let mut encoder = PayloadEncoder::with_capacity(request_payload_capacity(&request));
        let expected_payload = encode_request_into(&request, &mut encoder);

        // Act
        let frame = encode_worker_request_tlv_frame(&request);
        let (msg_type, payload) = decode_single_frame(&frame);

        // Assert
        assert_eq!(msg_type.as_u16(), 302);
        assert_eq!(payload.as_ref(), expected_payload.as_slice());
    }

    #[test]
    fn should_encode_response_message_tlv_frame() {
        // Arrange
        let response = RpcResponse::single(Uuid::new_v4(), Bytes::from_static(b"pong"));
        let expected_payload = encode_response_message(&response);

        // Act
        let frame = encode_response_message_tlv_frame(&response);
        let (msg_type, payload) = decode_single_frame(&frame);

        // Assert
        assert_eq!(msg_type.as_u16(), 303);
        assert_eq!(payload.as_ref(), expected_payload.as_slice());
    }

    #[test]
    fn should_encode_terminal_error_response_tlv_frame() {
        // Arrange
        let correlation_id = Uuid::new_v4();
        let message = "worker unavailable";
        let mut response_encoder =
            PayloadEncoder::with_capacity(terminal_error_response_message_capacity(message));
        let mut error_encoder = PayloadEncoder::with_capacity(error_body_capacity(message));
        let expected_payload = encode_terminal_error_response_message_into(
            &correlation_id,
            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
            message,
            &mut response_encoder,
            &mut error_encoder,
        );

        // Act
        let frame = encode_terminal_error_response_message_tlv_frame(
            &correlation_id,
            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
            message,
        );
        let (msg_type, payload) = decode_single_frame(&frame);

        // Assert
        assert_eq!(msg_type.as_u16(), 303);
        assert_eq!(payload.as_ref(), expected_payload.as_slice());
    }

    #[test]
    fn should_reject_removed_ack_message_type() {
        // Arrange
        let frame = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            MessageType::new(304),
            Bytes::new(),
            RouteFamily::new(1),
        );

        // Act
        let result = parse_request(&frame, &frame.payload, RouteFamily::new(1));

        // Assert
        assert!(result.is_err());
    }
}
