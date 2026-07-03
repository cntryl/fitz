//! RPC domain codec - request/response operations
//!
//! Encodes/decodes TLV messages for the RPC domain.
//! Supports Subscribe, Unsubscribe, Request, Response operations.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.

use crate::domains::rpc::protocol::{RpcClientResponseBody, RpcMessage, RpcRequest, RpcResponse};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use uuid::Uuid;

const UUID_BYTES_LEN: usize = 16;
const U8_LEN: usize = 1;
const U32_LEN: usize = 4;
const U64_LEN: usize = 8;

fn encoded_bytes_len(len: usize) -> usize {
    U32_LEN + len
}

fn encoded_string_len(value: &str) -> usize {
    encoded_bytes_len(value.len())
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
    encoded_bytes_len(UUID_BYTES_LEN)
        + encoded_string_len(request.route.as_str())
        + encoded_string_len(request.reply_route.as_str())
        + encoded_bytes_len(request.body.len())
}

/// Return the exact payload size for an RPC response message payload.
#[must_use]
pub fn response_message_capacity(response: &RpcResponse) -> usize {
    encoded_bytes_len(UUID_BYTES_LEN) + U64_LEN + encoded_bytes_len(response.body.len()) + U8_LEN
}

/// Return the exact payload size for an RPC terminal error response payload.
#[must_use]
pub fn terminal_error_response_message_capacity(message: &str) -> usize {
    encoded_bytes_len(UUID_BYTES_LEN)
        + U64_LEN
        + encoded_bytes_len(error_body_capacity(message))
        + U8_LEN
}

/// Return the exact payload size for an RPC ACK payload.
#[must_use]
pub fn ack_payload_capacity() -> usize {
    encoded_bytes_len(UUID_BYTES_LEN)
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
) -> Result<RpcMessage, String> {
    let mut dec = PayloadDecoder::new(payload);

    match ctx.msg_type.0 {
        300 => parse_subscribe(&mut dec, route_family),
        301 => parse_unsubscribe(&mut dec, route_family),
        302 => parse_rpc_request(&mut dec, route_family),
        303 => parse_rpc_response(&mut dec),
        304 => parse_ack(&mut dec),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
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
        300 | 301 => {
            let route = dec.get_string_ref()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        302 => {
            dec.skip_bytes()?;
            let route = dec.get_string_ref()?;
            dec.get_string_ref()?;
            dec.skip_bytes()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        303 | 304 => Ok(None),
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

// ===== Helper Parsers =====

/// Wire format: `[string worker_addr]`
fn parse_subscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<RpcMessage, String> {
    let worker_addr = RouteAddress::new(route_family, Route::new(dec.get_string_ref()?));

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(RpcMessage::RegisterWorker { worker_addr })
}

/// Wire format: `[string worker_addr]`
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<RpcMessage, String> {
    let worker_addr = RouteAddress::new(route_family, Route::new(dec.get_string_ref()?));

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(RpcMessage::UnregisterWorker { worker_addr })
}

/// Wire format: `[bytes correlation_id][string route][string reply_route][bytes body]`
fn parse_rpc_request(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<RpcMessage, String> {
    let correlation_id_bytes = dec.get_bytes()?;
    if correlation_id_bytes.len() != 16 {
        return Err("Correlation ID must be 16 bytes (UUID)".to_string());
    }
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&correlation_id_bytes);
    let correlation_id = Uuid::from_bytes(uuid_bytes);

    let route = Route::new(dec.get_string_ref()?);
    let reply_route = Route::new(dec.get_string_ref()?);

    let body = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(RpcMessage::Request(RpcRequest::new(
        route_family,
        correlation_id,
        route,
        reply_route,
        body,
    )))
}

fn parse_rpc_response(dec: &mut PayloadDecoder) -> Result<RpcMessage, String> {
    let correlation_id_bytes = dec.get_bytes()?;
    if correlation_id_bytes.len() != 16 {
        return Err("Correlation ID must be 16 bytes (UUID)".to_string());
    }
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&correlation_id_bytes);
    let correlation_id = Uuid::from_bytes(uuid_bytes);

    let seq = dec.get_u64()?;
    let body = dec.get_bytes()?;
    let stream_end = dec.get_u8()? != 0;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(RpcMessage::Response(RpcResponse {
        correlation_id,
        seq,
        body,
        stream_end,
    }))
}

fn parse_ack(dec: &mut PayloadDecoder) -> Result<RpcMessage, String> {
    let correlation_id_bytes = dec.get_bytes()?;
    if correlation_id_bytes.len() != 16 {
        return Err("Correlation ID must be 16 bytes (UUID)".to_string());
    }
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&correlation_id_bytes);
    let correlation_id = Uuid::from_bytes(uuid_bytes);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(RpcMessage::Ack { correlation_id })
}

// ===== Encoders for Outbound Messages =====

/// Encode RPC REQUEST delivery to worker (message type 302)
///
/// Wire format: `[bytes correlation_id][string route][string reply_route][bytes body]`
///
/// This encodes the `RpcWorkItem` to be sent from route actor to worker session actor.
pub fn encode_request_delivery(work_item: &crate::domains::rpc::protocol::RpcWorkItem) -> Vec<u8> {
    let capacity = encoded_bytes_len(UUID_BYTES_LEN)
        + encoded_string_len(work_item.route.as_str())
        + encoded_string_len(work_item.reply_route.as_str())
        + encoded_bytes_len(work_item.body.len());
    let mut enc = PayloadEncoder::with_capacity(capacity);
    encode_request_delivery_into(work_item, &mut enc)
}

/// Encode an RPC request payload directly from `RpcRequest` for dispatch to a worker.
pub fn encode_request_into(request: &RpcRequest, enc: &mut PayloadEncoder) -> Vec<u8> {
    encode_request_fields_into(
        &request.correlation_id,
        &request.route,
        &request.reply_route,
        &request.body,
        enc,
    )
}

/// Encode RPC REQUEST delivery using a reusable payload encoder.
pub fn encode_request_delivery_into(
    work_item: &crate::domains::rpc::protocol::RpcWorkItem,
    enc: &mut PayloadEncoder,
) -> Vec<u8> {
    encode_request_fields_into(
        &work_item.correlation_id,
        &work_item.route,
        &work_item.reply_route,
        &work_item.body,
        enc,
    )
}

fn encode_request_fields_into(
    correlation_id: &Uuid,
    route: &Route,
    reply_route: &Route,
    body: &[u8],
    enc: &mut PayloadEncoder,
) -> Vec<u8> {
    enc.clear();
    enc.put_bytes(correlation_id.as_bytes());
    enc.put_string(route.as_str());
    enc.put_string(reply_route.as_str());
    enc.put_bytes(body);
    enc.finish()
}

/// Encode RPC RESPONSE from worker to route (message type 303)
///
/// Wire format: `[bytes correlation_id][u64 seq][bytes body][u8 stream_end]`
pub fn encode_response_message(response: &RpcResponse) -> Vec<u8> {
    let mut enc = PayloadEncoder::with_capacity(response_message_capacity(response));
    encode_response_message_into(response, &mut enc)
}

/// Encode RPC RESPONSE from worker to route using a reusable payload encoder.
pub fn encode_response_message_into(response: &RpcResponse, enc: &mut PayloadEncoder) -> Vec<u8> {
    enc.clear();
    enc.put_bytes(response.correlation_id.as_bytes());
    enc.put_u64(response.seq);
    enc.put_bytes(&response.body);
    enc.put_u8(u8::from(response.stream_end));
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
    response_enc.put_bytes(correlation_id.as_bytes());
    response_enc.put_u64(0);
    response_enc.put_bytes(&error_body);
    response_enc.put_u8(1);
    response_enc.finish()
}

/// Encode RPC ACK to worker (message type 304)
///
/// Wire format: `[bytes correlation_id]`
///
/// Sent to acknowledge receipt of a worker's RESPONSE message (303).
/// This unblocks the worker so they can send additional responses.
#[must_use]
pub fn encode_ack(correlation_id: &Uuid) -> Vec<u8> {
    let mut enc = PayloadEncoder::with_capacity(ack_payload_capacity());
    encode_ack_into(correlation_id, &mut enc)
}

/// Encode RPC ACK using a reusable payload encoder.
pub fn encode_ack_into(correlation_id: &Uuid, enc: &mut PayloadEncoder) -> Vec<u8> {
    enc.clear();
    enc.put_bytes(correlation_id.as_bytes());
    enc.finish()
}
