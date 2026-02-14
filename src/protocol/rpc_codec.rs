//! RPC domain codec - request/response operations
//!
//! Encodes/decodes TLV messages for the RPC domain.
//! Supports Subscribe, Unsubscribe, Request, Response operations.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.

use crate::domains::rpc::protocol::{RpcMessage, RpcRequest, RpcResponse};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use uuid::Uuid;

/// Response from RPC operations
#[derive(Debug, Clone)]
pub enum RpcResponseMsg {
    /// Operation succeeded with optional data
    Ok { data: Vec<u8> },
    /// Operation failed with error message
    Error(String),
}

/// Parse incoming message from TLV-encoded bytes.
///
/// `route_family` is injected by the session layer — it is never read
/// from the wire payload.
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
) -> Result<RpcMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type.0 {
        300 => parse_subscribe(&mut dec, route_family),
        301 => parse_unsubscribe(&mut dec, route_family),
        302 => parse_rpc_request(&mut dec, route_family),
        303 => parse_rpc_response(&mut dec),
        304 => parse_ack(&mut dec),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &RpcResponseMsg) -> Vec<u8> {
    let mut enc = TlvEncoder::new();

    match response {
        RpcResponseMsg::Ok { data } => {
            enc.put_u8(0); // success flag
            enc.put_bytes(data);
        }
        RpcResponseMsg::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

/// Wire format: `[string worker_addr]`
fn parse_subscribe(dec: &mut TlvDecoder, route_family: RouteFamily) -> Result<RpcMessage, String> {
    let worker_addr_str = dec.get_string()?;
    let worker_addr = RouteAddress::new(route_family, Route::new(worker_addr_str));

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(RpcMessage::Subscribe { worker_addr })
}

/// Wire format: `[string worker_addr]`
fn parse_unsubscribe(
    dec: &mut TlvDecoder,
    route_family: RouteFamily,
) -> Result<RpcMessage, String> {
    let worker_addr_str = dec.get_string()?;
    let worker_addr = RouteAddress::new(route_family, Route::new(worker_addr_str));

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(RpcMessage::Unsubscribe { worker_addr })
}

/// Wire format: `[bytes correlation_id][string route][string reply_route][bytes body]`
fn parse_rpc_request(
    dec: &mut TlvDecoder,
    route_family: RouteFamily,
) -> Result<RpcMessage, String> {
    let correlation_id_bytes = dec.get_bytes()?;
    if correlation_id_bytes.len() != 16 {
        return Err("Correlation ID must be 16 bytes (UUID)".to_string());
    }
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&correlation_id_bytes);
    let correlation_id = Uuid::from_bytes(uuid_bytes);

    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    let reply_route_str = dec.get_string()?;
    let reply_route = Route::new(reply_route_str);

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

fn parse_rpc_response(dec: &mut TlvDecoder) -> Result<RpcMessage, String> {
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

fn parse_ack(dec: &mut TlvDecoder) -> Result<RpcMessage, String> {
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
/// This encodes the RpcWorkItem to be sent from route actor to worker session actor.
pub fn encode_request_delivery(work_item: &crate::domains::rpc::protocol::RpcWorkItem) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.put_bytes(work_item.correlation_id.as_bytes());
    enc.put_string(work_item.route.as_str());
    enc.put_string(work_item.reply_route.as_str());
    enc.put_bytes(&work_item.body);
    enc.finish()
}
