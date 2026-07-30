use bytes::Bytes;

use crate::dispatch::wire::kv::{KvMessage, KvNotification, KvResourceScope, ScanQuery};
use crate::protocol::kv_codec::frame_and_routes::parse_scope;
use crate::protocol::payload_codec::PayloadDecoder;
use crate::runtime::routing::{Route, RouteFamily};

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn read_scope(
    decoder: &mut PayloadDecoder<'_>,
    route_family: RouteFamily,
) -> Result<KvResourceScope, String> {
    parse_scope(route_family, decoder.get_string_ref()?)
}

fn read_optional_bytes(decoder: &mut PayloadDecoder<'_>) -> Result<Option<Bytes>, String> {
    decoder.get_optional_bytes()
}

fn read_optional_limit(decoder: &mut PayloadDecoder<'_>) -> Result<Option<usize>, String> {
    match decoder.get_u8()? {
        0 => Ok(None),
        1 => usize::try_from(decoder.get_u32()?)
            .map(Some)
            .map_err(|_| "SCAN limit exceeds platform capacity".to_string()),
        value => Err(format!("Invalid SCAN limit discriminator: {value}")),
    }
}

fn read_bool(decoder: &mut PayloadDecoder<'_>, field: &str) -> Result<bool, String> {
    match decoder.get_u8()? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(format!("Invalid {field} flag: {value}")),
    }
}

fn ensure_complete(decoder: &PayloadDecoder<'_>, op_name: &str) -> Result<(), String> {
    if decoder.is_complete() {
        Ok(())
    } else {
        Err(format!("Trailing data in {op_name} payload"))
    }
}

#[must_use]
pub fn encode_notify(subscription_id: u64, route: &Route, notification: KvNotification) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u64(subscription_id);
    buf.put_u32(usize_to_u32_saturating(route.as_str().len()));
    buf.put_slice(route.as_str().as_bytes());
    buf.put_u64(notification.mutation_count);
    buf
}

pub(super) fn parse_get(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    let mut decoder = PayloadDecoder::new(payload);
    let tx_id = decoder.get_u64()?;
    let scope = read_scope(&mut decoder, route_family)?;
    let key = decoder.get_bytes()?;
    ensure_complete(&decoder, "GET")?;
    Ok(KvMessage::Get { tx_id, scope, key })
}

pub(super) fn parse_put(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    let mut decoder = PayloadDecoder::new(payload);
    let tx_id = decoder.get_u64()?;
    let scope = read_scope(&mut decoder, route_family)?;
    let key = decoder.get_bytes()?;
    let value = decoder.get_bytes()?;
    ensure_complete(&decoder, "PUT")?;
    Ok(KvMessage::Put {
        tx_id,
        scope,
        key,
        value,
    })
}

pub(super) fn parse_insert(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    let mut decoder = PayloadDecoder::new(payload);
    let tx_id = decoder.get_u64()?;
    let scope = read_scope(&mut decoder, route_family)?;
    let key = decoder.get_bytes()?;
    let value = decoder.get_bytes()?;
    ensure_complete(&decoder, "INSERT")?;
    Ok(KvMessage::Insert {
        tx_id,
        scope,
        key,
        value,
    })
}

pub(super) fn parse_delete(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    let mut decoder = PayloadDecoder::new(payload);
    let tx_id = decoder.get_u64()?;
    let scope = read_scope(&mut decoder, route_family)?;
    let key = decoder.get_bytes()?;
    ensure_complete(&decoder, "DELETE")?;
    Ok(KvMessage::Delete { tx_id, scope, key })
}

pub(super) fn parse_delete_range(
    route_family: RouteFamily,
    payload: &[u8],
) -> Result<KvMessage, String> {
    let mut decoder = PayloadDecoder::new(payload);
    let tx_id = decoder.get_u64()?;
    let scope = read_scope(&mut decoder, route_family)?;
    let start = decoder.get_bytes()?;
    let end = decoder.get_bytes()?;
    ensure_complete(&decoder, "DELETE_RANGE")?;
    Ok(KvMessage::DeleteRange {
        tx_id,
        scope,
        start,
        end,
    })
}

pub(super) fn parse_scan(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    let mut decoder = PayloadDecoder::new(payload);
    let tx_id = decoder.get_u64()?;
    let scope = read_scope(&mut decoder, route_family)?;
    let start = read_optional_bytes(&mut decoder)?;
    let end = read_optional_bytes(&mut decoder)?;
    let limit = read_optional_limit(&mut decoder)?;
    let reverse = read_bool(&mut decoder, "SCAN reverse")?;
    ensure_complete(&decoder, "SCAN")?;
    Ok(KvMessage::Scan {
        tx_id,
        scope,
        query: ScanQuery {
            start,
            end,
            limit,
            reverse,
        },
    })
}
