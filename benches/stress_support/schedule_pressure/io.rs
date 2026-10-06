use super::super::durable::{complete, empty_legacy_success, payload, request, require_legacy_ok};
use super::super::types::BenchFailure;
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::testkit::{TestClient, TlvFrameBuilder, TlvFrameParser};
use std::future::Future;
use std::time::Duration;

pub const CRON: &str = "0 0 1 1 *";
pub const PATTERN: &str = "schedule://stress-bench/timing/**";

pub fn route(index: usize) -> String {
    format!("schedule://stress-bench/timing/item-{index:06}/run")
}

pub async fn bounded<T>(
    future: impl Future<Output = Result<T, BenchFailure>>,
) -> Result<T, BenchFailure> {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .map_err(|_| {
            BenchFailure::verification(
                "Schedule operation exceeded 60 seconds; outcome may be indeterminate",
            )
        })?
}

pub fn frame(kind: u16, route: &str) -> Vec<u8> {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(kind, &encoder.finish());
    builder.build()
}

pub async fn create(
    client: &mut TestClient,
    index: usize,
    generation: u64,
    mode: u8,
) -> Result<(), BenchFailure> {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(&route(index));
    encoder.put_string(CRON);
    encoder.put_u8(mode);
    encoder.put_bytes(&payload(index, generation));
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(700, &encoder.finish());
    empty_legacy_success(
        &bounded(request(client, &builder.build(), 700)).await?,
        "Schedule CREATE",
    )
}

pub async fn cancel(client: &mut TestClient, index: usize) -> Result<(), BenchFailure> {
    empty_legacy_success(
        &bounded(request(client, &frame(701, &route(index)), 701)).await?,
        "Schedule CANCEL",
    )
}

pub async fn subscribe(client: &mut TestClient) -> Result<u64, BenchFailure> {
    let body = bounded(request(client, &frame(703, PATTERN), 703)).await?;
    require_legacy_ok(&body, "Schedule SUBSCRIBE")?;
    let mut decoder = PayloadDecoder::new(&body);
    decoder.get_u8().map_err(BenchFailure::validation)?;
    let id = decoder
        .get_optional_u64()
        .map_err(BenchFailure::validation)?
        .filter(|id| *id > 0)
        .ok_or_else(|| BenchFailure::validation("missing subscription id"))?;
    complete(&decoder)?;
    Ok(id)
}

pub fn delivery(
    frame: &[u8],
    subscription: u64,
    generation: u64,
    count: usize,
) -> Result<usize, BenchFailure> {
    let mut parser = TlvFrameParser::new(frame);
    let (kind, body) = parser
        .next_field_ref()
        .ok_or_else(|| BenchFailure::validation("missing delivery field"))?;
    if kind != 705 || parser.next_field_ref().is_some() {
        return Err(BenchFailure::validation("expected one Schedule delivery"));
    }
    let mut decoder = PayloadDecoder::new(body);
    let id = decoder.get_u64().map_err(BenchFailure::validation)?;
    let received_route = decoder.get_string_ref().map_err(BenchFailure::validation)?;
    let body = decoder.get_bytes().map_err(BenchFailure::validation)?;
    complete(&decoder)?;
    let bytes: [u8; 8] = body
        .get(..8)
        .ok_or_else(|| BenchFailure::validation("truncated identity"))?
        .try_into()
        .map_err(|error: std::array::TryFromSliceError| {
            BenchFailure::validation(error.to_string())
        })?;
    let index = usize::try_from(u64::from_be_bytes(bytes))
        .map_err(|error| BenchFailure::validation(error.to_string()))?;
    if id != subscription
        || index >= count
        || received_route != route(index)
        || body.as_ref() != payload(index, generation)
    {
        return Err(BenchFailure::verification(
            "Schedule delivery identity or payload changed",
        ));
    }
    Ok(index)
}

pub async fn verify_definitions(
    client: &mut TestClient,
    count: usize,
    generation: u64,
    mode: u8,
) -> Result<(), BenchFailure> {
    let mut cursor: Option<String> = None;
    let mut found = 0;
    loop {
        let mut encoder = PayloadEncoder::new();
        encoder.put_optional_string(cursor.as_deref());
        encoder.put_optional_u64(Some(16));
        let mut builder = TlvFrameBuilder::new();
        builder.encode_field(707, &encoder.finish());
        let body = bounded(request(client, &builder.build(), 707)).await?;
        require_legacy_ok(&body, "Schedule LIST")?;
        let mut decoder = PayloadDecoder::new(&body);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        if decoder.get_u8().map_err(BenchFailure::validation)? != 1 {
            return Err(BenchFailure::validation("unknown list version"));
        }
        let has_more = decoder.get_u8().map_err(BenchFailure::validation)?;
        let next = decoder
            .get_optional_string()
            .map_err(BenchFailure::validation)?;
        let mut page = 0;
        loop {
            match decoder.get_u8().map_err(BenchFailure::validation)? {
                0 => break,
                1 => {}
                _ => return Err(BenchFailure::validation("invalid list marker")),
            }
            let received_route = decoder.get_string_ref().map_err(BenchFailure::validation)?;
            let cron = decoder.get_string_ref().map_err(BenchFailure::validation)?;
            let delivery_mode = decoder.get_u8().map_err(BenchFailure::validation)?;
            let body = decoder.get_bytes().map_err(BenchFailure::validation)?;
            if found >= count
                || page >= 16
                || received_route != route(found)
                || cron != CRON
                || delivery_mode != mode
                || body.as_ref() != payload(found, generation)
            {
                return Err(BenchFailure::verification(
                    "persisted definition set changed",
                ));
            }
            found += 1;
            page += 1;
        }
        complete(&decoder)?;
        match has_more {
            0 if next.is_none() && found == count => return Ok(()),
            1 if page > 0
                && next.as_deref() == Some(route(found - 1).as_str())
                && next != cursor =>
            {
                cursor = next;
            }
            _ => {
                return Err(BenchFailure::verification(
                    "invalid list continuation or missing definitions",
                ))
            }
        }
    }
}
