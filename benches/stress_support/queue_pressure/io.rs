use super::super::durable::{
    complete, empty_legacy_success, error_code, payload, request, require_ok,
};
use super::super::types::{BenchFailure, FailureKind};
use fitz::benchkit::{build_queue_complete, build_queue_dequeue, build_queue_enqueue};
use fitz::protocol::error_codes::queue::ERR_QUEUE_FULL;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::future::Future;
use std::time::Duration;

pub(super) const ROUTE: &str = "queue://stress-bench/work/offered-pressure";

pub(super) async fn bounded<T>(
    future: impl Future<Output = Result<T, BenchFailure>>,
) -> Result<T, BenchFailure> {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .map_err(|_| BenchFailure {
            kind: FailureKind::Timeout,
            detail: "Queue pressure operation exceeded 60 seconds; outcome may be indeterminate"
                .into(),
        })?
}

pub(super) async fn enqueue(
    client: &mut TestClient,
    sequence: usize,
) -> Result<Option<u64>, BenchFailure> {
    let sequence =
        u64::try_from(sequence).map_err(|error| BenchFailure::validation(error.to_string()))?;
    let body = request(
        client,
        &build_queue_enqueue(ROUTE, &payload(0, sequence)),
        200,
    )
    .await?;
    if body.first().copied() != Some(0)
        && error_code(&body, "Queue ENQUEUE")? == u32::from(ERR_QUEUE_FULL)
    {
        return Ok(None);
    }
    require_ok(&body, "Queue ENQUEUE")?;
    let mut decoder = PayloadDecoder::new(&body);
    decoder.get_u8().map_err(BenchFailure::validation)?;
    let id = decoder.get_u64().map_err(BenchFailure::validation)?;
    complete(&decoder)?;
    Ok(Some(id))
}

pub(super) enum Delivery {
    Empty,
    Rejected,
    Acknowledged { sequence: usize, id: u64 },
}

pub(super) async fn consume(client: &mut TestClient) -> Result<Delivery, BenchFailure> {
    match reserve(client).await? {
        Reservation::Empty => Ok(Delivery::Empty),
        Reservation::Rejected => Ok(Delivery::Rejected),
        Reservation::Message {
            sequence,
            id,
            token,
        } => {
            acknowledge(client, id, token).await?;
            Ok(Delivery::Acknowledged { sequence, id })
        }
    }
}

pub(super) enum Reservation {
    Empty,
    Rejected,
    Message {
        sequence: usize,
        id: u64,
        token: u64,
    },
}

pub(super) async fn reserve(client: &mut TestClient) -> Result<Reservation, BenchFailure> {
    let body = request(client, &build_queue_dequeue(ROUTE), 202).await?;
    if body.first().copied() != Some(0)
        && error_code(&body, "Queue RESERVE")? == u32::from(ERR_QUEUE_FULL)
    {
        return Ok(Reservation::Rejected);
    }
    require_ok(&body, "Queue RESERVE")?;
    let mut decoder = PayloadDecoder::new(&body);
    decoder.get_u8().map_err(BenchFailure::validation)?;
    match decoder.get_u32().map_err(BenchFailure::validation)? {
        0 => {
            complete(&decoder)?;
            return Ok(Reservation::Empty);
        }
        1 => {}
        _ => return Err(BenchFailure::validation("expected one reserved message")),
    }
    let id = decoder.get_u64().map_err(BenchFailure::validation)?;
    let token = decoder.get_u64().map_err(BenchFailure::validation)?;
    let body = decoder.get_bytes().map_err(BenchFailure::validation)?;
    complete(&decoder)?;
    let sequence_bytes: [u8; 8] = body
        .get(8..16)
        .ok_or_else(|| BenchFailure::validation("truncated pressure payload"))?
        .try_into()
        .map_err(|error: std::array::TryFromSliceError| {
            BenchFailure::validation(error.to_string())
        })?;
    let sequence = u64::from_be_bytes(sequence_bytes);
    if body.as_ref() != payload(0, sequence) {
        return Err(BenchFailure::verification("Queue pressure payload changed"));
    }
    Ok(Reservation::Message {
        sequence: usize::try_from(sequence)
            .map_err(|error| BenchFailure::validation(error.to_string()))?,
        id,
        token,
    })
}

pub(super) async fn acknowledge(
    client: &mut TestClient,
    id: u64,
    token: u64,
) -> Result<(), BenchFailure> {
    let result = request(client, &build_queue_complete(ROUTE, id, token), 204).await?;
    empty_legacy_success(&result, "Queue ACK")
}
