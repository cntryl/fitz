use super::super::durable::{
    complete, empty_legacy_success, error_code, payload, request, require_ok, response_payload,
};
use super::super::types::{BenchFailure, FailureKind};
use fitz::benchkit::{build_queue_complete, build_queue_dequeue, build_queue_enqueue};
use fitz::protocol::error_codes::queue::ERR_QUEUE_FULL;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::future::Future;
use std::time::{Duration, Instant};

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
    consume_observed(client, |_, _| Ok(())).await
}

pub(super) enum ConsumerOperation {
    Reserve,
    Ack,
}

pub(super) async fn consume_observed(
    client: &mut TestClient,
    mut observe: impl FnMut(ConsumerOperation, Duration) -> Result<(), BenchFailure>,
) -> Result<Delivery, BenchFailure> {
    let started = Instant::now();
    let reserved = reserve(client).await;
    observe(ConsumerOperation::Reserve, started.elapsed())?;
    match reserved? {
        Reservation::Empty => Ok(Delivery::Empty),
        Reservation::Rejected => Ok(Delivery::Rejected),
        Reservation::Message {
            sequence,
            id,
            token,
        } => {
            let started = Instant::now();
            let acknowledged = acknowledge(client, id, token).await;
            observe(ConsumerOperation::Ack, started.elapsed())?;
            acknowledged?;
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

#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AckProgress {
    DispatchStarted,
    FrameSent,
    FrameReceived,
    SuccessValidated,
    ErrorResponseValidated,
}

pub(super) async fn acknowledge_observed(
    client: &mut TestClient,
    id: u64,
    token: u64,
    mut observe: impl FnMut(AckProgress) -> Result<(), BenchFailure>,
) -> Result<(), BenchFailure> {
    let frame = build_queue_complete(ROUTE, id, token);
    observe(AckProgress::DispatchStarted)?;
    client
        .send_frame(&frame)
        .await
        .map_err(BenchFailure::transport)?;
    observe(AckProgress::FrameSent)?;
    let response = client
        .recv_frame_bytes_without_timeout()
        .await
        .map_err(BenchFailure::transport)?;
    validate_ack_response(&response, observe)
}

pub(super) fn validate_ack_response(
    response: &[u8],
    mut observe: impl FnMut(AckProgress) -> Result<(), BenchFailure>,
) -> Result<(), BenchFailure> {
    observe(AckProgress::FrameReceived)?;
    validate_ack_payload(&response_payload(response, 204)?, observe)
}

pub(super) fn validate_ack_payload(
    payload: &[u8],
    mut observe: impl FnMut(AckProgress) -> Result<(), BenchFailure>,
) -> Result<(), BenchFailure> {
    if let Err(error) = empty_legacy_success(payload, "Queue ACK") {
        if matches!(
            error.kind,
            crate::stress_support::types::FailureKind::DomainError
        ) {
            observe(AckProgress::ErrorResponseValidated)?;
        }
        return Err(error);
    }
    observe(AckProgress::SuccessValidated)
}
