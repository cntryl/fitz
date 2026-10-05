//! Strict TCP workloads shared by the durable-domain saturation and endurance rows.

#[path = "kv.rs"]
mod kv;
#[path = "queue.rs"]
mod queue;
#[path = "schedule.rs"]
mod schedule;
#[path = "stream.rs"]
mod stream;

use crate::stress_support::types::{BenchFailure, Domain, StepOutcome};
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::net::SocketAddr;

const RESPONSE_TIMEOUT_MS: u64 = 10_000;
pub(super) const PAYLOAD_SIZE: usize = 1_024;

pub(crate) enum DurableDriver {
    Queue(queue::QueueDriver),
    Kv(Box<kv::KvDriver>),
    Stream(stream::StreamDriver),
    Schedule(schedule::ScheduleDriver),
}

impl DurableDriver {
    pub(crate) async fn connect(
        domain: Domain,
        addr: SocketAddr,
        lane: usize,
    ) -> Result<Self, BenchFailure> {
        if lane >= 64 {
            return Err(BenchFailure::validation("durable lane must be below 64"));
        }
        match domain {
            Domain::Queue => queue::QueueDriver::connect(addr, lane)
                .await
                .map(Self::Queue),
            Domain::Kv => kv::KvDriver::connect(addr, lane)
                .await
                .map(|driver| Self::Kv(Box::new(driver))),
            Domain::Stream => stream::StreamDriver::connect(addr, lane)
                .await
                .map(Self::Stream),
            Domain::Schedule => schedule::ScheduleDriver::connect(addr, lane)
                .await
                .map(Self::Schedule),
            _ => Err(BenchFailure::validation(
                "expected a durable-domain workload",
            )),
        }
    }

    pub(crate) async fn step(&mut self, sequence: u64) -> Result<StepOutcome, BenchFailure> {
        match self {
            Self::Queue(driver) => driver.step(sequence).await,
            Self::Kv(driver) => driver.step(sequence).await,
            Self::Stream(driver) => driver.step(sequence).await,
            Self::Schedule(driver) => driver.step(sequence).await,
        }
    }

    pub(crate) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        match self {
            Self::Queue(driver) => driver.verify().await,
            Self::Kv(driver) => driver.verify().await,
            Self::Stream(driver) => driver.verify().await,
            Self::Schedule(driver) => driver.verify().await,
        }
    }

    pub(crate) async fn close(self) -> Result<(), BenchFailure> {
        match self {
            Self::Queue(driver) => driver.close().await,
            Self::Kv(driver) => driver.close().await,
            Self::Stream(driver) => driver.close().await,
            Self::Schedule(driver) => driver.close().await,
        }
    }
}

pub(super) fn payload(lane: usize, sequence: u64) -> Vec<u8> {
    let mut body = vec![0xA5; PAYLOAD_SIZE];
    body[..8].copy_from_slice(&u64::try_from(lane).unwrap_or(u64::MAX).to_be_bytes());
    body[8..16].copy_from_slice(&sequence.to_be_bytes());
    body
}

/// The caller installs an absolute timeout covering send, receive and validation.
pub(super) async fn request(
    client: &mut TestClient,
    frame: &[u8],
    expected_type: u16,
) -> Result<Vec<u8>, BenchFailure> {
    let response = client
        .request(frame, RESPONSE_TIMEOUT_MS)
        .await
        .map_err(BenchFailure::transport)?;
    let (message_type, header_len, length_offset) = match response.first().copied() {
        Some(0xFF) if response.len() >= 5 => (u16::from_be_bytes([response[1], response[2]]), 5, 3),
        Some(message_type) if response.len() >= 3 && message_type != 0xFF => {
            (u16::from(message_type), 3, 1)
        }
        _ => return Err(BenchFailure::validation("truncated TLV response header")),
    };
    let length = usize::from(u16::from_be_bytes([
        response[length_offset],
        response[length_offset + 1],
    ]));
    if message_type != expected_type || response.len() != header_len + length {
        return Err(BenchFailure::validation(format!(
            "expected one complete TLV type {expected_type}, got type {message_type} and {} bytes",
            response.len()
        )));
    }
    Ok(response[header_len..].to_vec())
}

pub(super) fn require_ok(body: &[u8], operation: &str) -> Result<(), BenchFailure> {
    require_coded_ok(body, operation, 1)
}

pub(super) fn require_versioned_ok(body: &[u8], operation: &str) -> Result<(), BenchFailure> {
    require_coded_ok(body, operation, 2)
}

fn require_coded_ok(body: &[u8], operation: &str, status: u8) -> Result<(), BenchFailure> {
    if body.first().copied() == Some(0) {
        return Ok(());
    }
    let (code, message) = coded_error(body, status)?;
    Err(BenchFailure::domain_error(format!(
        "{operation} failed: code {code}: {message}"
    )))
}

pub(super) fn error_code(body: &[u8]) -> Result<u32, BenchFailure> {
    coded_error(body, 1).map(|(code, _)| code)
}

fn coded_error(body: &[u8], status: u8) -> Result<(u32, &str), BenchFailure> {
    let mut decoder = PayloadDecoder::new(body);
    if decoder.get_u8().map_err(BenchFailure::validation)? != status {
        return Err(BenchFailure::validation("expected a coded error status"));
    }
    let code = decoder.get_u32().map_err(BenchFailure::validation)?;
    if code > u32::from(u16::MAX) {
        return Err(BenchFailure::validation("error code exceeds u16 range"));
    }
    let message = decoder.get_string_ref().map_err(BenchFailure::validation)?;
    complete(&decoder)?;
    Ok((code, message))
}

/// ACK and Schedule `CREATE/CANCEL/LIST_V2` retain the legacy plain envelope.
pub(super) fn require_plain_ok(body: &[u8], operation: &str) -> Result<(), BenchFailure> {
    if body.first().copied() == Some(0) {
        return Ok(());
    }
    let mut decoder = PayloadDecoder::new(body);
    if decoder.get_u8().map_err(BenchFailure::validation)? != 1 {
        return Err(BenchFailure::validation("expected a plain error status"));
    }
    let message = decoder.get_string_ref().map_err(BenchFailure::validation)?;
    complete(&decoder)?;
    Err(BenchFailure::domain_error(format!(
        "{operation} failed: {message}"
    )))
}

pub(super) fn complete(decoder: &PayloadDecoder<'_>) -> Result<(), BenchFailure> {
    if decoder.is_complete() {
        Ok(())
    } else {
        Err(BenchFailure::validation("trailing response payload bytes"))
    }
}

pub(super) fn empty_success(body: &[u8], operation: &str) -> Result<(), BenchFailure> {
    require_ok(body, operation)?;
    require_empty(body)
}

pub(super) fn empty_plain_success(body: &[u8], operation: &str) -> Result<(), BenchFailure> {
    require_plain_ok(body, operation)?;
    require_empty(body)
}

fn require_empty(body: &[u8]) -> Result<(), BenchFailure> {
    if body == [0] {
        Ok(())
    } else {
        Err(BenchFailure::validation("expected only the success status"))
    }
}
