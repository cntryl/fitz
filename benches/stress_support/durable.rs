//! Strict TCP workloads shared by the durable-domain saturation and endurance rows.

#[path = "kv.rs"]
mod kv;
#[path = "queue.rs"]
mod queue;
#[path = "schedule.rs"]
mod schedule;
#[path = "stream.rs"]
mod stream;

pub(super) use super::response::{complete, empty_success, error_code, require_ok};
use crate::stress_support::types::{BenchFailure, Domain, StepOutcome};
use fitz::testkit::TestClient;
use std::net::SocketAddr;

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
    client
        .send_frame(frame)
        .await
        .map_err(BenchFailure::transport)?;
    let response = client
        .recv_frame_bytes_without_timeout()
        .await
        .map_err(BenchFailure::transport)?;
    response_payload(&response, expected_type)
}

pub(super) fn response_payload(
    response: &[u8],
    expected_type: u16,
) -> Result<Vec<u8>, BenchFailure> {
    let (header, value) = fitz::protocol::wire2::header::decode_message(response, u32::MAX)
        .map_err(|error| BenchFailure::validation(format!("malformed response frame: {error}")))?;
    if header.message_type != expected_type {
        return Err(BenchFailure::validation(format!(
            "expected one complete message of type {expected_type}, got type {}",
            header.message_type
        )));
    }
    Ok(value.to_vec())
}
