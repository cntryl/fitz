//! Bounded live TCP operations. These drivers make no persistence or replay claim.

#[path = "lease.rs"]
mod lease;
#[path = "notice.rs"]
mod notice;
#[path = "rpc.rs"]
mod rpc;

use crate::stress_support::types::{BenchFailure, Domain, StepOutcome};
use bytes::Bytes;
use fitz::protocol::payload_codec::PayloadDecoder;
use std::net::SocketAddr;

pub(crate) enum EphemeralDriver {
    Rpc(rpc::RpcDriver),
    Notice(notice::NoticeDriver),
    Lease(lease::LeaseDriver),
}

impl EphemeralDriver {
    pub(crate) async fn connect(
        domain: Domain,
        addr: SocketAddr,
        lane: usize,
    ) -> Result<Self, BenchFailure> {
        match domain {
            Domain::Rpc => rpc::RpcDriver::connect(addr, lane).await.map(Self::Rpc),
            Domain::Notice => notice::NoticeDriver::connect(addr, lane)
                .await
                .map(Self::Notice),
            Domain::Lease => lease::LeaseDriver::connect(addr, lane)
                .await
                .map(Self::Lease),
            _ => Err(BenchFailure::validation("domain is not ephemeral")),
        }
    }

    pub(crate) async fn step(&mut self, sequence: u64) -> Result<StepOutcome, BenchFailure> {
        match self {
            Self::Rpc(driver) => driver.step(sequence).await,
            Self::Notice(driver) => driver.step(sequence).await,
            Self::Lease(driver) => driver.step().await,
        }
    }

    pub(crate) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        match self {
            Self::Rpc(driver) => driver.verify().await,
            Self::Notice(driver) => driver.verify().await,
            Self::Lease(driver) => driver.verify().await,
        }
    }

    pub(crate) async fn close(self) -> Result<(), BenchFailure> {
        match self {
            Self::Rpc(driver) => driver.close().await,
            Self::Notice(driver) => driver.close().await,
            Self::Lease(driver) => driver.close().await,
        }
    }
}

pub(super) fn single_field(frame: &[u8]) -> Result<(u16, &[u8]), BenchFailure> {
    let first = *frame
        .first()
        .ok_or_else(|| BenchFailure::validation("empty TCP TLV frame"))?;
    let (message_type, length_offset) = if first == 0xFF {
        let bytes = frame
            .get(1..3)
            .ok_or_else(|| BenchFailure::validation("truncated extended TLV type"))?;
        (u16::from_be_bytes([bytes[0], bytes[1]]), 3)
    } else {
        (u16::from(first), 1)
    };
    let bytes = frame
        .get(length_offset..length_offset + 2)
        .ok_or_else(|| BenchFailure::validation("truncated TLV length"))?;
    let length = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
    let payload_offset = length_offset + 2;
    if frame.len() != payload_offset + length {
        return Err(BenchFailure::validation(
            "TCP frame must contain exactly one complete TLV field",
        ));
    }
    Ok((message_type, &frame[payload_offset..]))
}

pub(super) fn body_for(frame: &[u8], expected_type: u16) -> Result<&[u8], BenchFailure> {
    let (message_type, body) = single_field(frame)?;
    if message_type != expected_type {
        return Err(BenchFailure::validation(format!(
            "expected message type {expected_type}, received {message_type}"
        )));
    }
    Ok(body)
}

pub(super) fn finish(decoder: &PayloadDecoder<'_>) -> Result<(), BenchFailure> {
    if decoder.is_complete() {
        Ok(())
    } else {
        Err(BenchFailure::validation("trailing response payload bytes"))
    }
}

pub(super) fn payload(
    marker: [u8; 12],
    lane: usize,
    sequence: u64,
    serial: &mut u64,
) -> Result<Bytes, BenchFailure> {
    *serial = serial
        .checked_add(1)
        .ok_or_else(|| BenchFailure::verification("packet identity exhausted"))?;
    let lane = u64::try_from(lane).map_err(|error| BenchFailure::validation(error.to_string()))?;
    let mut body = Vec::with_capacity(256);
    body.extend_from_slice(&marker);
    body.extend_from_slice(&lane.to_be_bytes());
    body.extend_from_slice(&sequence.to_be_bytes());
    body.extend_from_slice(&serial.to_be_bytes());
    body.extend((0_u8..220).map(|byte| byte ^ 0xA5));
    Ok(Bytes::from(body))
}
