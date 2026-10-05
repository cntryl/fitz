//! One live RPC exchange per lane, using the explicit legacy wire contract.
//! The worker echoes its delivered opaque UUID; cancellation is not negotiated.

use super::{body_for, finish, payload};
use crate::stress_support::types::{BenchFailure, StepOutcome};
use bytes::Bytes;
use fitz::benchkit::{build_rpc_request, build_rpc_response_frame_bytes, build_rpc_subscribe};
use fitz::protocol::error_codes::{decode_error_body, rpc};
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::testkit::transport::TlvFrameBuilder;
use fitz::testkit::TestClient;
use std::net::SocketAddr;
use uuid::Uuid;

pub(crate) struct RpcDriver {
    worker: TestClient,
    caller: TestClient,
    route: String,
    lane: usize,
    serial: u64,
}

struct Response {
    body: Bytes,
}

enum FirstFrame {
    Worker(Bytes),
    Caller(Bytes),
}

impl RpcDriver {
    pub(super) async fn connect(addr: SocketAddr, lane: usize) -> Result<Self, BenchFailure> {
        let route = format!("rpc://stress-bench/load/lane-{lane}");
        let mut worker = TestClient::new(addr)
            .await
            .map_err(BenchFailure::transport)?;
        let caller = TestClient::new(addr)
            .await
            .map_err(BenchFailure::transport)?;
        worker
            .send_frame(&build_rpc_subscribe(&route))
            .await
            .map_err(BenchFailure::transport)?;
        let frame = worker
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(BenchFailure::transport)?;
        require_registration_success(&frame, 300)?;
        let mut driver = Self {
            worker,
            caller,
            route,
            lane,
            serial: 0,
        };
        driver.verify().await?;
        Ok(driver)
    }

    pub(super) async fn step(&mut self, sequence: u64) -> Result<StepOutcome, BenchFailure> {
        let expected = payload(*b"stress-bench", self.lane, sequence, &mut self.serial)?;
        let request_frame = build_rpc_request(&self.route, &expected);
        let caller_uuid = frame_uuid(body_for(&request_frame, 302)?)?;
        self.caller
            .send_frame(&request_frame)
            .await
            .map_err(BenchFailure::transport)?;
        // A rejection answers the caller without dispatching a worker request.
        // Neither connection has another outstanding operation at this boundary.
        let first = tokio::select! {
            frame = self.worker.recv_frame_bytes_without_timeout() => {
                FirstFrame::Worker(frame.map_err(BenchFailure::transport)?)
            }
            frame = self.caller.recv_frame_bytes_without_timeout() => {
                FirstFrame::Caller(frame.map_err(BenchFailure::transport)?)
            }
        };
        let worker_frame = match first {
            FirstFrame::Caller(frame) => {
                let response = parse_response(&frame, caller_uuid)?;
                let (code, message) =
                    decode_error_body(&response.body).map_err(BenchFailure::validation)?;
                return if code == rpc::ERR_RPC_BACKPRESSURE {
                    Ok(StepOutcome::CapacityRejected(u32::from(code)))
                } else {
                    Err(BenchFailure::verification(format!(
                        "RPC rejected before dispatch with {code}: {message}"
                    )))
                };
            }
            FirstFrame::Worker(frame) => frame,
        };
        let worker_uuid = validate_worker_request(&worker_frame, &self.route, &expected)?;
        self.worker
            .send_frame_bytes(build_rpc_response_frame_bytes(
                worker_uuid,
                expected.clone(),
            ))
            .await
            .map_err(BenchFailure::transport)?;
        let frame = self
            .caller
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(BenchFailure::transport)?;
        let response = parse_response(&frame, caller_uuid)?;
        if response.body != expected {
            return Err(BenchFailure::verification(
                "RPC response body did not match the dispatched lane/sequence/payload",
            ));
        }
        Ok(StepOutcome::Completed)
    }

    pub(super) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        if self.step(u64::MAX).await? != StepOutcome::Completed {
            return Err(BenchFailure::verification(
                "RPC readiness/recovery probe did not complete",
            ));
        }
        // A real dispatched request and terminal reply were decoded and compared.
        Ok(2)
    }

    pub(super) async fn close(mut self) -> Result<(), BenchFailure> {
        let unregister = async {
            let mut payload = PayloadEncoder::new();
            payload.put_string(&self.route);
            let mut frame = TlvFrameBuilder::new();
            frame.encode_field(301, &payload.finish());
            self.worker
                .send_frame(&frame.build())
                .await
                .map_err(BenchFailure::transport)?;
            let response = self
                .worker
                .recv_frame_bytes_without_timeout()
                .await
                .map_err(BenchFailure::transport)?;
            require_registration_success(&response, 301)
        }
        .await;
        let (worker, caller) = tokio::join!(self.worker.close(), self.caller.close());
        unregister?;
        worker.map_err(BenchFailure::transport)?;
        caller.map_err(BenchFailure::transport)
    }
}

fn require_registration_success(frame: &[u8], expected_type: u16) -> Result<(), BenchFailure> {
    let mut decoder = PayloadDecoder::new(body_for(frame, expected_type)?);
    if decoder.get_u8().map_err(BenchFailure::validation)? != 0 {
        return Err(BenchFailure::verification("RPC registration was rejected"));
    }
    if !decoder
        .get_bytes()
        .map_err(BenchFailure::validation)?
        .is_empty()
    {
        return Err(BenchFailure::validation("unexpected RPC registration data"));
    }
    finish(&decoder)
}

fn frame_uuid(body: &[u8]) -> Result<Uuid, BenchFailure> {
    Uuid::from_slice(
        body.get(..16)
            .ok_or_else(|| BenchFailure::validation("missing RPC correlation UUID"))?,
    )
    .map_err(|error| BenchFailure::validation(error.to_string()))
}

fn validate_worker_request(
    frame: &[u8],
    route: &str,
    expected: &Bytes,
) -> Result<Uuid, BenchFailure> {
    let body = body_for(frame, 302)?;
    let correlation = frame_uuid(body)?;
    let mut decoder = PayloadDecoder::new(&body[16..]);
    if decoder.get_string_ref().map_err(BenchFailure::validation)? != route
        || decoder.get_bytes().map_err(BenchFailure::validation)? != *expected
    {
        return Err(BenchFailure::verification(
            "RPC worker received a different route or lane/sequence/payload",
        ));
    }
    finish(&decoder)?;
    Ok(correlation)
}

fn parse_response(frame: &[u8], expected_uuid: Uuid) -> Result<Response, BenchFailure> {
    let body = body_for(frame, 303)?;
    let correlation = frame_uuid(body)?;
    let mut decoder = PayloadDecoder::new(&body[16..]);
    let seq = decoder.get_u64().map_err(BenchFailure::validation)?;
    let flags = decoder.get_u8().map_err(BenchFailure::validation)?;
    let response_body = decoder.get_bytes().map_err(BenchFailure::validation)?;
    finish(&decoder)?;
    if correlation != expected_uuid || seq != 0 || flags != 1 {
        return Err(BenchFailure::verification(
            "RPC response UUID, sequence, or terminal flag was invalid",
        ));
    }
    Ok(Response {
        body: response_body,
    })
}
