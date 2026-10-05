//! Exact-route ownership cycles with process-local fencing verification.

use super::{body_for, finish};
use crate::stress_support::types::{BenchFailure, StepOutcome};
use fitz::benchkit::{build_lease_acquire_immediate, build_lease_query, build_lease_release};
use fitz::protocol::error_codes::{decode_error_body, lease};
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::net::SocketAddr;

pub(crate) struct LeaseDriver {
    holder: TestClient,
    observer: TestClient,
    route: String,
    owner: String,
    last_token: Option<u64>,
}

enum Acquire {
    Acquired(u64),
    Contended,
}

impl LeaseDriver {
    pub(super) async fn connect(addr: SocketAddr, lane: usize) -> Result<Self, BenchFailure> {
        let holder = TestClient::new(addr)
            .await
            .map_err(BenchFailure::transport)?;
        let observer = TestClient::new(addr)
            .await
            .map_err(BenchFailure::transport)?;
        let mut driver = Self {
            holder,
            observer,
            route: format!("lease://stress-bench/load/lane-{lane}"),
            owner: format!("bench-owner-{lane}"),
            last_token: None,
        };
        driver.verify().await?;
        Ok(driver)
    }

    pub(super) async fn step(&mut self) -> Result<StepOutcome, BenchFailure> {
        let token = match self.acquire().await? {
            Acquire::Acquired(token) => token,
            Acquire::Contended => return Ok(StepOutcome::Contended),
        };
        self.release(token).await?;
        Ok(StepOutcome::Completed)
    }

    async fn acquire(&mut self) -> Result<Acquire, BenchFailure> {
        let frame = build_lease_acquire_immediate(&self.route, &self.owner, 30);
        let response = request(&mut self.holder, &frame).await?;
        let decision = parse_acquire(body_for(&response, 400)?)?;
        if let Acquire::Acquired(token) = decision {
            if self.last_token.is_some_and(|previous| token <= previous) {
                return Err(BenchFailure::verification(
                    "Lease reacquisition did not advance the process-local fencing token",
                ));
            }
            self.last_token = Some(token);
        }
        Ok(decision)
    }

    async fn release(&mut self, token: u64) -> Result<(), BenchFailure> {
        let response = request(
            &mut self.holder,
            &build_lease_release(&self.route, &self.owner, token),
        )
        .await?;
        if body_for(&response, 402)? != [0] {
            return Err(BenchFailure::verification(
                "Lease release was not successful",
            ));
        }
        Ok(())
    }

    async fn query(&mut self, expected_held: bool) -> Result<(), BenchFailure> {
        let response = request(&mut self.observer, &build_lease_query(&self.route)).await?;
        let mut decoder = PayloadDecoder::new(body_for(&response, 403)?);
        let status = decoder.get_u8().map_err(BenchFailure::validation)?;
        let held = decoder.get_u8().map_err(BenchFailure::validation)?;
        if status != 0 || held != u8::from(expected_held) {
            return Err(BenchFailure::verification(
                "Lease query holder state mismatch",
            ));
        }
        if expected_held {
            let owner = decoder.get_string_ref().map_err(BenchFailure::validation)?;
            let valid_owner = owner
                .strip_prefix("session:")
                .and_then(|value| value.split_once(':'))
                .is_some_and(|(session, owner)| {
                    session.parse::<u64>().is_ok() && owner == self.owner
                });
            if !valid_owner || decoder.get_u64().map_err(BenchFailure::validation)? == 0 {
                return Err(BenchFailure::verification("Lease query owner/TTL mismatch"));
            }
        }
        if decoder.get_u32().map_err(BenchFailure::validation)? != 0 {
            return Err(BenchFailure::verification(
                "unexpected Lease pending waiters",
            ));
        }
        finish(&decoder)
    }

    pub(super) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        self.query(false).await?;
        let Acquire::Acquired(token) = self.acquire().await? else {
            return Err(BenchFailure::verification("Lease probe could not acquire"));
        };
        self.query(true).await?;
        let response = request(
            &mut self.observer,
            &build_lease_acquire_immediate(&self.route, "bench-contender", 30),
        )
        .await?;
        if !matches!(
            parse_acquire(body_for(&response, 400)?)?,
            Acquire::Contended
        ) {
            return Err(BenchFailure::verification(
                "Lease allowed a second live holder during verification",
            ));
        }
        self.release(token).await?;
        if self.step().await? != StepOutcome::Completed {
            return Err(BenchFailure::verification(
                "Lease reacquisition probe failed",
            ));
        }
        self.query(false).await?;
        // Eight real operations check free/held state, exclusion, release and fencing.
        Ok(8)
    }

    pub(super) async fn close(self) -> Result<(), BenchFailure> {
        let (holder, observer) = tokio::join!(self.holder.close(), self.observer.close());
        holder.map_err(BenchFailure::transport)?;
        observer.map_err(BenchFailure::transport)
    }
}

async fn request(client: &mut TestClient, frame: &[u8]) -> Result<bytes::Bytes, BenchFailure> {
    client
        .send_frame(frame)
        .await
        .map_err(BenchFailure::transport)?;
    client
        .recv_frame_bytes_without_timeout()
        .await
        .map_err(BenchFailure::transport)
}

fn parse_acquire(body: &[u8]) -> Result<Acquire, BenchFailure> {
    if body.first().copied() == Some(1) {
        let (code, message) = decode_error_body(body).map_err(BenchFailure::validation)?;
        return if code == lease::ERR_LEASE_HELD {
            Ok(Acquire::Contended)
        } else {
            Err(BenchFailure::verification(format!(
                "unexpected Lease acquire error {code}: {message}"
            )))
        };
    }
    let mut decoder = PayloadDecoder::new(body);
    let status = decoder.get_u8().map_err(BenchFailure::validation)?;
    let response_type = decoder.get_u8().map_err(BenchFailure::validation)?;
    let token = decoder.get_u64().map_err(BenchFailure::validation)?;
    finish(&decoder)?;
    if status != 0 || response_type != 0 {
        return Err(BenchFailure::verification(
            "Lease acquire did not return Acquired",
        ));
    }
    Ok(Acquire::Acquired(token))
}
