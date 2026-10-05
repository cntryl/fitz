//! A fixed committed history is replayed and verified; retained history never grows.

use super::{complete, error_code, payload, request, require_ok, require_versioned_ok};
use crate::stress_support::types::{BenchFailure, StepOutcome};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_read_with_limit,
};
use fitz::protocol::error_codes::stream::{ERR_BUSY, ERR_RESOURCE_NOT_FOUND};
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::net::SocketAddr;

const HISTORY_LEN: u32 = 16;

pub(crate) struct StreamDriver {
    client: TestClient,
    route: String,
    lane: usize,
    read_frame: Vec<u8>,
}

impl StreamDriver {
    pub(super) async fn connect(addr: SocketAddr, lane: usize) -> Result<Self, BenchFailure> {
        let route = format!("stream://stress-bench/history/lane-{lane}");
        let mut driver = Self {
            client: TestClient::new(addr)
                .await
                .map_err(BenchFailure::transport)?,
            read_frame: build_stream_read_with_limit(&route, 0, u64::from(HISTORY_LEN) + 1),
            route,
            lane,
        };
        let body = driver.read().await?;
        let missing = body.first().copied() != Some(0)
            && error_code(&body)? == u32::from(ERR_RESOURCE_NOT_FOUND);
        if missing || driver.check_read(&body, true)? == 0 {
            driver.seed().await?;
        }
        driver.verify().await?;
        Ok(driver)
    }

    async fn seed(&mut self) -> Result<(), BenchFailure> {
        let begun = request(&mut self.client, &build_stream_begin(&self.route), 600).await?;
        require_versioned_ok(&begun, "Stream BEGIN")?;
        let mut decoder = PayloadDecoder::new(&begun);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        let session = decoder.get_u64().map_err(BenchFailure::validation)?;
        if !decoder
            .get_bytes()
            .map_err(BenchFailure::validation)?
            .is_empty()
        {
            return Err(BenchFailure::validation("unexpected Stream BEGIN data"));
        }
        complete(&decoder)?;
        for offset in 0..HISTORY_LEN {
            let body = payload(self.lane, u64::from(offset));
            let appended = request(
                &mut self.client,
                &build_stream_append(session, u64::from(offset), &body),
                601,
            )
            .await?;
            let data = Self::write_data(&appended, "Stream APPEND")?;
            let mut decoder = PayloadDecoder::new(&data);
            if decoder.get_u64().map_err(BenchFailure::validation)? != u64::from(offset) {
                return Err(BenchFailure::verification(
                    "Stream assigned the wrong offset",
                ));
            }
            complete(&decoder)?;
        }
        let committed = request(&mut self.client, &build_stream_commit(session, 1), 602).await?;
        if !Self::write_data(&committed, "Stream COMMIT")?.is_empty() {
            return Err(BenchFailure::validation("unexpected Stream COMMIT data"));
        }
        Ok(())
    }

    fn write_data(body: &[u8], operation: &str) -> Result<bytes::Bytes, BenchFailure> {
        require_versioned_ok(body, operation)?;
        let mut decoder = PayloadDecoder::new(body);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        let data = decoder.get_bytes().map_err(BenchFailure::validation)?;
        complete(&decoder)?;
        Ok(data)
    }

    async fn read(&mut self) -> Result<Vec<u8>, BenchFailure> {
        request(&mut self.client, &self.read_frame, 604).await
    }

    fn check_read(&self, body: &[u8], allow_empty: bool) -> Result<u64, BenchFailure> {
        require_ok(body, "Stream READ")?;
        let mut outer = PayloadDecoder::new(body);
        outer.get_u8().map_err(BenchFailure::validation)?;
        if outer
            .get_optional_u64()
            .map_err(BenchFailure::validation)?
            .is_some()
        {
            return Err(BenchFailure::validation(
                "Stream READ unexpectedly owns a session",
            ));
        }
        let data = outer.get_bytes().map_err(BenchFailure::validation)?;
        complete(&outer)?;
        let mut decoder = PayloadDecoder::new(&data);
        let count = decoder.get_u32().map_err(BenchFailure::validation)?;
        if count != HISTORY_LEN && !(allow_empty && count == 0) {
            return Err(BenchFailure::verification(format!(
                "Stream history has {count} events, expected {HISTORY_LEN}"
            )));
        }
        for offset in 0..count {
            let route = decoder.get_string_ref().map_err(BenchFailure::validation)?;
            let kind = decoder.get_u8().map_err(BenchFailure::validation)?;
            let resource_offset = decoder.get_u64().map_err(BenchFailure::validation)?;
            decoder
                .get_optional_u64()
                .map_err(BenchFailure::validation)?;
            decoder
                .get_optional_u64()
                .map_err(BenchFailure::validation)?;
            let body = decoder.get_bytes().map_err(BenchFailure::validation)?;
            let metadata = decoder
                .get_optional_bytes()
                .map_err(BenchFailure::validation)?;
            decoder.get_u64().map_err(BenchFailure::validation)?;
            if route != self.route
                || kind != 0
                || resource_offset != u64::from(offset)
                || body.as_ref() != payload(self.lane, u64::from(offset))
                || metadata.is_some()
            {
                return Err(BenchFailure::verification(
                    "Stream replay changed route, offset or body",
                ));
            }
        }
        let last_offset = decoder.get_u64().map_err(BenchFailure::validation)?;
        decoder
            .get_optional_u64()
            .map_err(BenchFailure::validation)?;
        decoder
            .get_optional_u64()
            .map_err(BenchFailure::validation)?;
        let has_more = decoder.get_u8().map_err(BenchFailure::validation)?;
        complete(&decoder)?;
        if has_more != 0 || (count > 0 && last_offset != u64::from(count - 1)) {
            return Err(BenchFailure::verification(
                "Stream replay cursor did not end at its history",
            ));
        }
        Ok(u64::from(count))
    }

    pub(super) async fn step(&mut self, _sequence: u64) -> Result<StepOutcome, BenchFailure> {
        let body = self.read().await?;
        if body.first().copied() != Some(0) && error_code(&body)? == u32::from(ERR_BUSY) {
            return Ok(StepOutcome::CapacityRejected(u32::from(ERR_BUSY)));
        }
        self.check_read(&body, false)?;
        Ok(StepOutcome::Completed)
    }

    pub(super) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        let body = self.read().await?;
        self.check_read(&body, false)
    }

    pub(super) async fn close(self) -> Result<(), BenchFailure> {
        self.client.close().await.map_err(BenchFailure::transport)
    }
}
