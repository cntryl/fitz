use super::{complete, empty_success, error_code, payload, request, require_ok};
use crate::stress_support::types::{BenchFailure, StepOutcome};
use fitz::benchkit::{build_kv_begin, build_kv_commit, build_kv_put, build_kv_rollback};
use fitz::protocol::error_codes::kv::ERR_BUSY;
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::testkit::{TestClient, TlvFrameBuilder};
use std::net::SocketAddr;

const KEY_COUNT: usize = 32;

pub(crate) struct KvDriver {
    client: TestClient,
    route: String,
    lane: usize,
    expected: [Option<Vec<u8>>; KEY_COUNT],
}

impl KvDriver {
    pub(super) async fn connect(addr: SocketAddr, lane: usize) -> Result<Self, BenchFailure> {
        let mut driver = Self {
            client: TestClient::new(addr)
                .await
                .map_err(BenchFailure::transport)?,
            route: format!("kv://stress-bench/state/lane-{lane}"),
            lane,
            expected: std::array::from_fn(|_| None),
        };
        if driver.step(0).await? != StepOutcome::Completed {
            return Err(BenchFailure::verification(
                "KV seed transaction was rejected",
            ));
        }
        Ok(driver)
    }

    async fn begin(&mut self, mode: u8) -> Result<Vec<u8>, BenchFailure> {
        request(&mut self.client, &build_kv_begin(&self.route, mode, 1), 100).await
    }

    fn transaction(body: &[u8]) -> Result<u64, BenchFailure> {
        require_ok(body)?;
        let mut decoder = PayloadDecoder::new(body);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        let id = decoder.get_u64().map_err(BenchFailure::validation)?;
        complete(&decoder)?;
        Ok(id)
    }

    pub(super) async fn step(&mut self, sequence: u64) -> Result<StepOutcome, BenchFailure> {
        let begin = self.begin(1).await?;
        if begin.first().copied() != Some(0) && error_code(&begin)? == u32::from(ERR_BUSY) {
            return Ok(StepOutcome::CapacityRejected(u32::from(ERR_BUSY)));
        }
        let tx_id = Self::transaction(&begin)?;
        let slot = usize::try_from(sequence % 32)
            .map_err(|error| BenchFailure::validation(error.to_string()))?;
        let key = format!("key-{slot}");
        let expected = payload(self.lane, sequence);
        let put = request(
            &mut self.client,
            &build_kv_put(tx_id, &self.route, key.as_bytes(), &expected),
            104,
        )
        .await?;
        empty_success(&put)?;
        let committed =
            request(&mut self.client, &build_kv_commit(tx_id, &self.route), 101).await?;
        empty_success(&committed)?;
        self.expected[slot] = Some(expected);
        self.verify_slot(slot).await?;
        Ok(StepOutcome::Completed)
    }

    async fn verify_slot(&mut self, slot: usize) -> Result<(), BenchFailure> {
        let begin = self.begin(0).await?;
        let tx_id = Self::transaction(&begin)?;
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(tx_id);
        encoder.put_string(&self.route);
        encoder.put_bytes(format!("key-{slot}").as_bytes());
        let mut frame = TlvFrameBuilder::new();
        frame.encode_field(103, &encoder.finish());
        let body = request(&mut self.client, &frame.build(), 103).await?;
        require_ok(&body)?;
        let mut decoder = PayloadDecoder::new(&body);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        let found = decoder.get_u8().map_err(BenchFailure::validation)?;
        let value = decoder.get_bytes().map_err(BenchFailure::validation)?;
        complete(&decoder)?;
        let expected = self.expected[slot]
            .as_ref()
            .ok_or_else(|| BenchFailure::verification("KV slot has no expected committed value"))?;
        let matches = found == 1 && value.as_ref() == expected.as_slice();
        let rolled_back = request(
            &mut self.client,
            &build_kv_rollback(tx_id, &self.route),
            102,
        )
        .await?;
        empty_success(&rolled_back)?;
        if !matches {
            return Err(BenchFailure::verification(format!(
                "KV committed readback changed slot {slot}"
            )));
        }
        Ok(())
    }

    pub(super) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        let mut checked = 0;
        for slot in 0..KEY_COUNT {
            if self.expected[slot].is_some() {
                self.verify_slot(slot).await?;
                checked += 1;
            }
        }
        if checked == 0 {
            return Err(BenchFailure::verification(
                "KV verification had no committed values",
            ));
        }
        Ok(checked)
    }

    pub(super) async fn close(self) -> Result<(), BenchFailure> {
        self.client.close().await.map_err(BenchFailure::transport)
    }
}
