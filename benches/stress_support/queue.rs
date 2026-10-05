use super::{complete, empty_success, error_code, payload, request, require_ok};
use crate::stress_support::types::{BenchFailure, StepOutcome};
use fitz::benchkit::{build_queue_complete, build_queue_dequeue, build_queue_enqueue};
use fitz::protocol::error_codes::queue::ERR_QUEUE_FULL;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::net::SocketAddr;

pub(crate) struct QueueDriver {
    client: TestClient,
    route: String,
    lane: usize,
}

impl QueueDriver {
    pub(super) async fn connect(addr: SocketAddr, lane: usize) -> Result<Self, BenchFailure> {
        let mut driver = Self {
            client: TestClient::new(addr)
                .await
                .map_err(BenchFailure::transport)?,
            route: format!("queue://stress-bench/work/lane-{lane}"),
            lane,
        };
        if driver.step(0).await? != StepOutcome::Completed {
            return Err(BenchFailure::verification(
                "Queue seed lifecycle was rejected",
            ));
        }
        Ok(driver)
    }

    pub(super) async fn step(&mut self, sequence: u64) -> Result<StepOutcome, BenchFailure> {
        let expected_body = payload(self.lane, sequence);
        let body = request(
            &mut self.client,
            &build_queue_enqueue(&self.route, &expected_body),
            200,
        )
        .await?;
        // Only ENQUEUE rejection can leave this lifecycle without an outstanding message.
        if body.first().copied() != Some(0) && error_code(&body)? == u32::from(ERR_QUEUE_FULL) {
            return Ok(StepOutcome::CapacityRejected(u32::from(ERR_QUEUE_FULL)));
        }
        require_ok(&body)?;
        let mut decoder = PayloadDecoder::new(&body);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        let enqueued_id = decoder.get_u64().map_err(BenchFailure::validation)?;
        complete(&decoder)?;

        let reserved = request(&mut self.client, &build_queue_dequeue(&self.route), 202).await?;
        require_ok(&reserved)?;
        let mut decoder = PayloadDecoder::new(&reserved);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        if decoder.get_u32().map_err(BenchFailure::validation)? != 1 {
            return Err(BenchFailure::verification(
                "Queue reserve must deliver one message",
            ));
        }
        let reserved_id = decoder.get_u64().map_err(BenchFailure::validation)?;
        let token = decoder.get_u64().map_err(BenchFailure::validation)?;
        let body = decoder.get_bytes().map_err(BenchFailure::validation)?;
        complete(&decoder)?;
        if reserved_id != enqueued_id || body.as_ref() != expected_body.as_slice() {
            return Err(BenchFailure::verification(
                "Queue message id or payload changed",
            ));
        }
        let acknowledged = request(
            &mut self.client,
            &build_queue_complete(&self.route, reserved_id, token),
            204,
        )
        .await?;
        empty_success(&acknowledged)?;
        Ok(StepOutcome::Completed)
    }

    pub(super) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        let body = request(&mut self.client, &build_queue_dequeue(&self.route), 202).await?;
        require_ok(&body)?;
        let mut decoder = PayloadDecoder::new(&body);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        if decoder.get_u32().map_err(BenchFailure::validation)? != 0 {
            return Err(BenchFailure::verification(
                "Queue retained work after successful ACKs",
            ));
        }
        complete(&decoder)?;
        Ok(1)
    }

    pub(super) async fn close(self) -> Result<(), BenchFailure> {
        self.client.close().await.map_err(BenchFailure::transport)
    }
}
