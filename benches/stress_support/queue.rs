use super::{complete, empty_legacy_success, error_code, payload, request, require_ok};
use crate::stress_support::types::{BenchFailure, StepOutcome};
use fitz::benchkit::{build_queue_complete, build_queue_dequeue, build_queue_enqueue};
use fitz::protocol::error_codes::queue::ERR_QUEUE_FULL;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::net::SocketAddr;
use std::time::Duration;

struct Reservation {
    id: u64,
    token: u64,
    body: Vec<u8>,
    capacity_rejections: u64,
}

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
        if !driver.step(0).await?.is_completed() {
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
        if body.first().copied() != Some(0)
            && error_code(&body, "Queue ENQUEUE")? == u32::from(ERR_QUEUE_FULL)
        {
            return Ok(StepOutcome::CapacityRejected(u32::from(ERR_QUEUE_FULL)));
        }
        require_ok(&body, "Queue ENQUEUE")?;
        let mut decoder = PayloadDecoder::new(&body);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        let enqueued_id = decoder.get_u64().map_err(BenchFailure::validation)?;
        complete(&decoder)?;

        let reservation = self.reserve().await?;
        if reservation.id != enqueued_id || reservation.body != expected_body {
            return Err(BenchFailure::verification(
                "Queue message id or payload changed",
            ));
        }
        let acknowledged = request(
            &mut self.client,
            &build_queue_complete(&self.route, reservation.id, reservation.token),
            204,
        )
        .await?;
        empty_legacy_success(&acknowledged, "Queue ACK")?;
        Ok(if reservation.capacity_rejections == 0 {
            StepOutcome::Completed
        } else {
            StepOutcome::CompletedWithCapacityRejections {
                code: u32::from(ERR_QUEUE_FULL),
                count: reservation.capacity_rejections,
            }
        })
    }

    async fn reserve(&mut self) -> Result<Reservation, BenchFailure> {
        let mut capacity_rejections = 0_u64;
        loop {
            let response =
                request(&mut self.client, &build_queue_dequeue(&self.route), 202).await?;
            if response.first().copied() != Some(0)
                && error_code(&response, "Queue RESERVE")? == u32::from(ERR_QUEUE_FULL)
            {
                capacity_rejections = capacity_rejections.saturating_add(1);
                tokio::time::sleep(reserve_backoff(capacity_rejections)).await;
                continue;
            }
            require_ok(&response, "Queue RESERVE")?;
            let mut decoder = PayloadDecoder::new(&response);
            decoder.get_u8().map_err(BenchFailure::validation)?;
            if decoder.get_u32().map_err(BenchFailure::validation)? != 1 {
                return Err(BenchFailure::verification(
                    "Queue reserve must deliver one message",
                ));
            }
            let id = decoder.get_u64().map_err(BenchFailure::validation)?;
            let token = decoder.get_u64().map_err(BenchFailure::validation)?;
            let body = decoder
                .get_bytes()
                .map_err(BenchFailure::validation)?
                .to_vec();
            complete(&decoder)?;
            return Ok(Reservation {
                id,
                token,
                body,
                capacity_rejections,
            });
        }
    }

    pub(super) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        let body = request(&mut self.client, &build_queue_dequeue(&self.route), 202).await?;
        require_ok(&body, "Queue verification RESERVE")?;
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

fn reserve_backoff(rejections: u64) -> Duration {
    Duration::from_millis(match rejections {
        0 | 1 => 5,
        2 => 10,
        3 => 20,
        4 => 40,
        5 => 80,
        _ => 160,
    })
}
