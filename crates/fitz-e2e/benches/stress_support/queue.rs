use super::{complete, empty_success, error_code, payload, request, require_ok};
use crate::stress_support::types::{BenchFailure, StepOutcome};
use fitz::benchkit::{build_queue_complete, build_queue_dequeue, build_queue_enqueue};
use fitz::protocol::error_codes::queue::ERR_BUSY;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz_e2e::testkit::TestClient;
use std::collections::HashMap;
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
    pub(crate) async fn open(addr: SocketAddr, lane: usize) -> Result<Self, BenchFailure> {
        Ok(Self {
            client: TestClient::new(addr)
                .await
                .map_err(BenchFailure::transport)?,
            route: format!("queue://stress-bench/work/lane-{lane}"),
            lane,
        })
    }

    pub(super) async fn connect(addr: SocketAddr, lane: usize) -> Result<Self, BenchFailure> {
        let mut driver = Self::open(addr, lane).await?;
        if !driver.step(0).await?.is_completed() {
            return Err(BenchFailure::verification(
                "Queue seed lifecycle was rejected",
            ));
        }
        Ok(driver)
    }

    pub(super) async fn step(&mut self, sequence: u64) -> Result<StepOutcome, BenchFailure> {
        let Some(enqueued_id) = self.enqueue(sequence).await? else {
            return Ok(StepOutcome::CapacityRejected(u32::from(ERR_BUSY)));
        };
        self.acknowledge(enqueued_id, sequence).await
    }

    pub(crate) async fn enqueue(&mut self, sequence: u64) -> Result<Option<u64>, BenchFailure> {
        let expected_body = payload(self.lane, sequence);
        let body = request(
            &mut self.client,
            &build_queue_enqueue(&self.route, &expected_body),
            200,
        )
        .await?;
        // Only ENQUEUE rejection can leave this lifecycle without an outstanding message.
        if body.first().copied() != Some(0)
            && error_code(&body, "Queue ENQUEUE")? == u32::from(ERR_BUSY)
        {
            return Ok(None);
        }
        require_ok(&body, "Queue ENQUEUE")?;
        let mut decoder = PayloadDecoder::new(&body);
        decoder.get_u8().map_err(BenchFailure::validation)?;
        let enqueued_id = decoder.get_u64().map_err(BenchFailure::validation)?;
        complete(&decoder)?;

        Ok(Some(enqueued_id))
    }

    pub(crate) async fn acknowledge(
        &mut self,
        enqueued_id: u64,
        sequence: u64,
    ) -> Result<StepOutcome, BenchFailure> {
        let expected_body = payload(self.lane, sequence);
        let reservation = self
            .reserve_next()
            .await?
            .ok_or_else(|| BenchFailure::verification("Queue reserve must deliver one message"))?;
        if reservation.id != enqueued_id || reservation.body != expected_body {
            return Err(BenchFailure::verification(
                "Queue message id or payload changed",
            ));
        }
        self.ack(&reservation).await?;
        Ok(if reservation.capacity_rejections == 0 {
            StepOutcome::Completed
        } else {
            StepOutcome::CompletedWithCapacityRejections {
                code: u32::from(ERR_BUSY),
                count: reservation.capacity_rejections,
            }
        })
    }

    async fn ack(&mut self, reservation: &Reservation) -> Result<(), BenchFailure> {
        let acknowledged = request(
            &mut self.client,
            &build_queue_complete(&self.route, reservation.id, reservation.token),
            204,
        )
        .await?;
        empty_success(&acknowledged, "Queue ACK")
    }

    #[allow(dead_code)] // Only the mixed endurance target reads redeliveries after restart.
    pub(crate) async fn drain_redeliveries(
        &mut self,
        acknowledged: &mut HashMap<u64, u64>,
    ) -> Result<u64, BenchFailure> {
        let mut redelivered = 0;
        while let Some(reservation) = self.reserve_next().await? {
            let sequence = acknowledged.remove(&reservation.id).ok_or_else(|| {
                BenchFailure::verification(
                    "Queue redelivered an unknown or repeated ID after restart",
                )
            })?;
            if reservation.body != payload(self.lane, sequence) {
                return Err(BenchFailure::verification(
                    "Queue redelivery changed its payload",
                ));
            }
            self.ack(&reservation).await?;
            redelivered += 1;
        }
        Ok(redelivered)
    }

    async fn reserve_next(&mut self) -> Result<Option<Reservation>, BenchFailure> {
        let mut capacity_rejections = 0_u64;
        loop {
            let response =
                request(&mut self.client, &build_queue_dequeue(&self.route), 202).await?;
            if response.first().copied() != Some(0)
                && error_code(&response, "Queue RESERVE")? == u32::from(ERR_BUSY)
            {
                capacity_rejections = capacity_rejections.saturating_add(1);
                tokio::time::sleep(reserve_backoff(capacity_rejections)).await;
                continue;
            }
            require_ok(&response, "Queue RESERVE")?;
            let mut decoder = PayloadDecoder::new(&response);
            decoder.get_u8().map_err(BenchFailure::validation)?;
            match decoder.get_u32().map_err(BenchFailure::validation)? {
                0 => {
                    complete(&decoder)?;
                    return Ok(None);
                }
                1 => {}
                _ => {
                    return Err(BenchFailure::verification(
                        "Queue reserve exceeded one message",
                    ))
                }
            }
            let id = decoder.get_u64().map_err(BenchFailure::validation)?;
            let token = decoder.get_u64().map_err(BenchFailure::validation)?;
            let body = decoder
                .get_bytes()
                .map_err(BenchFailure::validation)?
                .to_vec();
            complete(&decoder)?;
            return Ok(Some(Reservation {
                id,
                token,
                body,
                capacity_rejections,
            }));
        }
    }

    pub(crate) async fn verify(&mut self) -> Result<u64, BenchFailure> {
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

    pub(crate) async fn close(self) -> Result<(), BenchFailure> {
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
