//! Notice completion means an observed live delivery, never a publish ACK.

use super::{body_for, finish, payload};
use crate::stress_support::types::{BenchFailure, StepOutcome};
use fitz::benchkit::{build_notice_publish, build_notice_subscribe, build_notice_unsubscribe};
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestClient;
use std::net::SocketAddr;
use std::time::Duration;

const DELIVERY_WINDOW: Duration = Duration::from_secs(5);

struct Connections {
    publisher: TestClient,
    subscriber: TestClient,
    subscription_id: u64,
}

pub(crate) struct NoticeDriver {
    connections: Option<Connections>,
    addr: SocketAddr,
    route: String,
    lane: usize,
    serial: u64,
}

impl NoticeDriver {
    pub(super) async fn connect(addr: SocketAddr, lane: usize) -> Result<Self, BenchFailure> {
        let route = format!("notice://stress-bench/load/lane-{lane}");
        let connections = Some(Connections::connect(addr, &route).await?);
        let mut driver = Self {
            connections,
            addr,
            route,
            lane,
            serial: 0,
        };
        driver.verify().await?;
        Ok(driver)
    }

    pub(super) async fn step(&mut self, sequence: u64) -> Result<StepOutcome, BenchFailure> {
        if self.connections.is_none() {
            self.connections = Some(Connections::connect(self.addr, &self.route).await?);
        }
        let expected = payload(*b"stress-bench", self.lane, sequence, &mut self.serial)?;
        let connections = self
            .connections
            .as_mut()
            .ok_or_else(|| BenchFailure::transport("Notice connections unavailable"))?;
        connections
            .publisher
            .send_frame(&build_notice_publish(&self.route, &expected))
            .await
            .map_err(BenchFailure::transport)?;
        let delivery = tokio::time::timeout(
            DELIVERY_WINDOW,
            connections.subscriber.recv_frame_bytes_without_timeout(),
        )
        .await;
        let frame = if let Ok(result) = delivery {
            result.map_err(BenchFailure::transport)?
        } else {
            // A canceled TCP read may have consumed a partial frame. Drop both
            // old sessions so a late delivery cannot match the next operation.
            if let Some(connections) = self.connections.take() {
                connections.close(false).await?;
            }
            return Ok(StepOutcome::DeliveryWindowMiss);
        };
        let mut decoder = PayloadDecoder::new(body_for(&frame, 504)?);
        let subscription_id = decoder.get_u64().map_err(BenchFailure::validation)?;
        let route = decoder.get_string_ref().map_err(BenchFailure::validation)?;
        let body = decoder.get_bytes().map_err(BenchFailure::validation)?;
        finish(&decoder)?;
        if subscription_id != connections.subscription_id || route != self.route || body != expected
        {
            return Err(BenchFailure::verification(
                "Notice delivery subscription, route, or lane/sequence/payload mismatch",
            ));
        }
        Ok(StepOutcome::Completed)
    }

    pub(super) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        if self.step(u64::MAX).await? != StepOutcome::Completed {
            return Err(BenchFailure::verification(
                "Notice readiness/recovery delivery probe was missing",
            ));
        }
        // A real notification identified the live subscription and exact payload.
        Ok(1)
    }

    pub(super) async fn close(self) -> Result<(), BenchFailure> {
        match self.connections {
            Some(connections) => connections.close(true).await,
            None => Ok(()),
        }
    }
}

impl Connections {
    async fn connect(addr: SocketAddr, route: &str) -> Result<Self, BenchFailure> {
        let publisher = TestClient::new(addr)
            .await
            .map_err(BenchFailure::transport)?;
        let mut subscriber = TestClient::new(addr)
            .await
            .map_err(BenchFailure::transport)?;
        subscriber
            .send_frame(&build_notice_subscribe(route))
            .await
            .map_err(BenchFailure::transport)?;
        let frame = subscriber
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(BenchFailure::transport)?;
        let mut decoder = PayloadDecoder::new(body_for(&frame, 501)?);
        if decoder.get_u8().map_err(BenchFailure::validation)? != 0 {
            return Err(BenchFailure::verification("Notice subscription rejected"));
        }
        let subscription_id = decoder
            .get_optional_u64()
            .map_err(BenchFailure::validation)?
            .ok_or_else(|| BenchFailure::validation("Notice subscription identity missing"))?;
        finish(&decoder)?;
        Ok(Self {
            publisher,
            subscriber,
            subscription_id,
        })
    }

    async fn close(mut self, unsubscribe: bool) -> Result<(), BenchFailure> {
        let unregister = async {
            if unsubscribe {
                self.subscriber
                    .send_frame(&build_notice_unsubscribe(self.subscription_id))
                    .await
                    .map_err(BenchFailure::transport)?;
                let response = self
                    .subscriber
                    .recv_frame_bytes_without_timeout()
                    .await
                    .map_err(BenchFailure::transport)?;
                if body_for(&response, 502)? != [0] {
                    return Err(BenchFailure::verification("Notice unsubscribe rejected"));
                }
            }
            Ok(())
        }
        .await;
        let (publisher, subscriber) = tokio::join!(self.publisher.close(), self.subscriber.close());
        unregister?;
        publisher.map_err(BenchFailure::transport)?;
        subscriber.map_err(BenchFailure::transport)
    }
}
