//! Bounded definition create/list/cancel checks; this workload makes no firing claim.

use super::{complete, empty_legacy_success, payload, request, require_legacy_ok};
use crate::stress_support::types::{BenchFailure, StepOutcome};
use fitz::benchkit::build_schedule_create;
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::testkit::{TestClient, TlvFrameBuilder};
use std::net::SocketAddr;

const CRON: &str = "0 0 1 1 *";
const PAGE_LIMIT: u64 = 8;
const MAX_LIST_PAGES: usize = 9;

pub(crate) struct ScheduleDriver {
    client: TestClient,
    route: String,
    lane: usize,
    cancel_frame: Vec<u8>,
}

impl ScheduleDriver {
    pub(super) async fn connect(addr: SocketAddr, lane: usize) -> Result<Self, BenchFailure> {
        let route = format!("schedule://stress-bench/timing/lane-{lane}/run");
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(&route);
        let mut frame = TlvFrameBuilder::new();
        frame.encode_field(701, &encoder.finish());
        let mut driver = Self {
            client: TestClient::new(addr)
                .await
                .map_err(BenchFailure::transport)?,
            cancel_frame: frame.build(),
            route,
            lane,
        };
        if driver.step(0).await? != StepOutcome::Completed {
            return Err(BenchFailure::verification(
                "Schedule readiness lifecycle was rejected",
            ));
        }
        Ok(driver)
    }

    pub(super) async fn step(&mut self, sequence: u64) -> Result<StepOutcome, BenchFailure> {
        let expected = payload(self.lane, sequence);
        let created = request(
            &mut self.client,
            &build_schedule_create(&self.route, CRON, &expected),
            700,
        )
        .await?;
        // Schedule's mailbox rejection shares its backend error code: never guess capacity.
        empty_legacy_success(&created, "Schedule CREATE")?;
        self.check_definition(Some(&expected)).await?;
        let cancelled = request(&mut self.client, &self.cancel_frame, 701).await?;
        empty_legacy_success(&cancelled, "Schedule CANCEL")?;
        self.check_definition(None).await?;
        Ok(StepOutcome::Completed)
    }

    async fn check_definition(&mut self, expected: Option<&[u8]>) -> Result<(), BenchFailure> {
        let mut cursor = None;
        let mut last_route: Option<String> = None;
        let mut found = false;
        for _ in 0..MAX_LIST_PAGES {
            let mut encoder = PayloadEncoder::new();
            encoder.put_optional_string(cursor.as_deref());
            encoder.put_optional_u64(Some(PAGE_LIMIT));
            let mut frame = TlvFrameBuilder::new();
            frame.encode_field(707, &encoder.finish());
            let body = request(&mut self.client, &frame.build(), 707).await?;
            require_legacy_ok(&body, "Schedule LIST_V2")?;
            let mut decoder = PayloadDecoder::new(&body);
            decoder.get_u8().map_err(BenchFailure::validation)?;
            if decoder.get_u8().map_err(BenchFailure::validation)? != 1 {
                return Err(BenchFailure::validation(
                    "unsupported Schedule LIST version",
                ));
            }
            let has_more = decoder.get_u8().map_err(BenchFailure::validation)?;
            let next = decoder
                .get_optional_string()
                .map_err(BenchFailure::validation)?;
            let mut page_count = 0;
            loop {
                match decoder.get_u8().map_err(BenchFailure::validation)? {
                    0 => break,
                    1 => {}
                    _ => return Err(BenchFailure::validation("invalid Schedule entry marker")),
                }
                page_count += 1;
                if page_count > PAGE_LIMIT {
                    return Err(BenchFailure::validation(
                        "Schedule LIST exceeded page limit",
                    ));
                }
                let route = decoder.get_string_ref().map_err(BenchFailure::validation)?;
                let cron = decoder.get_string_ref().map_err(BenchFailure::validation)?;
                let mode = decoder.get_u8().map_err(BenchFailure::validation)?;
                let body = decoder.get_bytes().map_err(BenchFailure::validation)?;
                if last_route.as_deref().is_some_and(|last| route <= last) {
                    return Err(BenchFailure::verification(
                        "Schedule LIST did not advance in route order",
                    ));
                }
                last_route = Some(route.to_string());
                if route == self.route {
                    if found || expected.is_none() {
                        return Err(BenchFailure::verification(
                            "Schedule definition duplicated or survived cancellation",
                        ));
                    }
                    if cron != CRON || mode != 0 || Some(body.as_ref()) != expected {
                        return Err(BenchFailure::verification(
                            "Schedule stored definition changed",
                        ));
                    }
                    found = true;
                }
            }
            complete(&decoder)?;
            match has_more {
                0 => {
                    if next.is_some() || found != expected.is_some() {
                        return Err(BenchFailure::verification(
                            "Schedule LIST missed its expected definition",
                        ));
                    }
                    return Ok(());
                }
                1 if page_count > 0 && next.is_some() && next != cursor => cursor = next,
                _ => return Err(BenchFailure::validation("invalid Schedule continuation")),
            }
        }
        Err(BenchFailure::verification(
            "Schedule LIST exceeded its bounded route scan",
        ))
    }

    pub(super) async fn verify(&mut self) -> Result<u64, BenchFailure> {
        self.check_definition(None).await?;
        Ok(1)
    }

    pub(super) async fn close(self) -> Result<(), BenchFailure> {
        self.client.close().await.map_err(BenchFailure::transport)
    }
}
