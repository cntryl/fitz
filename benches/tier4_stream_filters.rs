#[path = "tier4_stream_support.rs"]
mod tier4_stream_support;
#[path = "tier4_stream_transport.rs"]
mod tier4_stream_transport;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_stream_support::{
    build_stream_append_with_discriminator, build_stream_read_with_filter, equality_filter,
    measure_operations, tag_row, LayerKind, ReadScope, RowDimensions, StorageProfile,
    TransportKind, CANONICAL_PAYLOAD_SIZE,
};
use crate::tier4_stream_transport::{
    begin_session, request_read_count, request_success, with_transport_client,
};
use cntryl_stress::{stress, StressContext};
use fitz::benchkit::{build_stream_begin, build_stream_commit};
use std::time::Instant;

const FILTER_HISTORY_DEPTH: usize = 10_000;
const FILTER_READ_LIMIT: usize = 48;

#[derive(Clone, Copy)]
enum FilterCase {
    Unfiltered,
    NoMatch,
    Match25Percent,
    MatchAll,
}

impl FilterCase {
    const fn label(self) -> &'static str {
        match self {
            Self::Unfiltered => "unfiltered",
            Self::NoMatch => "0",
            Self::Match25Percent => "2500",
            Self::MatchAll => "10000",
        }
    }

    const fn discriminator(self, offset: usize) -> &'static str {
        match self {
            Self::Match25Percent if offset.is_multiple_of(4) => "keep",
            Self::Unfiltered | Self::NoMatch | Self::Match25Percent => "drop",
            Self::MatchAll => "keep",
        }
    }

    const fn expected_page_items(self) -> usize {
        // Stream reads preserve the scanned page shape: non-matching events
        // are encoded as filtered items rather than omitted from the response.
        // The benchmark's `filter_match_count` dimension describes selectivity;
        // the transport response still contains one item per scanned offset.
        let _ = self;
        FILTER_READ_LIMIT
    }
}

fn dimensions(case: FilterCase) -> RowDimensions<'static> {
    RowDimensions {
        scenario: "filter_selectivity",
        storage_profile: StorageProfile::Memory,
        layer: LayerKind::Tcp,
        write_mode: "not_applicable",
        write_operation: "none",
        payload_size: CANONICAL_PAYLOAD_SIZE,
        history_depth: FILTER_HISTORY_DEPTH,
        read_limit: FILTER_READ_LIMIT,
        read_scope: ReadScope::Resource,
        route_count: 1,
        filter_match_count: case.label(),
        client_count: 1,
        workload_mix: "read_only",
        completed_unit: "read_request",
        gate_class: "characterization",
    }
}

fn measure_filter_case(ctx: &mut StressContext, case: FilterCase, measurement: &'static str) {
    tag_row(ctx, &dimensions(case));
    ctx.parameter(
        "filter_mode",
        if matches!(case, FilterCase::Unfiltered) {
            "unfiltered"
        } else {
            "discriminator_equals"
        },
    );
    with_transport_client(
        StorageProfile::Memory,
        TransportKind::Tcp,
        |runtime, _server, client| {
            let route = format!("stream://tier4-filter/{}/resource", case.label());
            let session_id = runtime.block_on(begin_session(client, &build_stream_begin(&route)));
            let payload = vec![0x5A; CANONICAL_PAYLOAD_SIZE];
            for offset in 0..FILTER_HISTORY_DEPTH {
                let append = build_stream_append_with_discriminator(
                    session_id,
                    offset as u64,
                    &payload,
                    case.discriminator(offset),
                );
                runtime.block_on(request_success(client, &append));
            }
            runtime.block_on(request_success(
                client,
                &build_stream_commit(
                    session_id,
                    crate::tier4_stream_support::STREAM_SYNC_COMMIT_MODE,
                ),
            ));
            let filter = equality_filter("keep");
            let read = build_stream_read_with_filter(
                &route,
                0,
                FILTER_READ_LIMIT as u64,
                (!matches!(case, FilterCase::Unfiltered)).then_some(&filter),
            );
            runtime.block_on(request_read_count(
                client,
                &read,
                case.expected_page_items(),
            ));

            measure_operations(ctx, measurement, 1, |latencies| {
                let started = Instant::now();
                runtime.block_on(request_read_count(
                    client,
                    &read,
                    case.expected_page_items(),
                ));
                latencies.push(started.elapsed());
            });
        },
    );
}

macro_rules! filter_row {
    ($name:ident, $measurement:literal, $case:expr) => {
        #[stress(tier = 4)]
        fn $name(ctx: &mut StressContext) {
            measure_filter_case(ctx, $case, $measurement);
        }
    };
}

filter_row!(
    should_characterize_unfiltered_10000_record_read,
    "unfiltered_10000_record_read",
    FilterCase::Unfiltered
);
filter_row!(
    should_characterize_zero_match_10000_record_filter,
    "zero_match_10000_record_filter",
    FilterCase::NoMatch
);
filter_row!(
    should_characterize_25_percent_match_10000_record_filter,
    "25_percent_match_10000_record_filter",
    FilterCase::Match25Percent
);
filter_row!(
    should_characterize_all_match_10000_record_filter,
    "all_match_10000_record_filter",
    FilterCase::MatchAll
);

cntryl_stress::stress_main!();
