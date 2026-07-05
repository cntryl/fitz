#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::protocol::mux::Mux;
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder};
use std::hint::black_box;

const PIPELINE_RECORDS: usize = 256;
const PIPELINE_REPEAT_COUNT: usize = 65_536;

fn encode_frame(records: usize, size: usize) -> bytes::Bytes {
    let mut encoder = TlvEncoder::with_capacity(1024 * 8);
    let payload = vec![0u8; size];
    for i in 0..records {
        encoder.encode(
            MessageType::new(u16::try_from(i).unwrap_or(u16::MAX)),
            &payload,
        );
    }
    encoder.finish()
}

fn decode_only(ctx: &mut StressContext, size: usize) {
    let data = encode_frame(PIPELINE_RECORDS, size);
    let decoder = TlvDecoder::new();
    let mut refs = Vec::with_capacity(PIPELINE_RECORDS);

    tier2_stress::measure_iterations(
        ctx,
        (PIPELINE_RECORDS * PIPELINE_REPEAT_COUNT) as u64,
        || {
            for _ in 0..PIPELINE_REPEAT_COUNT {
                refs.clear();
                decoder
                    .decode_refs_into(black_box(&data), &mut refs)
                    .unwrap();
                black_box(&refs);
            }
        },
    );
}

fn decode_then_mux_route_ref(ctx: &mut StressContext, size: usize) {
    let data = encode_frame(PIPELINE_RECORDS, size);
    let decoder = TlvDecoder::new();
    let mut refs = Vec::with_capacity(PIPELINE_RECORDS);
    let mut warm_refs = Vec::with_capacity(PIPELINE_RECORDS);
    decoder.decode_refs_into(&data, &mut warm_refs).unwrap();

    let mut mux = Mux::new(PIPELINE_RECORDS);
    for tlv_ref in &warm_refs {
        let cref = mux.route_ref(tlv_ref.ty, tlv_ref.value).unwrap();
        mux.release(cref.channel);
    }

    tier2_stress::measure_iterations(
        ctx,
        (PIPELINE_RECORDS * PIPELINE_REPEAT_COUNT) as u64,
        || {
            for _ in 0..PIPELINE_REPEAT_COUNT {
                refs.clear();
                decoder
                    .decode_refs_into(black_box(&data), &mut refs)
                    .unwrap();

                for tlv_ref in &refs {
                    let cref = mux.route_ref(tlv_ref.ty, tlv_ref.value).unwrap();
                    mux.release(cref.channel);
                }
            }
        },
    );
}

#[stress_test(tier = 2, name = "decode_only_16b")]
fn should_decode_only_16b(ctx: &mut StressContext) {
    decode_only(ctx, 16);
}

#[stress_test(tier = 2, name = "decode_only_64b")]
fn should_decode_only_64b(ctx: &mut StressContext) {
    decode_only(ctx, 64);
}

#[stress_test(tier = 2, name = "decode_only_256b")]
fn should_decode_only_256b(ctx: &mut StressContext) {
    decode_only(ctx, 256);
}

#[stress_test(tier = 2, name = "decode_then_mux_route_ref_16b")]
fn should_decode_then_mux_route_ref_16b(ctx: &mut StressContext) {
    decode_then_mux_route_ref(ctx, 16);
}

#[stress_test(tier = 2, name = "decode_then_mux_route_ref_64b")]
fn should_decode_then_mux_route_ref_64b(ctx: &mut StressContext) {
    decode_then_mux_route_ref(ctx, 64);
}

#[stress_test(tier = 2, name = "decode_then_mux_route_ref_256b")]
fn should_decode_then_mux_route_ref_256b(ctx: &mut StressContext) {
    decode_then_mux_route_ref(ctx, 256);
}

stress_main!();
