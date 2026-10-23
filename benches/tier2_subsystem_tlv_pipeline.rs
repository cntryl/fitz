#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use fitz::protocol::message::MessageType;
use fitz::protocol::mux::Mux;
use fitz::protocol::wire2::header::{decode_message, encode_message, HeaderFlags};

const PIPELINE_MESSAGES: usize = 256;
const PIPELINE_REPEAT_COUNT: usize = 65_536;

fn configure_decode_measurement(ctx: &mut StressContext) {
    ctx.parameter("completed_unit", "decoded_messages");
    ctx.parameter("logical_unit", "message");
}

fn configure_decode_route_measurement(ctx: &mut StressContext) {
    ctx.parameter("completed_unit", "routed_messages");
    ctx.parameter("logical_unit", "message");
}

/// One frame per message, as the wire carries them; the pipeline decodes a
/// batch of frames back to back.
fn encoded_frames(size: usize) -> Vec<Vec<u8>> {
    let payload = vec![0u8; size];
    (0..PIPELINE_MESSAGES)
        .map(|i| {
            // Message type 100 is assigned to the KV domain and is accepted by
            // the manifest-backed mux.
            encode_message(
                100,
                HeaderFlags::none(),
                std::num::NonZeroU64::new(u64::try_from(i + 1).unwrap_or(u64::MAX)),
                &payload,
            )
        })
        .collect()
}

fn decode_only(ctx: &mut StressContext, name: &str, size: usize) {
    let frames = encoded_frames(size);
    configure_decode_measurement(ctx);

    tier2_stress::measure_iterations(
        ctx,
        name,
        (PIPELINE_MESSAGES * PIPELINE_REPEAT_COUNT) as u64,
        || {
            for _ in 0..PIPELINE_REPEAT_COUNT {
                for frame in &frames {
                    black_box(decode_message(black_box(frame), u32::MAX).unwrap());
                }
            }
        },
    );
}

fn decode_then_mux_route_ref(ctx: &mut StressContext, name: &str, size: usize) {
    let frames = encoded_frames(size);
    let mut mux = Mux::new(PIPELINE_MESSAGES);
    configure_decode_route_measurement(ctx);

    tier2_stress::measure_iterations(
        ctx,
        name,
        (PIPELINE_MESSAGES * PIPELINE_REPEAT_COUNT) as u64,
        || {
            for _ in 0..PIPELINE_REPEAT_COUNT {
                for frame in &frames {
                    let (header, value) = decode_message(black_box(frame), u32::MAX).unwrap();
                    let cref = mux
                        .route_ref(MessageType::new(header.message_type), value)
                        .unwrap();
                    mux.release(cref.channel);
                }
            }
        },
    );
}

#[stress(tier = 2, name = "decode_only_16b")]
fn should_decode_only_16b(ctx: &mut StressContext) {
    decode_only(ctx, "decode_only_16b", 16);
}

#[stress(tier = 2, name = "decode_only_64b")]
fn should_decode_only_64b(ctx: &mut StressContext) {
    decode_only(ctx, "decode_only_64b", 64);
}

#[stress(tier = 2, name = "decode_only_256b")]
fn should_decode_only_256b(ctx: &mut StressContext) {
    decode_only(ctx, "decode_only_256b", 256);
}

#[stress(tier = 2, name = "decode_then_mux_route_ref_16b")]
fn should_decode_then_mux_route_ref_16b(ctx: &mut StressContext) {
    decode_then_mux_route_ref(ctx, "decode_then_mux_route_ref_16b", 16);
}

#[stress(tier = 2, name = "decode_then_mux_route_ref_64b")]
fn should_decode_then_mux_route_ref_64b(ctx: &mut StressContext) {
    decode_then_mux_route_ref(ctx, "decode_then_mux_route_ref_64b", 64);
}

#[stress(tier = 2, name = "decode_then_mux_route_ref_256b")]
fn should_decode_then_mux_route_ref_256b(ctx: &mut StressContext) {
    decode_then_mux_route_ref(ctx, "decode_then_mux_route_ref_256b", 256);
}

stress_main!();
