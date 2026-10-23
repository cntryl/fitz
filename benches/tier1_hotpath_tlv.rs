use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::protocol::wire2::header::{decode_message, encode_message, HeaderFlags};

stress_allocator!();

const DECODE_ONE_BATCH_OPS: u64 = 256;

fn record_group(ctx: &mut StressContext, group: &str, payload_size: usize) {
    ctx.parameter("group", group);
    ctx.parameter("payload_size", payload_size);
}

fn mark_validated_micro(ctx: &mut StressContext) {
    ctx.metadata("validated_micro", "true");
}

fn encoded_message(size: usize) -> Vec<u8> {
    let payload = vec![0_u8; size];
    encode_message(
        42,
        HeaderFlags::none(),
        std::num::NonZeroU64::new(7),
        &payload,
    )
}

macro_rules! encode_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_message_encode_sizes", $size);
            mark_validated_micro(ctx);
            let payload = vec![0_u8; $size];

            ctx.measure($bench_name, || {
                black_box(encode_message(
                    42,
                    HeaderFlags::none(),
                    std::num::NonZeroU64::new(7),
                    black_box(&payload),
                ));
            });
        }
    };
}

encode_bench!(should_encode_message_256b, "encode_message_256b", 256);

macro_rules! decode_one_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_message_decode_single", $size);
            mark_validated_micro(ctx);
            ctx.parameter("completed_unit", "decoded_messages");
            ctx.parameter("logical_unit", "message");
            let data = encoded_message($size);

            ctx.measure_batch($bench_name, DECODE_ONE_BATCH_OPS, || {
                for _ in 0..DECODE_ONE_BATCH_OPS {
                    black_box(decode_message(black_box(&data), u32::MAX).unwrap());
                }
            });
        }
    };
}

decode_one_bench!(should_decode_one_16b, "decode_one_16b", 16);
decode_one_bench!(should_decode_one_64b, "decode_one_64b", 64);
decode_one_bench!(should_decode_one_256b, "decode_one_256b", 256);

stress_main!();
