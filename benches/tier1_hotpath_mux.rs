use cntryl_stress::{black_box, stress_allocator, stress_main, stress_test, StressContext};
use fitz::protocol::mux::{Mux, MuxError};
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder};

stress_allocator!();

fn encoded_record(size: usize) -> bytes::Bytes {
    let mut encoder = TlvEncoder::with_capacity(256);
    let payload = vec![0_u8; size];
    encoder.encode(MessageType::new(120), &payload);
    encoder.finish()
}

fn record_group(ctx: &mut StressContext, payload_size: usize) {
    ctx.parameter("group", "hotpath_mux");
    ctx.parameter("payload_size", payload_size);
}

macro_rules! route_owning_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, $size);
            let data = encoded_record($size);
            let decoder = TlvDecoder::new();
            let (record, _) = decoder.decode_one(&data).unwrap();
            let mut mux = Mux::new(1024);

            ctx.measure_micro(|| {
                let message = mux.route(record.clone()).unwrap();
                mux.release(message.channel);
                black_box(message);
            });
        }
    };
}

macro_rules! route_ref_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, $size);
            let data = encoded_record($size);
            let decoder = TlvDecoder::new();
            let (message_type, payload, _) = decoder.decode_one_ref(&data).unwrap();
            let mut mux = Mux::new(1024);

            ctx.measure_micro(|| {
                let message = mux.route_ref(message_type, black_box(payload)).unwrap();
                mux.release(message.channel);
                black_box(message);
            });
        }
    };
}

macro_rules! decode_then_route_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, $size);
            let data = encoded_record($size);
            let decoder = TlvDecoder::new();
            let mut mux = Mux::new(1024);

            ctx.measure_micro(|| {
                let (record, _) = decoder.decode_one(black_box(&data)).unwrap();
                let message = mux.route(record).unwrap();
                mux.release(message.channel);
                black_box(message);
            });
        }
    };
}

macro_rules! release_after_route_ref_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, $size);
            let data = encoded_record($size);
            let decoder = TlvDecoder::new();
            let (message_type, payload, _) = decoder.decode_one_ref(&data).unwrap();
            let mut mux = Mux::new(1);

            ctx.measure_micro(|| {
                let message = mux.route_ref(message_type, payload).unwrap();
                mux.release(message.channel);
                black_box(mux.occupancy(message.channel));
            });
        }
    };
}

macro_rules! channel_full_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, $size);
            let data = encoded_record($size);
            let decoder = TlvDecoder::new();
            let (message_type, payload, _) = decoder.decode_one_ref(&data).unwrap();
            let mut mux = Mux::new(1);
            mux.route_ref(message_type, payload).unwrap();

            ctx.measure_micro(|| match mux.route_ref(message_type, payload) {
                Err(MuxError::ChannelFull(channel)) => black_box(channel),
                _ => panic!("expected ChannelFull"),
            });
        }
    };
}

route_owning_bench!(
    should_route_owning_record_clone_0b,
    "route_owning_record_clone_0b",
    0
);
route_owning_bench!(
    should_route_owning_record_clone_16b,
    "route_owning_record_clone_16b",
    16
);
route_owning_bench!(
    should_route_owning_record_clone_64b,
    "route_owning_record_clone_64b",
    64
);
route_ref_bench!(should_route_ref_zero_copy_0b, "route_ref_zero_copy_0b", 0);
route_ref_bench!(
    should_route_ref_zero_copy_16b,
    "route_ref_zero_copy_16b",
    16
);
route_ref_bench!(
    should_route_ref_zero_copy_64b,
    "route_ref_zero_copy_64b",
    64
);
decode_then_route_bench!(
    should_decode_then_route_owning_0b,
    "decode_then_route_owning_0b",
    0
);
decode_then_route_bench!(
    should_decode_then_route_owning_16b,
    "decode_then_route_owning_16b",
    16
);
decode_then_route_bench!(
    should_decode_then_route_owning_64b,
    "decode_then_route_owning_64b",
    64
);
release_after_route_ref_bench!(
    should_release_after_route_ref_0b,
    "release_after_route_ref_0b",
    0
);
release_after_route_ref_bench!(
    should_release_after_route_ref_16b,
    "release_after_route_ref_16b",
    16
);
release_after_route_ref_bench!(
    should_release_after_route_ref_64b,
    "release_after_route_ref_64b",
    64
);
channel_full_bench!(
    should_route_ref_channel_full_0b,
    "route_ref_channel_full_0b",
    0
);
channel_full_bench!(
    should_route_ref_channel_full_16b,
    "route_ref_channel_full_16b",
    16
);
channel_full_bench!(
    should_route_ref_channel_full_64b,
    "route_ref_channel_full_64b",
    64
);

stress_main!();
