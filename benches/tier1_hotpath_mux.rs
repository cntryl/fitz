use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::protocol::mux::Mux;
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

macro_rules! decode_then_route_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, $size);
            let data = encoded_record($size);
            let decoder = TlvDecoder::new();
            let mut mux = Mux::new(1024);

            ctx.measure($bench_name, || {
                let (record, _) = decoder.decode_one(black_box(&data)).unwrap();
                let message = mux.route(record).unwrap();
                mux.release(message.channel);
                black_box(message);
            });
        }
    };
}

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

stress_main!();
