use bytes::Bytes;
use cntryl_stress::{black_box, stress_allocator, stress_main, stress_test, StressContext};
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder, TlvRecord};

stress_allocator!();

fn record_group(ctx: &mut StressContext, group: &str, payload_size: usize) {
    ctx.parameter("group", group);
    ctx.parameter("payload_size", payload_size);
}

macro_rules! encode_clear_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_tlv_encode_sizes", $size);
            let payload = vec![0_u8; $size];
            let mut encoder = TlvEncoder::with_capacity(1024);

            ctx.measure_micro(|| {
                encoder.clear();
                encoder.encode(MessageType::new(42), black_box(&payload));
                black_box(&encoder);
            });
        }
    };
}

macro_rules! encode_new_finish_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_tlv_encode_sizes", $size);
            let payload = vec![0_u8; $size];

            ctx.measure_micro(|| {
                let mut encoder = TlvEncoder::with_capacity(1024);
                encoder.encode(MessageType::new(42), black_box(&payload));
                let out: Bytes = encoder.finish();
                black_box(out);
            });
        }
    };
}

encode_clear_bench!(should_encode_clear_encode_0b, "encode_clear_encode_0b", 0);
encode_clear_bench!(
    should_encode_clear_encode_16b,
    "encode_clear_encode_16b",
    16
);
encode_clear_bench!(
    should_encode_clear_encode_64b,
    "encode_clear_encode_64b",
    64
);
encode_clear_bench!(
    should_encode_clear_encode_256b,
    "encode_clear_encode_256b",
    256
);
encode_new_finish_bench!(should_encode_new_finish_0b, "encode_new_finish_0b", 0);
encode_new_finish_bench!(should_encode_new_finish_16b, "encode_new_finish_16b", 16);
encode_new_finish_bench!(should_encode_new_finish_64b, "encode_new_finish_64b", 64);
encode_new_finish_bench!(should_encode_new_finish_256b, "encode_new_finish_256b", 256);

fn encoded_records(size: usize, records: usize) -> Bytes {
    let mut encoder = TlvEncoder::with_capacity(1024 * 8);
    let payload = vec![0_u8; size];
    for i in 0..records {
        encoder.encode(
            MessageType::new(u16::try_from(i).unwrap_or(u16::MAX)),
            &payload,
        );
    }
    encoder.finish()
}

macro_rules! decode_all_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            const RECORDS: usize = 64;
            record_group(ctx, "hotpath_tlv_decode_sizes", $size);
            ctx.parameter("records", RECORDS);
            let data = encoded_records($size, RECORDS);
            let decoder = TlvDecoder::new();

            ctx.measure_micro(|| {
                black_box(decoder.decode_all(black_box(&data)).unwrap());
            });
        }
    };
}

macro_rules! decode_iter_reuse_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            const RECORDS: usize = 64;
            record_group(ctx, "hotpath_tlv_decode_sizes", $size);
            ctx.parameter("records", RECORDS);
            let data = encoded_records($size, RECORDS);
            let decoder = TlvDecoder::new();
            let mut out: Vec<TlvRecord> = Vec::with_capacity(RECORDS);

            ctx.measure_micro(|| {
                out.clear();
                let mut offset = 0usize;
                while offset < data.len() {
                    let (record, consumed) = decoder.decode_one(&data[offset..]).unwrap();
                    out.push(record);
                    offset += consumed;
                }
                black_box(&out);
            });
        }
    };
}

decode_all_bench!(should_decode_all_0b_64recs, "decode_all_0b_64recs", 0);
decode_all_bench!(should_decode_all_16b_64recs, "decode_all_16b_64recs", 16);
decode_all_bench!(should_decode_all_64b_64recs, "decode_all_64b_64recs", 64);
decode_all_bench!(should_decode_all_256b_64recs, "decode_all_256b_64recs", 256);
decode_iter_reuse_bench!(
    should_decode_iter_reuse_0b_64recs,
    "decode_iter_reuse_0b_64recs",
    0
);
decode_iter_reuse_bench!(
    should_decode_iter_reuse_16b_64recs,
    "decode_iter_reuse_16b_64recs",
    16
);
decode_iter_reuse_bench!(
    should_decode_iter_reuse_64b_64recs,
    "decode_iter_reuse_64b_64recs",
    64
);
decode_iter_reuse_bench!(
    should_decode_iter_reuse_256b_64recs,
    "decode_iter_reuse_256b_64recs",
    256
);

macro_rules! decode_one_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress_test(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_tlv_decode_single", $size);
            let mut encoder = TlvEncoder::with_capacity(256);
            let payload = vec![0_u8; $size];
            encoder.encode(MessageType::new(42), &payload);
            let data = encoder.finish();
            let decoder = TlvDecoder::new();

            ctx.measure_micro(|| {
                black_box(decoder.decode_one(black_box(&data)).unwrap());
            });
        }
    };
}

decode_one_bench!(should_decode_one_0b, "decode_one_0b", 0);
decode_one_bench!(should_decode_one_16b, "decode_one_16b", 16);
decode_one_bench!(should_decode_one_64b, "decode_one_64b", 64);
decode_one_bench!(should_decode_one_256b, "decode_one_256b", 256);

stress_main!();
