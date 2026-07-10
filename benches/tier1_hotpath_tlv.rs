use bytes::Bytes;
use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder, TlvRef};

stress_allocator!();

const DECODE_ONE_BATCH_OPS: u64 = 256;
const DECODE_ITER_REUSE_BATCH_OPS: u64 = 16;

fn record_group(ctx: &mut StressContext, group: &str, payload_size: usize) {
    ctx.parameter("group", group);
    ctx.parameter("payload_size", payload_size);
}

fn mark_validated_micro(ctx: &mut StressContext) {
    ctx.metadata("validated_micro", "true");
}

macro_rules! encode_clear_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_tlv_encode_sizes", $size);
            mark_validated_micro(ctx);
            let payload = vec![0_u8; $size];
            let mut encoder = TlvEncoder::with_capacity(1024);

            ctx.measure($bench_name, || {
                encoder.clear();
                encoder.encode(MessageType::new(42), black_box(&payload));
                black_box(&encoder);
            });
        }
    };
}

encode_clear_bench!(
    should_encode_clear_encode_256b,
    "encode_clear_encode_256b",
    256
);

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
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            const RECORDS: usize = 64;
            record_group(ctx, "hotpath_tlv_decode_sizes", $size);
            ctx.parameter("records", RECORDS);
            let data = encoded_records($size, RECORDS);
            let decoder = TlvDecoder::new();
            let mut refs: Vec<TlvRef<'_>> = Vec::with_capacity(RECORDS);

            ctx.measure($bench_name, || {
                refs.clear();
                decoder
                    .decode_refs_into(black_box(&data), &mut refs)
                    .unwrap();
                black_box(&refs);
            });
        }
    };
}

macro_rules! decode_iter_reuse_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            const RECORDS: usize = 64;
            record_group(ctx, "hotpath_tlv_decode_sizes", $size);
            ctx.parameter("records", RECORDS);
            ctx.parameter("completed_unit", "decode_passes");
            ctx.parameter("logical_unit", "decode_pass");
            ctx.parameter("decoded_records_per_logical_operation", RECORDS.to_string());
            let data = encoded_records($size, RECORDS);
            let decoder = TlvDecoder::new();
            let mut checksum = 0usize;

            ctx.measure_batch($bench_name, DECODE_ITER_REUSE_BATCH_OPS, || {
                for _ in 0..DECODE_ITER_REUSE_BATCH_OPS {
                    let mut offset = 0usize;
                    let mut local_checksum = 0usize;
                    while offset < data.len() {
                        let (msg_type, slice, consumed) =
                            decoder.decode_one_ref(&data[offset..]).unwrap();
                        local_checksum = local_checksum
                            .wrapping_add(usize::from(msg_type.as_u16()))
                            .wrapping_add(slice.len());
                        offset += consumed;
                    }
                    checksum ^= local_checksum;
                }
                black_box(checksum);
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
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_tlv_decode_single", $size);
            mark_validated_micro(ctx);
            ctx.parameter("completed_unit", "decoded_records");
            ctx.parameter("logical_unit", "tlv_record");
            let mut encoder = TlvEncoder::with_capacity(256);
            let payload = vec![0_u8; $size];
            encoder.encode(MessageType::new(42), &payload);
            let data = encoder.finish();
            let decoder = TlvDecoder::new();

            ctx.measure_batch($bench_name, DECODE_ONE_BATCH_OPS, || {
                for _ in 0..DECODE_ONE_BATCH_OPS {
                    black_box(decoder.decode_one_ref(black_box(&data)).unwrap());
                }
            });
        }
    };
}

decode_one_bench!(should_decode_one_16b, "decode_one_16b", 16);
decode_one_bench!(should_decode_one_64b, "decode_one_64b", 64);
decode_one_bench!(should_decode_one_256b, "decode_one_256b", 256);

stress_main!();
