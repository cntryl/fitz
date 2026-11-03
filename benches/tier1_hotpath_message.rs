use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::protocol::wire2::header::{decode_message, encode_message, HeaderFlags, HEADER_LEN};
use fitz::protocol::wire2::DEFAULT_MAX_MESSAGE_BYTES;
use fitz::protocol::wire3::extensions::{
    decode_core_payload, encode_capability_ids, encode_payload, ExtensionDirection, ExtensionField,
    CAPABILITY_IDS_FIELD_ID, CONNECT_MESSAGE_TYPE,
};
use fitz::protocol::wire3::header::decode_message as decode_wire3;

stress_allocator!();

const DECODE_ONE_BATCH_OPS: u64 = 256;

fn record_group(ctx: &mut StressContext, group: &str, payload_size: usize) {
    ctx.parameter("group", group);
    ctx.parameter("payload_size", payload_size);
}

fn mark_validated_micro(ctx: &mut StressContext) {
    ctx.metadata("validated_micro", "true");
}

fn encoded_messages(size: usize) -> Vec<Vec<u8>> {
    (0..64)
        .map(|index| {
            let mut payload = vec![0_u8; size];
            if let Some(first) = payload.first_mut() {
                *first = u8::try_from(index).expect("frame variant fits u8");
            }
            encode_message(
                42,
                HeaderFlags::none(),
                std::num::NonZeroU64::new(
                    u64::try_from(index).expect("frame variant fits u64") + 1,
                ),
                &payload,
            )
        })
        .collect()
}

macro_rules! encode_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr, $allocation_bytes:literal) => {
        #[stress(tier = 1, max_allocs_per_op = 1, max_bytes_per_op = $allocation_bytes)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_message_encode_sizes", $size);
            mark_validated_micro(ctx);
            ctx.metadata("row_class", "construction");
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

encode_bench!(should_encode_message_256b, "encode_message_256b", 256, 271);

macro_rules! decode_one_bench {
    ($fn_name:ident, $bench_name:literal, $size:expr) => {
        #[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_message_decode_single", $size);
            mark_validated_micro(ctx);
            ctx.parameter("completed_unit", "decoded_messages");
            ctx.parameter("logical_unit", "message");
            let frames = encoded_messages($size);
            let mut index = 0;

            ctx.measure_batch($bench_name, DECODE_ONE_BATCH_OPS, || {
                let mut checksum = 0_u64;
                for _ in 0..DECODE_ONE_BATCH_OPS {
                    let frame = black_box(&frames[index]);
                    let (header, value) = decode_message(frame, u32::MAX).unwrap();
                    checksum = checksum
                        .wrapping_add(u64::from(header.message_type))
                        .wrapping_add(header.correlation.unwrap().get())
                        .wrapping_add(u64::from(value[0]));
                    index = (index + 1) % frames.len();
                }
                black_box(checksum);
            });
        }
    };
}

decode_one_bench!(should_decode_one_16b, "decode_one_16b", 16);
decode_one_bench!(should_decode_one_64b, "decode_one_64b", 64);
decode_one_bench!(should_decode_one_256b, "decode_one_256b", 256);

const WIRE3_MIN_FRAME_LIMIT_BYTES: usize = 512 * 1024;
const WIRE3_DEFAULT_FRAME_LIMIT_BYTES: usize = DEFAULT_MAX_MESSAGE_BYTES as usize;
const WIRE3_CORE_BYTES: [u8; 64] = [7; 64];
const WIRE3_DECODE_BATCH_OPS: u64 = 65_536;
const WIRE3_EXTENSION_HEADER_BYTES: usize = 6;
const WIRE3_FRAME_VARIANTS: usize = 64;

fn record_wire3_case(
    ctx: &mut StressContext,
    case: &str,
    frame_size: usize,
    logical_unit: &str,
    frames_per_operation: u64,
) {
    ctx.parameter("group", "hotpath_wire3_codec");
    ctx.parameter("case", case);
    ctx.parameter("frame_size_bytes", frame_size);
    ctx.parameter("logical_unit", logical_unit);
    ctx.parameter("frames_per_logical_operation", frames_per_operation);
    ctx.metadata("validated_micro", "true");
}

fn wire3_core_only_frames() -> Vec<Vec<u8>> {
    (0..WIRE3_FRAME_VARIANTS)
        .map(|index| {
            let mut core = WIRE3_CORE_BYTES;
            core[0] = u8::try_from(index).expect("frame variant fits u8");
            encode_message(
                104,
                HeaderFlags::none(),
                std::num::NonZeroU64::new(
                    u64::try_from(index).expect("frame variant fits u64") + 1,
                ),
                &core,
            )
        })
        .collect()
}

fn wire3_one_unknown_extension_frames() -> Vec<Vec<u8>> {
    (0..WIRE3_FRAME_VARIANTS)
        .map(|index| {
            let mut core = WIRE3_CORE_BYTES;
            core[0] = u8::try_from(index).expect("frame variant fits u8");
            let fields = [ExtensionField {
                field_id: 4096,
                value: b"future",
            }];
            let payload = encode_payload(&core, 104, ExtensionDirection::ClientToBroker, &fields)
                .expect("valid unknown extension");
            encode_message(
                104,
                HeaderFlags::extensions_present(),
                std::num::NonZeroU64::new(
                    u64::try_from(index).expect("frame variant fits u64") + 1,
                ),
                payload.value.as_ref(),
            )
        })
        .collect()
}

fn wire3_many_tiny_unknown_extensions_frames(frame_limit_bytes: usize) -> Vec<Vec<u8>> {
    let value_length = frame_limit_bytes - HEADER_LEN;
    let field_count =
        (value_length - 4 - (WIRE3_EXTENSION_HEADER_BYTES + 1)) / WIRE3_EXTENSION_HEADER_BYTES;
    let trailing_value_length = value_length
        - 4
        - field_count * WIRE3_EXTENSION_HEADER_BYTES
        - WIRE3_EXTENSION_HEADER_BYTES;
    (0..4)
        .map(|variant| {
            let mut payload = Vec::with_capacity(value_length);
            payload.extend_from_slice(&0_u32.to_be_bytes());
            for _ in 0..field_count {
                payload.extend_from_slice(
                    &u16::try_from(4096 + variant)
                        .expect("unknown field ID fits u16")
                        .to_be_bytes(),
                );
                payload.extend_from_slice(&0_u32.to_be_bytes());
            }
            payload.extend_from_slice(
                &u16::try_from(4096 + variant)
                    .expect("unknown field ID fits u16")
                    .to_be_bytes(),
            );
            payload.extend_from_slice(
                &u32::try_from(trailing_value_length)
                    .expect("trailing extension value fits u32")
                    .to_be_bytes(),
            );
            payload.resize(
                payload.len() + trailing_value_length,
                u8::try_from(variant).expect("frame variant fits u8"),
            );
            assert_eq!(payload.len(), value_length);

            let frame = encode_message(
                104 + u16::try_from(variant).expect("frame variant fits u16"),
                HeaderFlags::extensions_present(),
                std::num::NonZeroU64::new(
                    u64::try_from(variant).expect("frame variant fits u64") + 1,
                ),
                &payload,
            );
            assert_eq!(frame.len(), frame_limit_bytes);
            frame
        })
        .collect()
}

fn wire3_max_capability_list_connect_frames() -> Vec<Vec<u8>> {
    let ids = (1..=u32::from(u16::MAX)).collect::<Vec<_>>();
    let capabilities = encode_capability_ids(&ids).expect("maximum capability count fits u16");
    (0..4)
        .map(|variant| {
            let core = [u8::try_from(variant).expect("frame variant fits u8")];
            let fields = [ExtensionField {
                field_id: CAPABILITY_IDS_FIELD_ID,
                value: &capabilities,
            }];
            let payload = encode_payload(
                &core,
                CONNECT_MESSAGE_TYPE,
                ExtensionDirection::ClientToBroker,
                &fields,
            )
            .expect("valid maximum capability list");
            encode_message(
                CONNECT_MESSAGE_TYPE,
                HeaderFlags::extensions_present(),
                std::num::NonZeroU64::new(
                    u64::try_from(variant).expect("frame variant fits u64") + 1,
                ),
                payload.value.as_ref(),
            )
        })
        .collect()
}

fn wire3_max_frame_extension_encode_inputs(frame_limit_bytes: usize) -> (Vec<u8>, Vec<u8>) {
    let core = vec![7_u8; 64];
    let extension_value_length = frame_limit_bytes - HEADER_LEN - 4 - core.len() - 6;
    let extension_value = vec![9_u8; extension_value_length];
    (core, extension_value)
}

#[stress(tier = 1, max_allocs_per_op = 2, max_bytes_per_op = 2_097_137)]
fn should_encode_a_default_limit_extension_frame(ctx: &mut StressContext) {
    let (core, extension_value) =
        wire3_max_frame_extension_encode_inputs(WIRE3_DEFAULT_FRAME_LIMIT_BYTES);
    let fields = [ExtensionField {
        field_id: 4096,
        value: &extension_value,
    }];
    record_wire3_case(
        ctx,
        "wire3_default_limit_extension_encode",
        WIRE3_DEFAULT_FRAME_LIMIT_BYTES,
        "frame",
        1,
    );
    ctx.metadata("row_class", "construction");
    ctx.measure("wire3_default_limit_extension_encode", || {
        let payload = encode_payload(&core, 104, ExtensionDirection::ClientToBroker, &fields)
            .expect("valid default-limit extension payload");
        let frame = encode_message(
            104,
            HeaderFlags::extensions_present(),
            std::num::NonZeroU64::new(1),
            payload.value.as_ref(),
        );
        black_box(frame);
    });
}

#[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
fn should_decode_wire_two_core_only_64b(ctx: &mut StressContext) {
    let frames = wire3_core_only_frames();
    record_wire3_case(
        ctx,
        "wire2_core_only_decode_64b",
        frames[0].len(),
        "frame",
        WIRE3_DECODE_BATCH_OPS,
    );
    ctx.measure_batch("wire2_core_only_decode_64b", WIRE3_DECODE_BATCH_OPS, || {
        let mut index = 0;
        let mut checksum = 0_u64;
        for _ in 0..WIRE3_DECODE_BATCH_OPS {
            let frame = black_box(&frames[index]);
            let (header, value) = decode_message(frame, u32::MAX).unwrap();
            checksum = checksum
                .wrapping_add(u64::from(header.message_type))
                .wrapping_add(header.correlation.unwrap().get())
                .wrapping_add(u64::from(value[0]));
            index = (index + 1) % frames.len();
        }
        black_box(checksum);
    });
}

#[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
fn should_decode_wire_three_core_only_64b(ctx: &mut StressContext) {
    let frames = wire3_core_only_frames();
    record_wire3_case(
        ctx,
        "wire3_core_only_decode_64b",
        frames[0].len(),
        "frame",
        WIRE3_DECODE_BATCH_OPS,
    );
    ctx.measure_batch("wire3_core_only_decode_64b", WIRE3_DECODE_BATCH_OPS, || {
        let mut index = 0;
        let mut checksum = 0_u64;
        for _ in 0..WIRE3_DECODE_BATCH_OPS {
            let frame = black_box(&frames[index]);
            let (header, value) = decode_wire3(frame, u32::MAX).unwrap();
            let range = decode_core_payload(
                value,
                header.message_type,
                ExtensionDirection::ClientToBroker,
                header.flags.is_extensions_present(),
            )
            .unwrap();
            let core = range
                .get(value)
                .expect("decoded core range fits frame payload");
            checksum = checksum
                .wrapping_add(u64::from(header.message_type))
                .wrapping_add(header.correlation.unwrap().get())
                .wrapping_add(u64::from(core[0]));
            index = (index + 1) % frames.len();
        }
        black_box(checksum);
    });
}

#[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
fn should_decode_wire_three_one_unknown_extension_64b(ctx: &mut StressContext) {
    let frames = wire3_one_unknown_extension_frames();
    record_wire3_case(
        ctx,
        "wire3_one_unknown_extension_64b",
        frames[0].len(),
        "frame",
        WIRE3_DECODE_BATCH_OPS,
    );
    ctx.measure_batch(
        "wire3_one_unknown_extension_64b",
        WIRE3_DECODE_BATCH_OPS,
        || {
            let mut index = 0;
            let mut checksum = 0_u64;
            for _ in 0..WIRE3_DECODE_BATCH_OPS {
                let frame = black_box(&frames[index]);
                let (header, value) = decode_wire3(frame, u32::MAX).unwrap();
                let range = decode_core_payload(
                    value,
                    header.message_type,
                    ExtensionDirection::ClientToBroker,
                    header.flags.is_extensions_present(),
                )
                .unwrap();
                let core = range
                    .get(value)
                    .expect("decoded core range fits frame payload");
                checksum = checksum
                    .wrapping_add(u64::from(header.message_type))
                    .wrapping_add(header.correlation.unwrap().get())
                    .wrapping_add(u64::from(core[0]));
                index = (index + 1) % frames.len();
            }
            black_box(checksum);
        },
    );
}

fn measure_dense_wire3_unknown_extension_scan(
    ctx: &mut StressContext,
    case: &str,
    frame_limit_bytes: usize,
) {
    let frames = wire3_many_tiny_unknown_extensions_frames(frame_limit_bytes);
    record_wire3_case(ctx, case, frames[0].len(), "frame", 1);
    let mut index = 0;
    ctx.measure(case, || {
        let (header, value) = decode_wire3(
            black_box(&frames[index]),
            u32::try_from(frame_limit_bytes).expect("frame limit fits u32"),
        )
        .unwrap();
        let range = decode_core_payload(
            value,
            header.message_type,
            ExtensionDirection::ClientToBroker,
            header.flags.is_extensions_present(),
        )
        .unwrap();
        let checksum = range
            .get(value)
            .expect("decoded core range fits frame payload")
            .len()
            .wrapping_add(usize::from(header.message_type));
        index = (index + 1) % frames.len();
        black_box((checksum, index));
    });
}

#[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
fn should_scan_dense_unknown_extensions_at_the_minimum_frame_limit(ctx: &mut StressContext) {
    measure_dense_wire3_unknown_extension_scan(
        ctx,
        "wire3_many_tiny_extensions_min_frame",
        WIRE3_MIN_FRAME_LIMIT_BYTES,
    );
}

#[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
fn should_scan_dense_unknown_extensions_at_the_default_frame_limit(ctx: &mut StressContext) {
    measure_dense_wire3_unknown_extension_scan(
        ctx,
        "wire3_many_tiny_extensions_default_frame",
        WIRE3_DEFAULT_FRAME_LIMIT_BYTES,
    );
}

#[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
fn should_validate_the_maximum_capability_count(ctx: &mut StressContext) {
    let frames = wire3_max_capability_list_connect_frames();
    record_wire3_case(
        ctx,
        "wire3_max_capability_list",
        frames[0].len(),
        "frame",
        1,
    );
    let mut index = 0;
    ctx.measure("wire3_max_capability_list", || {
        let (header, value) = decode_wire3(black_box(&frames[index]), u32::MAX).unwrap();
        let range = decode_core_payload(
            value,
            header.message_type,
            ExtensionDirection::ClientToBroker,
            header.flags.is_extensions_present(),
        )
        .unwrap();
        let checksum = range
            .get(value)
            .expect("decoded core range fits frame payload")
            .first()
            .copied()
            .unwrap_or_default()
            + u8::try_from(header.correlation.unwrap().get()).unwrap_or_default();
        index = (index + 1) % frames.len();
        black_box((checksum, index));
    });
}

stress_main!();
