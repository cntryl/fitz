use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use bytes::Bytes;
use fitz::protocol::tlv::{TlvDecoder, TlvEncoder, MessageType, TlvRecord};

#[path = "config.rs"]
mod config;

/// Encode benches: reuse vs finish and payload-size sweep
fn bench_tlv_encode_sizes(c: &mut Criterion) {
    let sizes = [0usize, 16, 64, 256];

    let mut group = c.benchmark_group("hotpath_tlv_encode_sizes");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // Reuse encoder + clear
        let mut encoder = TlvEncoder::with_capacity(1024);
        let payload = vec![0u8; size];

        group.throughput(Throughput::Elements(size as u64));
        let bench_name = format!("encode_clear_encode_{}B", size);
        group.bench_function(&bench_name, |b| {
            b.iter(|| {
                encoder.clear();
                encoder.encode(MessageType::new(42), black_box(&payload));
                black_box(&encoder);
            })
        });

        // include finish() cost — finish consumes the encoder, so allocate per-iteration
        let bench_name = format!("encode_new_finish_{}B", size);
        group.bench_function(&bench_name, |b| {
            b.iter(|| {
                // realistic path: build and finish the buffer
                let mut e = TlvEncoder::with_capacity(1024);
                e.encode(MessageType::new(42), black_box(&payload));
                let out: Bytes = e.finish();
                black_box(out);
            })
        });
    }

    group.finish();
}

/// Decode benches: decode_all (stress), decode_one (single-record realism), and decode-iterator with preallocated Vec to reduce reallocation bias
fn bench_tlv_decode_sizes(c: &mut Criterion) {
    let sizes = [0usize, 16, 64, 256];
    let records = 256usize;

    let mut group = c.benchmark_group("hotpath_tlv_decode_sizes");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // build frame
        let mut encoder = TlvEncoder::with_capacity(1024 * 8);
        let payload = vec![0u8; size];
        for i in 0..records {
            encoder.encode(MessageType::new((i % 0xFF) as u16), &payload);
        }
        let data = encoder.finish();

        let decoder = TlvDecoder::new();

        group.throughput(Throughput::Elements(records as u64));
        let bench_name = format!("decode_all_{}B_{}recs", size, records);
        group.bench_function(&bench_name, |b| {
            b.iter(|| {
                // Ensure result escapes to black_box so it can't be optimized away
                black_box(decoder.decode_all(&data).unwrap());
            })
        });

        // decode-by-iter-loop but reuse a preallocated Vec to avoid Vec growth allocations
        let bench_name = format!("decode_iter_reuse_{}B_{}recs", size, records);
        group.bench_function(&bench_name, |b| {
            // pre-allocate outside hot path
            let mut out: Vec<TlvRecord> = Vec::with_capacity(records);
            b.iter(|| {
                out.clear();
                // iterate by using decode_one repeatedly
                let mut offset = 0usize;
                while offset < data.len() {
                    let (rec, consumed) = decoder.decode_one(&data[offset..]).unwrap();
                    out.push(rec);
                    offset += consumed;
                }
                black_box(&out);
            })
        });
    }

    group.finish();
}

/// Single-record decode bench for RPC/control-plane realism
fn bench_tlv_decode_single_record(c: &mut Criterion) {
    let sizes = [0usize, 16, 64, 256];

    let mut group = c.benchmark_group("hotpath_tlv_decode_single");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    for &size in &sizes {
        let mut encoder = TlvEncoder::with_capacity(256);
        let payload = vec![0u8; size];
        encoder.encode(MessageType::new(42), &payload);
        let data = encoder.finish();

        let decoder = TlvDecoder::new();
        let bench_name = format!("decode_one_{}B", size);
        group.bench_function(&bench_name, |b| {
            b.iter(|| {
                black_box(decoder.decode_one(black_box(&data)).unwrap());
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_tlv_encode_sizes, bench_tlv_decode_sizes, bench_tlv_decode_single_record
}
criterion_main!(benches);
