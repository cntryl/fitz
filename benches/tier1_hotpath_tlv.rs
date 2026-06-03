use bytes::Bytes;
use criterion::{Criterion, SamplingMode, Throughput, black_box, criterion_group, criterion_main};
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder, TlvRecord};

#[path = "criterion_config.rs"]
mod criterion_config;

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

/// Decode benches: decode_all (batch), decode-iterator with preallocated Vec.
/// Batch size 64 keeps each iteration under the tier1 target (<10 µs per op).
fn bench_tlv_decode_sizes(c: &mut Criterion) {
    let sizes = [0usize, 16, 64, 256];
    let records = 64usize;

    let mut group = c.benchmark_group("hotpath_tlv_decode_sizes");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // build frame
        let mut encoder = TlvEncoder::with_capacity(1024 * 8);
        let payload = vec![0u8; size];
        for i in 0..records {
            encoder.encode(MessageType::new(i as u16), &payload);
        }
        let data = encoder.finish();

        group.throughput(Throughput::Elements(records as u64));
        let bench_name = format!("decode_all_{}B_{}recs", size, records);
        group.bench_function(&bench_name, |b| {
            let decoder = TlvDecoder::new();
            b.iter(|| {
                // Ensure result escapes to black_box so it can't be optimized away
                black_box(decoder.decode_all(&data).unwrap());
            })
        });

        // decode-by-iter-loop but reuse a preallocated Vec to avoid Vec growth allocations
        let bench_name = format!("decode_iter_reuse_{}B_{}recs", size, records);
        group.bench_function(&bench_name, |b| {
            let decoder = TlvDecoder::new();
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

        let bench_name = format!("decode_one_{}B", size);
        group.bench_function(&bench_name, |b| {
            let decoder = TlvDecoder::new();
            b.iter(|| {
                black_box(decoder.decode_one(black_box(&data)).unwrap());
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets = bench_tlv_encode_sizes, bench_tlv_decode_sizes, bench_tlv_decode_single_record
}
criterion_main!(benches);
