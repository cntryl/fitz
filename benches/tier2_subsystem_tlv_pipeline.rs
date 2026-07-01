#![allow(deprecated)]
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::protocol::mux::Mux;
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder};

#[path = "criterion_config.rs"]
mod criterion_config;

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

fn bench_pipeline_decode_only(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize];
    let records = 256usize;

    let mut group = c.benchmark_group("subsystem_tlv_pipeline");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        let data = encode_frame(records, size);
        let decoder = TlvDecoder::new();
        let mut refs = Vec::with_capacity(records);

        group.throughput(Throughput::Elements(records as u64));
        group.bench_function(format!("decode_only_{size}B"), |b| {
            b.iter(|| {
                refs.clear();
                decoder
                    .decode_refs_into(black_box(&data), &mut refs)
                    .unwrap();
                black_box(&refs);
            });
        });
    }

    group.finish();
}

fn bench_pipeline_mux_route_ref_only(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize];
    let records = 256usize;

    let mut group = c.benchmark_group("subsystem_tlv_pipeline");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        let data = encode_frame(records, size);
        let decoder = TlvDecoder::new();
        let mut refs = Vec::with_capacity(records);
        decoder.decode_refs_into(&data, &mut refs).unwrap();
        let mut mux = Mux::new(records);

        for tlv_ref in &refs {
            let cref = mux.route_ref(tlv_ref.ty, tlv_ref.value).unwrap();
            mux.release(cref.channel);
        }

        group.throughput(Throughput::Elements(records as u64));
        group.bench_function(format!("mux_route_ref_only_{size}B"), |b| {
            b.iter(|| {
                for tlv_ref in &refs {
                    let cref = mux.route_ref(tlv_ref.ty, black_box(tlv_ref.value)).unwrap();
                    mux.release(cref.channel);
                }
            });
        });
    }

    group.finish();
}

fn bench_pipeline_decode_then_mux_route_ref(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize];
    let records = 256usize;

    let mut group = c.benchmark_group("subsystem_tlv_pipeline");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        let data = encode_frame(records, size);
        let decoder = TlvDecoder::new();
        let mut refs = Vec::with_capacity(records);
        let mut warm_refs = Vec::with_capacity(records);
        decoder.decode_refs_into(&data, &mut warm_refs).unwrap();

        let mut mux = Mux::new(records);
        for tlv_ref in &warm_refs {
            let cref = mux.route_ref(tlv_ref.ty, tlv_ref.value).unwrap();
            mux.release(cref.channel);
        }

        group.throughput(Throughput::Elements(records as u64));
        group.bench_function(format!("decode_then_mux_route_ref_{size}B"), |b| {
            b.iter(|| {
                refs.clear();
                decoder
                    .decode_refs_into(black_box(&data), &mut refs)
                    .unwrap();

                for tlv_ref in &refs {
                    let cref = mux.route_ref(tlv_ref.ty, tlv_ref.value).unwrap();
                    mux.release(cref.channel);
                }
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets =
        bench_pipeline_decode_only,
        bench_pipeline_mux_route_ref_only,
        bench_pipeline_decode_then_mux_route_ref
}
criterion_main!(benches);
