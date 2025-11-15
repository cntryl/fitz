use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::SubSender;
use fitz::routing::{RouteTable, RtSubscription, DEFAULT_RF};
use tokio::sync::mpsc;

#[path = "../config.rs"]
mod config;

// =============================================================================
// Helpers
// =============================================================================

fn make_sub(id: u64, pattern: String, sender: &SubSender) -> RtSubscription {
    RtSubscription {
        id,
        route_pattern: pattern,
        channel_id: 1,
        sender: sender.clone(),
    }
}

// =============================================================================
// SIMPLE BENCHES
// =============================================================================

fn bench_route_table_insert(c: &mut Criterion) {
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    c.bench_function("route_table_insert", |b| {
        b.iter(|| {
            let mut rt = RouteTable::new();
            for i in 0..10 {
                let pat = format!("scheme://realm/area{}/resource/op", i);
                rt.insert(DEFAULT_RF, make_sub(i, pat, &tx));
            }
            rt
        });
    });
}

fn bench_route_table_remove(c: &mut Criterion) {
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    c.bench_function("route_table_remove", |b| {
        b.iter(|| {
            let mut rt = RouteTable::new();
            for i in 0..10 {
                let pat = format!("scheme://realm/area{}/resource/op", i);
                rt.insert(DEFAULT_RF, make_sub(i, pat, &tx));
            }
            for i in 0..5 {
                rt.remove(DEFAULT_RF, i);
            }
            rt
        });
    });
}

fn bench_route_table_match_exact(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    for i in 0..100 {
        let pat = format!("scheme://realm/area{}/resource/op", i);
        rt.insert(DEFAULT_RF, make_sub(i, pat, &tx));
    }

    let target = "scheme://realm/area42/resource/op".to_string();

    c.bench_function("route_table_match_exact", |b| {
        b.iter(|| rt.matching_subscribers(DEFAULT_RF, &target));
    });
}

fn bench_route_table_match_global_wildcard(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    rt.insert(DEFAULT_RF, make_sub(1, "*".to_string(), &tx));

    for i in 0..100 {
        let pat = format!("scheme://realm/area{}/resource/op", i);
        rt.insert(DEFAULT_RF, make_sub(i + 2, pat, &tx));
    }

    let target = "scheme://realm/area42/resource/op".to_string();

    c.bench_function("route_table_match_global_wildcard", |b| {
        b.iter(|| rt.matching_subscribers(DEFAULT_RF, &target));
    });
}

fn bench_route_table_match_trailing_wildcard(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    for i in 0..20 {
        let pat = format!("scheme://realm/area{}/*", i);
        rt.insert(DEFAULT_RF, make_sub(i, pat, &tx));
    }

    let target = "scheme://realm/area10/resource/op".to_string();

    c.bench_function("route_table_match_trailing_wildcard", |b| {
        b.iter(|| rt.matching_subscribers(DEFAULT_RF, &target));
    });
}

fn bench_route_table_match_mid_path_wildcard(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    for i in 0..20 {
        let pat = format!("scheme://realm/*/resource{}/op", i);
        rt.insert(DEFAULT_RF, make_sub(i, pat, &tx));
    }

    let target = "scheme://realm/anyarea/resource10/op".to_string();

    c.bench_function("route_table_match_mid_path_wildcard", |b| {
        b.iter(|| rt.matching_subscribers(DEFAULT_RF, &target));
    });
}

fn bench_route_table_match_none(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    for i in 0..100 {
        let pat = format!("scheme://other/area{}/resource/op", i);
        rt.insert(DEFAULT_RF, make_sub(i, pat, &tx));
    }

    let target = "scheme://nomatch/area/resource/op".to_string();

    c.bench_function("route_table_match_none", |b| {
        b.iter(|| rt.matching_subscribers(DEFAULT_RF, &target));
    });
}

fn bench_route_table_cleanup_channel(c: &mut Criterion) {
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    c.bench_function("route_table_cleanup_channel", |b| {
        b.iter(|| {
            let mut rt = RouteTable::new();

            for channel_id in 1..=5 {
                for i in 0..20 {
                    let pat = format!("scheme://realm/area{}/resource/op", i);
                    rt.insert(
                        DEFAULT_RF,
                        RtSubscription {
                            id: (channel_id * 100 + i) as u64,
                            route_pattern: pat,
                            channel_id,
                            sender: tx.clone(),
                        },
                    );
                }
            }

            rt.cleanup_channel(DEFAULT_RF, 3);
            rt
        });
    });
}

// =============================================================================
// SCALING
// =============================================================================

fn bench_route_table_match_exact_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_match_exact_scaling");
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    for &n in &[1_000, 10_000, 100_000] {
        let mut rt = RouteTable::new();

        let patterns: Vec<_> = (0..n)
            .map(|i| format!("scheme://realm/area{}/resource/op", i))
            .collect();

        for (i, pat) in patterns.iter().enumerate() {
            rt.insert(DEFAULT_RF, make_sub(i as u64, pat.clone(), &tx));
        }

        let target = patterns[n / 2].clone();

        group.bench_with_input(format!("{}", n), &rt, |b, rt| {
            b.iter(|| rt.matching_subscribers(DEFAULT_RF, &target));
        });
    }

    group.finish();
}

fn bench_route_table_match_wildcard_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_match_wildcard_scaling");
    let (tx, _rx) = mpsc::channel::<(
        String,
        Option<String>,
        Vec<u8>,
        Option<String>,
        Option<u32>,
        bool,
    )>(100);

    for &n in &[1_000, 10_000, 100_000] {
        let mut rt = RouteTable::new();

        let patterns: Vec<_> = (0..n)
            .map(|i| {
                if i % 10 == 0 {
                    format!("scheme://realm/area{}/*", i)
                } else {
                    format!("scheme://realm/area{}/resource/op", i)
                }
            })
            .collect();

        for (i, pat) in patterns.iter().enumerate() {
            rt.insert(DEFAULT_RF, make_sub(i as u64, pat.clone(), &tx));
        }

        let target = format!("scheme://realm/area{}/resource/op", n / 2);

        group.bench_with_input(format!("{}", n), &rt, |b, rt| {
            b.iter(|| rt.matching_subscribers(DEFAULT_RF, &target));
        });
    }

    group.finish();
}

// =============================================================================
// GROUP
// =============================================================================

criterion_group! {
    name = hotpath_route_table;
    config = config::criterion_config();
    targets =
        bench_route_table_insert,
        bench_route_table_remove,
        bench_route_table_match_exact,
        bench_route_table_match_global_wildcard,
        bench_route_table_match_trailing_wildcard,
        bench_route_table_match_mid_path_wildcard,
        bench_route_table_match_none,
        bench_route_table_cleanup_channel,
        bench_route_table_match_exact_scaling,
        bench_route_table_match_wildcard_scaling,
}

criterion_main!(hotpath_route_table);
