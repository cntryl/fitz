use criterion::{criterion_group, criterion_main, Criterion};
use fitz::routing::{RouteTable, RtSubscription};
use tokio::sync::mpsc;

#[path = "../config.rs"]
mod config;

// ============================================================================
// ROUTE TABLE BENCHMARKS
// ============================================================================

/// Benchmark: Insert subscription into route table
fn bench_route_table_insert(c: &mut Criterion) {
    c.bench_function("route_table_insert", |b| {
        b.iter(|| {
            let mut rt = RouteTable::new();
            let (tx, _rx) = mpsc::channel(100);
            for i in 0..10 {
                let sub = RtSubscription {
                    id: i,
                    route_pattern: format!("scheme://realm/area{}/resource/op", i),
                    channel_id: 1,
                    sender: tx.clone(),
                };
                rt.insert(sub);
            }
            rt
        });
    });
}

/// Benchmark: Remove subscription from route table
fn bench_route_table_remove(c: &mut Criterion) {
    c.bench_function("route_table_remove", |b| {
        b.iter(|| {
            let mut rt = RouteTable::new();
            let (tx, _rx) = mpsc::channel(100);
            for i in 0..10 {
                let sub = RtSubscription {
                    id: i,
                    route_pattern: format!("scheme://realm/area{}/resource/op", i),
                    channel_id: 1,
                    sender: tx.clone(),
                };
                rt.insert(sub);
            }
            // Remove half of them
            for i in 0..5 {
                rt.remove(i);
            }
            rt
        });
    });
}

/// Benchmark: Find matching subscribers - exact match
fn bench_route_table_match_exact(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    for i in 0..100 {
        let sub = RtSubscription {
            id: i,
            route_pattern: format!("scheme://realm/area{}/resource/op", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_exact", |b| {
        b.iter(|| {
            rt.matching_subscribers("scheme://realm/area42/resource/op")
        });
    });
}

/// Benchmark: Find matching subscribers - global wildcard
fn bench_route_table_match_global_wildcard(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    
    // Add one global wildcard subscription
    let sub = RtSubscription {
        id: 1,
        route_pattern: "*".to_string(),
        channel_id: 1,
        sender: tx.clone(),
    };
    rt.insert(sub);
    
    // Add many specific subscriptions
    for i in 0..100 {
        let sub = RtSubscription {
            id: i + 2,
            route_pattern: format!("scheme://realm/area{}/resource/op", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_global_wildcard", |b| {
        b.iter(|| {
            rt.matching_subscribers("scheme://realm/area42/resource/op")
        });
    });
}

/// Benchmark: Find matching subscribers - trailing wildcard
fn bench_route_table_match_trailing_wildcard(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    
    // Add trailing wildcard subscriptions at different levels
    for i in 0..20 {
        let sub = RtSubscription {
            id: i,
            route_pattern: format!("scheme://realm/area{}/*", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_trailing_wildcard", |b| {
        b.iter(|| {
            rt.matching_subscribers("scheme://realm/area10/resource/op")
        });
    });
}

/// Benchmark: Find matching subscribers - mid-path wildcard
fn bench_route_table_match_mid_path_wildcard(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    
    // Add mid-path wildcard subscriptions
    for i in 0..20 {
        let sub = RtSubscription {
            id: i,
            route_pattern: format!("scheme://realm/*/resource{}/op", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_mid_path_wildcard", |b| {
        b.iter(|| {
            rt.matching_subscribers("scheme://realm/anyarea/resource10/op")
        });
    });
}

/// Benchmark: Find matching subscribers - no matches
fn bench_route_table_match_none(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    
    for i in 0..100 {
        let sub = RtSubscription {
            id: i,
            route_pattern: format!("scheme://other/area{}/resource/op", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_none", |b| {
        b.iter(|| {
            rt.matching_subscribers("scheme://nomatch/area/resource/op")
        });
    });
}

/// Benchmark: Cleanup channel subscriptions
fn bench_route_table_cleanup_channel(c: &mut Criterion) {
    c.bench_function("route_table_cleanup_channel", |b| {
        b.iter(|| {
            let mut rt = RouteTable::new();
            let (tx, _rx) = mpsc::channel(100);
            
            // Add subscriptions for multiple channels
            for channel_id in 1..=5 {
                for i in 0..20 {
                    let sub = RtSubscription {
                        id: (channel_id * 100 + i) as u64,
                        route_pattern: format!("scheme://realm/area{}/resource/op", i),
                        channel_id,
                        sender: tx.clone(),
                    };
                    rt.insert(sub);
                }
            }
            
            // Cleanup channel 3
            rt.cleanup_channel(3);
            rt
        });
    });
}

// ============================================================================
// SCALING BENCHMARKS (Confirm scaling behavior)
// ============================================================================

/// Benchmark: Match exact route at various scales (1K, 10K, 100K subscriptions)
fn bench_route_table_match_exact_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_match_exact_scaling");
    
    for &n in &[1_000, 10_000, 100_000] {
        let mut rt = RouteTable::new();
        let (tx, _rx) = mpsc::channel(100);
        
        // Insert N unique subscriptions
        for i in 0..n {
            let sub = RtSubscription {
                id: i,
                route_pattern: format!("scheme://realm/area{}/resource/op", i),
                channel_id: 1,
                sender: tx.clone(),
            };
            rt.insert(sub);
        }
        
        group.bench_with_input(format!("{}", n), &rt, |b, rt| {
            b.iter(|| {
                rt.matching_subscribers(&format!("scheme://realm/area{}/resource/op", n / 2))
            });
        });
    }
    
    group.finish();
}

/// Benchmark: Match with wildcards at various scales (1K, 10K, 100K subscriptions)
fn bench_route_table_match_wildcard_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_match_wildcard_scaling");
    
    for &n in &[1_000, 10_000, 100_000] {
        let mut rt = RouteTable::new();
        let (tx, _rx) = mpsc::channel(100);
        
        // Insert N subscriptions (10% with trailing wildcards)
        for i in 0..n {
            let pattern = if i % 10 == 0 {
                format!("scheme://realm/area{}/*", i)
            } else {
                format!("scheme://realm/area{}/resource/op", i)
            };
            let sub = RtSubscription {
                id: i,
                route_pattern: pattern,
                channel_id: 1,
                sender: tx.clone(),
            };
            rt.insert(sub);
        }
        
        group.bench_with_input(format!("{}", n), &rt, |b, rt| {
            b.iter(|| {
                rt.matching_subscribers(&format!("scheme://realm/area{}/resource/op", n / 2))
            });
        });
    }
    
    group.finish();
}

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
