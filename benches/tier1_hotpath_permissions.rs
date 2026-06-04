use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::auth::{Access, Permission};
use fitz::runtime::routing::Route;
use fitz::session::permissions::SessionPermissions;

#[path = "criterion_config.rs"]
mod criterion_config;

const EXACT_PERMISSION_RAW: [&str; 3] = [
    "rpc://acme/auth/users#write",
    "notify://prod/events/orders#read",
    "queue://staging/jobs/worker#write",
];
const WILDCARD_PERMISSION_RAW: [&str; 3] = [
    "rpc://acme/*/users#write",
    "notify://prod/events/*#read",
    "queue://staging/*/worker#write",
];
const DOUBLESTAR_PERMISSION_RAW: [&str; 3] = [
    "rpc://acme/**#write",
    "notify://prod/**#read",
    "queue://**#write",
];
const LARGE_PERMISSION_RAW: [&str; 8] = [
    "rpc://acme/auth/**#write",
    "rpc://acme/api/**#write",
    "notify://prod/events/**#read",
    "notify://prod/logs/**#read",
    "queue://staging/**#write",
    "stream://acme/**#read",
    "lease://acme/**#write",
    "kv://acme/**#write",
];

fn parse_permissions(raws: &[&str]) -> Vec<Permission> {
    raws.iter()
        .map(|raw| Permission::parse(raw).expect("permission should parse"))
        .collect()
}

fn bench_permission_compilation(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_permissions_compile");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("compile_exact_3_rules", |b| {
        b.iter_batched(
            || parse_permissions(&EXACT_PERMISSION_RAW),
            |perms| {
                black_box(SessionPermissions::from_permissions(perms));
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("compile_wildcard_3_rules", |b| {
        b.iter_batched(
            || parse_permissions(&WILDCARD_PERMISSION_RAW),
            |perms| {
                black_box(SessionPermissions::from_permissions(perms));
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("compile_doublestar_3_rules", |b| {
        b.iter_batched(
            || parse_permissions(&DOUBLESTAR_PERMISSION_RAW),
            |perms| {
                black_box(SessionPermissions::from_permissions(perms));
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_permission_allows_cache_hit(c: &mut Criterion) {
    let exact_perms =
        SessionPermissions::from_permissions(parse_permissions(&EXACT_PERMISSION_RAW));
    let wildcard_perms =
        SessionPermissions::from_permissions(parse_permissions(&WILDCARD_PERMISSION_RAW));
    let doublestar_perms =
        SessionPermissions::from_permissions(parse_permissions(&DOUBLESTAR_PERMISSION_RAW));
    let large_perms =
        SessionPermissions::from_permissions(parse_permissions(&LARGE_PERMISSION_RAW));
    let allow_all = SessionPermissions::all();
    let deny_all = SessionPermissions::empty();

    let exact_route = Route::new("rpc://acme/auth/users");
    let wildcard_route = Route::new("rpc://acme/admin/users");
    let doublestar_route = Route::new("rpc://acme/auth/users/session/create");
    let late_match_route = Route::new("kv://acme/app/users");

    let _ = exact_perms.allows(&exact_route, Access::Write);
    let _ = wildcard_perms.allows(&wildcard_route, Access::Write);
    let _ = doublestar_perms.allows(&doublestar_route, Access::Write);
    let _ = large_perms.allows(&late_match_route, Access::Write);
    let _ = allow_all.allows(&exact_route, Access::Write);
    let _ = deny_all.allows(&exact_route, Access::Read);

    let mut group = c.benchmark_group("hotpath_permissions_hit");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("allows_exact_granted_cache_hit", |b| {
        b.iter(|| {
            black_box(exact_perms.allows(black_box(&exact_route), black_box(Access::Write)));
        })
    });

    group.bench_function("allows_wildcard_granted_cache_hit", |b| {
        b.iter(|| {
            black_box(wildcard_perms.allows(black_box(&wildcard_route), black_box(Access::Write)));
        })
    });

    group.bench_function("allows_doublestar_deep_cache_hit", |b| {
        b.iter(|| {
            black_box(
                doublestar_perms.allows(black_box(&doublestar_route), black_box(Access::Write)),
            );
        })
    });

    group.bench_function("allows_large_set_last_match_cache_hit", |b| {
        b.iter(|| {
            black_box(large_perms.allows(black_box(&late_match_route), black_box(Access::Write)));
        })
    });

    group.bench_function("allows_allow_all_cache_hit", |b| {
        b.iter(|| {
            black_box(allow_all.allows(black_box(&exact_route), black_box(Access::Write)));
        })
    });

    group.bench_function("allows_deny_by_default_cache_hit", |b| {
        b.iter(|| {
            black_box(deny_all.allows(black_box(&exact_route), black_box(Access::Read)));
        })
    });

    group.finish();
}

fn bench_permission_allows_cache_miss(c: &mut Criterion) {
    let exact_route = Route::new("rpc://acme/auth/users");
    let wildcard_route = Route::new("rpc://acme/admin/users");
    let doublestar_route = Route::new("rpc://acme/auth/users/session/create");
    let late_match_route = Route::new("kv://acme/app/users");

    let mut group = c.benchmark_group("hotpath_permissions_miss");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("allows_exact_granted_cache_miss", |b| {
        b.iter_batched(
            || SessionPermissions::from_permissions(parse_permissions(&EXACT_PERMISSION_RAW)),
            |perms| {
                black_box(perms.allows(black_box(&exact_route), black_box(Access::Write)));
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("allows_wildcard_granted_cache_miss", |b| {
        b.iter_batched(
            || SessionPermissions::from_permissions(parse_permissions(&WILDCARD_PERMISSION_RAW)),
            |perms| {
                black_box(perms.allows(black_box(&wildcard_route), black_box(Access::Write)));
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("allows_doublestar_deep_cache_miss", |b| {
        b.iter_batched(
            || SessionPermissions::from_permissions(parse_permissions(&DOUBLESTAR_PERMISSION_RAW)),
            |perms| {
                black_box(perms.allows(black_box(&doublestar_route), black_box(Access::Write)));
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("allows_large_set_last_match_cache_miss", |b| {
        b.iter_batched(
            || SessionPermissions::from_permissions(parse_permissions(&LARGE_PERMISSION_RAW)),
            |perms| {
                black_box(perms.allows(black_box(&late_match_route), black_box(Access::Write)));
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets =
        bench_permission_compilation,
        bench_permission_allows_cache_hit,
        bench_permission_allows_cache_miss
}
criterion_main!(benches);
