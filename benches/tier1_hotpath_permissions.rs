use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::auth::{Access, Permission};
use fitz::runtime::routing::Route;
use fitz::session::permissions::SessionPermissions;

#[path = "criterion_config.rs"]
mod criterion_config;

fn bench_permission_compilation(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - precompute permission strings
    let exact_perms = vec![
        Permission::parse("rpc://acme/auth/users#write").unwrap(),
        Permission::parse("notify://prod/events/orders#read").unwrap(),
        Permission::parse("queue://staging/jobs/worker#write").unwrap(),
    ];

    let wildcard_perms = vec![
        Permission::parse("rpc://acme/*/users#write").unwrap(),
        Permission::parse("notify://prod/events/*#read").unwrap(),
        Permission::parse("queue://staging/*/worker#write").unwrap(),
    ];

    let doublestar_perms = vec![
        Permission::parse("rpc://acme/**#write").unwrap(),
        Permission::parse("notify://prod/**#read").unwrap(),
        Permission::parse("queue://**#write").unwrap(),
    ];

    let mut group = c.benchmark_group("hotpath_permissions");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("compile_exact_permissions", |b| {
        b.iter(|| {
            // ONLY hot path - compile exact route permissions
            let _perms = SessionPermissions::from_permissions(black_box(exact_perms.clone()));
        })
    });

    group.bench_function("compile_wildcard_permissions", |b| {
        b.iter(|| {
            // ONLY hot path - compile wildcard permissions
            let _perms = SessionPermissions::from_permissions(black_box(wildcard_perms.clone()));
        })
    });

    group.bench_function("compile_doublestar_permissions", |b| {
        b.iter(|| {
            // ONLY hot path - compile double-star (deep) permissions
            let _perms = SessionPermissions::from_permissions(black_box(doublestar_perms.clone()));
        })
    });

    group.finish();
}

fn bench_permission_allows_exact_match(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let perms = SessionPermissions::from_permissions(vec![
        Permission::parse("rpc://acme/auth/users#write").unwrap(),
        Permission::parse("notify://prod/events/orders#read").unwrap(),
    ]);

    let allowed_route = Route::new("rpc://acme/auth/users");
    let denied_route = Route::new("rpc://acme/auth/admin");

    let mut group = c.benchmark_group("hotpath_permissions");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("allows_exact_match_granted", |b| {
        b.iter(|| {
            // ONLY hot path - permission check (should succeed)
            let _result = perms.allows(black_box(&allowed_route), black_box(Access::Write));
        })
    });

    group.bench_function("allows_exact_match_denied", |b| {
        b.iter(|| {
            // ONLY hot path - permission check (should fail)
            let _result = perms.allows(black_box(&denied_route), black_box(Access::Write));
        })
    });

    group.finish();
}

fn bench_permission_allows_wildcard_match(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let perms = SessionPermissions::from_permissions(vec![
        Permission::parse("rpc://acme/*/users#write").unwrap(),
        Permission::parse("notify://prod/events/*#read").unwrap(),
    ]);

    let allowed_route1 = Route::new("rpc://acme/auth/users");
    let allowed_route2 = Route::new("rpc://acme/admin/users");
    let denied_route = Route::new("rpc://other/auth/users");

    let mut group = c.benchmark_group("hotpath_permissions");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("allows_wildcard_match_granted", |b| {
        b.iter(|| {
            // ONLY hot path - wildcard permission check (should succeed)
            let _result = perms.allows(black_box(&allowed_route1), black_box(Access::Write));
        })
    });

    group.bench_function("allows_wildcard_match_different_area", |b| {
        b.iter(|| {
            // ONLY hot path - wildcard matches different area
            let _result = perms.allows(black_box(&allowed_route2), black_box(Access::Write));
        })
    });

    group.bench_function("allows_wildcard_match_denied", |b| {
        b.iter(|| {
            // ONLY hot path - wildcard doesn't match different realm
            let _result = perms.allows(black_box(&denied_route), black_box(Access::Write));
        })
    });

    group.finish();
}

fn bench_permission_allows_doublestar_match(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let perms = SessionPermissions::from_permissions(vec![
        Permission::parse("rpc://acme/**#write").unwrap(),
        Permission::parse("notify://**#read").unwrap(),
    ]);

    let deep_route = Route::new("rpc://acme/auth/users/session/create");
    let shallow_route = Route::new("rpc://acme/auth");
    let global_route = Route::new("notify://prod/events/orders/items/added");

    let mut group = c.benchmark_group("hotpath_permissions");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("allows_doublestar_deep_match", |b| {
        b.iter(|| {
            // ONLY hot path - double-star matches deep route
            let _result = perms.allows(black_box(&deep_route), black_box(Access::Write));
        })
    });

    group.bench_function("allows_doublestar_shallow_match", |b| {
        b.iter(|| {
            // ONLY hot path - double-star matches shallow route
            let _result = perms.allows(black_box(&shallow_route), black_box(Access::Write));
        })
    });

    group.bench_function("allows_doublestar_global_match", |b| {
        b.iter(|| {
            // ONLY hot path - global ** permission
            let _result = perms.allows(black_box(&global_route), black_box(Access::Read));
        })
    });

    group.finish();
}

fn bench_permission_allows_access_levels(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let read_perms = SessionPermissions::from_permissions(vec![Permission::parse(
        "rpc://acme/**#read",
    )
    .unwrap()]);

    let write_perms =
        SessionPermissions::from_permissions(vec![
            Permission::parse("rpc://acme/**#write").unwrap()
        ]);

    let all_perms =
        SessionPermissions::from_permissions(vec![Permission::parse("rpc://acme/**#*").unwrap()]);

    let route = Route::new("rpc://acme/auth/users");

    let mut group = c.benchmark_group("hotpath_permissions");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("allows_access_read_granted", |b| {
        b.iter(|| {
            // ONLY hot path - read permission check
            let _result = read_perms.allows(black_box(&route), black_box(Access::Read));
        })
    });

    group.bench_function("allows_access_read_denied_for_write", |b| {
        b.iter(|| {
            // ONLY hot path - read permission doesn't grant write
            let _result = read_perms.allows(black_box(&route), black_box(Access::Write));
        })
    });

    group.bench_function("allows_access_write_granted", |b| {
        b.iter(|| {
            // ONLY hot path - write permission check
            let _result = write_perms.allows(black_box(&route), black_box(Access::Write));
        })
    });

    group.bench_function("allows_access_all_granted_read", |b| {
        b.iter(|| {
            // ONLY hot path - all permission grants read
            let _result = all_perms.allows(black_box(&route), black_box(Access::Read));
        })
    });

    group.bench_function("allows_access_all_granted_write", |b| {
        b.iter(|| {
            // ONLY hot path - all permission grants write
            let _result = all_perms.allows(black_box(&route), black_box(Access::Write));
        })
    });

    group.finish();
}

fn bench_permission_allows_multiple_permissions(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - simulate realistic permission set
    let small_set = SessionPermissions::from_permissions(vec![
        Permission::parse("rpc://acme/**#write").unwrap(),
        Permission::parse("notify://prod/**#read").unwrap(),
    ]);

    let large_set = SessionPermissions::from_permissions(vec![
        Permission::parse("rpc://acme/auth/**#write").unwrap(),
        Permission::parse("rpc://acme/api/**#write").unwrap(),
        Permission::parse("notify://prod/events/**#read").unwrap(),
        Permission::parse("notify://prod/logs/**#read").unwrap(),
        Permission::parse("queue://staging/**#write").unwrap(),
        Permission::parse("stream://acme/**#read").unwrap(),
        Permission::parse("lease://acme/**#write").unwrap(),
        Permission::parse("kv://acme/**#write").unwrap(),
    ]);

    let first_match_route = Route::new("rpc://acme/auth/users");
    let last_match_route = Route::new("kv://acme/app/users");

    let mut group = c.benchmark_group("hotpath_permissions");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("allows_small_set_first_match", |b| {
        b.iter(|| {
            // ONLY hot path - check with 2 permissions (early match)
            let _result = small_set.allows(black_box(&first_match_route), black_box(Access::Write));
        })
    });

    group.bench_function("allows_large_set_first_match", |b| {
        b.iter(|| {
            // ONLY hot path - check with 8 permissions (early match)
            let _result = large_set.allows(black_box(&first_match_route), black_box(Access::Write));
        })
    });

    group.bench_function("allows_large_set_last_match", |b| {
        b.iter(|| {
            // ONLY hot path - check with 8 permissions (late match)
            let _result = large_set.allows(black_box(&last_match_route), black_box(Access::Write));
        })
    });

    group.finish();
}

fn bench_permission_allows_deny_by_default(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let empty_perms = SessionPermissions::empty();
    let route = Route::new("rpc://acme/auth/users");

    let mut group = c.benchmark_group("hotpath_permissions");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("allows_deny_by_default", |b| {
        b.iter(|| {
            // ONLY hot path - empty permission set denies everything
            let _result = empty_perms.allows(black_box(&route), black_box(Access::Read));
        })
    });

    group.finish();
}

fn bench_permission_allows_allow_all(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let all_perms = SessionPermissions::all();
    let route = Route::new("rpc://acme/auth/users");

    let mut group = c.benchmark_group("hotpath_permissions");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("allows_allow_all", |b| {
        b.iter(|| {
            // ONLY hot path - allow-all permission grants everything
            let _result = all_perms.allows(black_box(&route), black_box(Access::Write));
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets =
        bench_permission_compilation,
        bench_permission_allows_exact_match,
        bench_permission_allows_wildcard_match,
        bench_permission_allows_doublestar_match,
        bench_permission_allows_access_levels,
        bench_permission_allows_multiple_permissions,
        bench_permission_allows_deny_by_default,
        bench_permission_allows_allow_all
}
criterion_main!(benches);
