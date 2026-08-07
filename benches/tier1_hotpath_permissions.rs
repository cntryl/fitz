use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::auth::{Access, Permission};
use fitz::runtime::routing::Route;
use fitz::session::permissions::SessionPermissions;
use std::time::Instant;

stress_allocator!();

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
const CACHE_HIT_REPEAT_COUNT: u64 = 16_777_216;

fn parse_permissions(raws: &[&str]) -> Vec<Permission> {
    raws.iter()
        .map(|raw| Permission::parse(raw).expect("permission should parse"))
        .collect()
}

fn record_group(ctx: &mut StressContext, group: &str) {
    ctx.parameter("group", group);
}

fn mark_validated_micro(ctx: &mut StressContext) {
    ctx.metadata("validated_micro", "true");
}

fn warmed_permissions(raws: &[&str], route: &Route, access: Access) -> SessionPermissions {
    let permissions = SessionPermissions::from_permissions(parse_permissions(raws));
    let _ = permissions.allows(route, access);
    permissions
}

macro_rules! cache_hit_bench {
    ($fn_name:ident, $bench_name:literal, $raw:ident, $route:literal, $access:expr) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_permissions_hit");
            mark_validated_micro(ctx);
            let route = Route::new($route);
            let permissions = warmed_permissions(&$raw, &route, $access);

            let started = Instant::now();
            for _ in 0..CACHE_HIT_REPEAT_COUNT {
                black_box(permissions.allows(black_box(&route), black_box($access)));
            }
            let _ = ctx.record_external($bench_name, started.elapsed(), CACHE_HIT_REPEAT_COUNT);
        }
    };
}

cache_hit_bench!(
    should_allows_exact_granted_cache_hit,
    "allows_exact_granted_cache_hit",
    EXACT_PERMISSION_RAW,
    "rpc://acme/auth/users",
    Access::Write
);
cache_hit_bench!(
    should_allows_wildcard_granted_cache_hit,
    "allows_wildcard_granted_cache_hit",
    WILDCARD_PERMISSION_RAW,
    "rpc://acme/admin/users",
    Access::Write
);
cache_hit_bench!(
    should_allows_doublestar_deep_cache_hit,
    "allows_doublestar_deep_cache_hit",
    DOUBLESTAR_PERMISSION_RAW,
    "rpc://acme/auth/users/session/create",
    Access::Write
);
cache_hit_bench!(
    should_allows_large_set_last_match_cache_hit,
    "allows_large_set_last_match_cache_hit",
    LARGE_PERMISSION_RAW,
    "kv://acme/app/users",
    Access::Write
);

#[stress(tier = 1, name = "allows_deny_by_default_cache_hit")]
fn should_allows_deny_by_default_cache_hit(ctx: &mut StressContext) {
    record_group(ctx, "hotpath_permissions_hit");
    mark_validated_micro(ctx);
    let permissions = SessionPermissions::empty();
    let route = Route::new("rpc://acme/auth/users");
    let _ = permissions.allows(&route, Access::Read);

    let started = Instant::now();
    for _ in 0..CACHE_HIT_REPEAT_COUNT {
        black_box(permissions.allows(black_box(&route), black_box(Access::Read)));
    }
    let _ = ctx.record_external(
        "allows_deny_by_default_cache_hit",
        started.elapsed(),
        CACHE_HIT_REPEAT_COUNT,
    );
}

stress_main!();
