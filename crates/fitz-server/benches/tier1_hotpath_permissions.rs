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
const CHURN_BATCH_OPS: u64 = 64;

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

fn churn_routes(count: usize, route_bytes: usize) -> Vec<String> {
    (0..count)
        .map(|index| {
            let realm = if index % 2 == 0 { "acme" } else { "nope" };
            if route_bytes == 0 {
                format!("rpc://{realm}/auth/items/item-{index}")
            } else {
                let prefix = format!("rpc://{realm}/auth/items/");
                let suffix = format!("/item-{index}");
                let padding = "x".repeat(route_bytes.saturating_sub(prefix.len() + suffix.len()));
                format!("{prefix}{padding}{suffix}")
            }
        })
        .collect()
}

fn measure_churn(ctx: &mut StressContext, name: &str, count: usize, route_bytes: usize) {
    record_group(ctx, "hotpath_permissions_churn");
    mark_validated_micro(ctx);
    ctx.parameter("route_count", count);
    ctx.parameter(
        "route_bytes",
        if route_bytes == 0 {
            "short"
        } else {
            "about_1k"
        },
    );
    ctx.parameter("logical_unit", "authorization_check");

    let permissions =
        SessionPermissions::from_permissions(parse_permissions(&["rpc://acme/**#write"]));
    let routes = churn_routes(count, route_bytes);
    assert_eq!(routes.len(), count);
    for (index, route) in routes.iter().enumerate() {
        assert_eq!(
            permissions.allows_route(route, Access::Write),
            index % 2 == 0
        );
    }

    let mut index = 0;
    ctx.measure_batch(name, CHURN_BATCH_OPS, || {
        for _ in 0..CHURN_BATCH_OPS {
            let allowed = permissions.allows_route(black_box(&routes[index]), Access::Write);
            black_box(allowed);
            index = (index + 1) % routes.len();
        }
    });
}

macro_rules! churn_bench {
    ($fn_name:ident, $bench_name:literal, $count:literal, $route_bytes:literal) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            measure_churn(ctx, $bench_name, $count, $route_bytes);
        }
    };
}

churn_bench!(
    should_churn_128_short_routes,
    "churn_128_short_routes",
    128,
    0
);
churn_bench!(
    should_churn_256_short_routes,
    "churn_256_short_routes",
    256,
    0
);
churn_bench!(
    should_churn_300_short_routes,
    "churn_300_short_routes",
    300,
    0
);
churn_bench!(
    should_churn_32_long_routes,
    "churn_32_long_routes",
    32,
    1024
);
churn_bench!(
    should_churn_96_long_routes,
    "churn_96_long_routes",
    96,
    1024
);

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
