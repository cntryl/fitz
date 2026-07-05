use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::auth::{Access, Permission};
use fitz::runtime::routing::Route;
use fitz::session::permissions::SessionPermissions;

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

fn parse_permissions(raws: &[&str]) -> Vec<Permission> {
    raws.iter()
        .map(|raw| Permission::parse(raw).expect("permission should parse"))
        .collect()
}

fn record_group(ctx: &mut StressContext, group: &str) {
    ctx.parameter("group", group);
}

macro_rules! compile_bench {
    ($fn_name:ident, $bench_name:literal, $raw:ident) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_permissions_compile");

            ctx.measure("operation", || {
                black_box(SessionPermissions::from_permissions(parse_permissions(
                    &$raw,
                )));
            });
        }
    };
}

compile_bench!(
    should_compile_exact_3_rules,
    "compile_exact_3_rules",
    EXACT_PERMISSION_RAW
);
compile_bench!(
    should_compile_wildcard_3_rules,
    "compile_wildcard_3_rules",
    WILDCARD_PERMISSION_RAW
);
compile_bench!(
    should_compile_doublestar_3_rules,
    "compile_doublestar_3_rules",
    DOUBLESTAR_PERMISSION_RAW
);

fn warmed_permissions(raws: &[&str], route: &Route, access: Access) -> SessionPermissions {
    let permissions = SessionPermissions::from_permissions(parse_permissions(raws));
    let _ = permissions.allows(route, access);
    permissions
}

macro_rules! cache_hit_bench {
    ($fn_name:ident, $bench_name:literal, $raw:ident, $route:literal, $access:expr) => {
        #[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_permissions_hit");
            let route = Route::new($route);
            let permissions = warmed_permissions(&$raw, &route, $access);

            ctx.measure("operation", || {
                black_box(permissions.allows(black_box(&route), black_box($access)));
            });
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

#[stress(
    tier = 1,
    name = "allows_deny_by_default_cache_hit",
    max_allocs_per_op = 0,
    max_bytes_per_op = 0
)]
fn should_allows_deny_by_default_cache_hit(ctx: &mut StressContext) {
    record_group(ctx, "hotpath_permissions_hit");
    let permissions = SessionPermissions::empty();
    let route = Route::new("rpc://acme/auth/users");
    let _ = permissions.allows(&route, Access::Read);

    ctx.measure("operation", || {
        black_box(permissions.allows(black_box(&route), black_box(Access::Read)));
    });
}

macro_rules! cache_miss_bench {
    ($fn_name:ident, $bench_name:literal, $raw:ident, $route:literal, $access:expr) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "hotpath_permissions_miss");
            let route = Route::new($route);

            ctx.measure("operation", || {
                let permissions = SessionPermissions::from_permissions(parse_permissions(&$raw));
                black_box(permissions.allows(black_box(&route), black_box($access)));
            });
        }
    };
}

cache_miss_bench!(
    should_allows_exact_granted_cache_miss,
    "allows_exact_granted_cache_miss",
    EXACT_PERMISSION_RAW,
    "rpc://acme/auth/users",
    Access::Write
);
cache_miss_bench!(
    should_allows_wildcard_granted_cache_miss,
    "allows_wildcard_granted_cache_miss",
    WILDCARD_PERMISSION_RAW,
    "rpc://acme/admin/users",
    Access::Write
);
cache_miss_bench!(
    should_allows_doublestar_deep_cache_miss,
    "allows_doublestar_deep_cache_miss",
    DOUBLESTAR_PERMISSION_RAW,
    "rpc://acme/auth/users/session/create",
    Access::Write
);
cache_miss_bench!(
    should_allows_large_set_last_match_cache_miss,
    "allows_large_set_last_match_cache_miss",
    LARGE_PERMISSION_RAW,
    "kv://acme/app/users",
    Access::Write
);

stress_main!();
