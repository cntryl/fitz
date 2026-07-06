use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::runtime::routing::Route;

stress_allocator!();

fn record_group(ctx: &mut StressContext) {
    ctx.parameter("group", "hotpath_routing");
}

macro_rules! route_new_bench {
    ($fn_name:ident, $bench_name:literal, $segments:expr, $route:literal) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx);
            ctx.parameter("segments", $segments);
            ctx.parameter("route_bytes", $route.len());
            let route = $route.to_string();

            ctx.measure($bench_name, || {
                black_box(Route::new(black_box(&route)));
            });
        }
    };
}

route_new_bench!(
    should_route_new_2_segments,
    "route_new_2_segments",
    2,
    "rpc://realm-alpha-000000000000000000000000000000000000/auth-command-00000000000000000000000000000000"
);
route_new_bench!(
    should_route_new_3_segments,
    "route_new_3_segments",
    3,
    "rpc://realm-alpha-000000000000000000000000000000000000/auth-command-00000000000000000000000000000000/users-bucket-0000000000000000000000000000000"
);
route_new_bench!(
    should_route_new_4_segments,
    "route_new_4_segments",
    4,
    "rpc://realm-alpha-000000000000000000000000000000000000/auth-command-00000000000000000000000000000000/users-bucket-0000000000000000000000000000000/session-create-000000000000000000000000000"
);
route_new_bench!(
    should_route_new_5_segments,
    "route_new_5_segments",
    5,
    "rpc://realm-alpha-000000000000000000000000000000000000/auth-command-00000000000000000000000000000000/users-bucket-0000000000000000000000000000000/session-create-000000000000000000000000000/token-issue-0000000000000000000000000000"
);
route_new_bench!(
    should_route_new_6_segments,
    "route_new_6_segments",
    6,
    "rpc://realm-alpha-000000000000000000000000000000000000/auth-command-00000000000000000000000000000000/users-bucket-0000000000000000000000000000000/session-create-000000000000000000000000000/token-issue-0000000000000000000000000000/refresh-window-00000000000000000000000"
);

stress_main!();
