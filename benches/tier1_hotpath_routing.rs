use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::runtime::routing::Route;

stress_allocator!();

fn record_group(ctx: &mut StressContext) {
    ctx.parameter("group", "hotpath_routing");
}

macro_rules! route_new_bench {
    ($fn_name:ident, $bench_name:literal, $segments:expr, [$($route:literal),+ $(,)?]) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx);
            ctx.parameter("segments", $segments);
            let routes = [$($route.to_string()),+];
            let mut index = 0usize;

            ctx.measure($bench_name, || {
                let route = &routes[index];
                index = (index + 1) % routes.len();
                black_box(Route::new(black_box(route)));
            });
        }
    };
}

route_new_bench!(
    should_route_new_2_segments,
    "route_new_2_segments",
    2,
    [
        "rpc://acme/auth",
        "notify://prod/events",
        "queue://staging/jobs",
    ]
);
route_new_bench!(
    should_route_new_3_segments,
    "route_new_3_segments",
    3,
    [
        "rpc://acme/auth/users",
        "notify://prod/events/orders",
        "queue://staging/jobs/worker",
    ]
);
route_new_bench!(
    should_route_new_4_segments,
    "route_new_4_segments",
    4,
    [
        "rpc://acme/auth/users/authenticate",
        "notify://prod/events/orders/created",
        "queue://staging/jobs/worker/process",
    ]
);
route_new_bench!(
    should_route_new_5_segments,
    "route_new_5_segments",
    5,
    [
        "rpc://acme/auth/users/session/create",
        "notify://prod/events/orders/items/added",
        "queue://staging/jobs/worker/task/execute",
    ]
);
route_new_bench!(
    should_route_new_6_segments,
    "route_new_6_segments",
    6,
    [
        "rpc://acme/auth/users/session/token/refresh",
        "notify://prod/events/orders/items/status/changed",
        "queue://staging/jobs/worker/task/result/complete",
    ]
);

stress_main!();
