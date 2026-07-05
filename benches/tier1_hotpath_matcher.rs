use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::runtime::matcher::Pattern;
use fitz::runtime::routing::Route;

stress_allocator!();

fn record_group(ctx: &mut StressContext) {
    ctx.parameter("group", "hotpath_matcher");
}

#[stress(
    tier = 1,
    name = "exact_literal_match",
    max_allocs_per_op = 0,
    max_bytes_per_op = 0
)]
fn should_exact_literal_match(ctx: &mut StressContext) {
    record_group(ctx);
    let pattern = Pattern::new("notify://acme/orders/create");
    let route = Route::new("notify://acme/orders/create");

    ctx.measure("exact_literal_match", || {
        black_box(pattern.matches(black_box(&route)))
    });
}

#[stress(
    tier = 1,
    name = "single_star_match",
    max_allocs_per_op = 0,
    max_bytes_per_op = 0
)]
fn should_single_star_match(ctx: &mut StressContext) {
    record_group(ctx);
    let pattern = Pattern::new("notify://acme/orders/*");
    let routes = [
        Route::new("notify://acme/orders/create"),
        Route::new("notify://acme/orders/update"),
    ];
    let mut index = 0usize;

    ctx.measure("single_star_match", || {
        let matched = pattern.matches(black_box(&routes[index]));
        index = (index + 1) % routes.len();
        black_box(matched)
    });
}

#[stress(
    tier = 1,
    name = "double_star_at_end",
    max_allocs_per_op = 0,
    max_bytes_per_op = 0
)]
fn should_double_star_at_end(ctx: &mut StressContext) {
    record_group(ctx);
    let pattern = Pattern::new("notify://acme/orders/**");
    let routes = [
        Route::new("notify://acme/orders"),
        Route::new("notify://acme/orders/create"),
        Route::new("notify://acme/orders/items/history/view"),
    ];
    let mut index = 0usize;

    ctx.measure("double_star_at_end", || {
        let matched = pattern.matches(black_box(&routes[index]));
        index = (index + 1) % routes.len();
        black_box(matched)
    });
}

#[stress(
    tier = 1,
    name = "double_star_in_middle",
    max_allocs_per_op = 0,
    max_bytes_per_op = 0
)]
fn should_double_star_in_middle(ctx: &mut StressContext) {
    record_group(ctx);
    let pattern = Pattern::new("notify://acme/**/created");
    let routes = [
        Route::new("notify://acme/created"),
        Route::new("notify://acme/orders/created"),
        Route::new("notify://acme/orders/items/history/created"),
    ];
    let mut index = 0usize;

    ctx.measure("double_star_in_middle", || {
        let matched = pattern.matches(black_box(&routes[index]));
        index = (index + 1) % routes.len();
        black_box(matched)
    });
}

#[stress(
    tier = 1,
    name = "negative_match_late_fail",
    max_allocs_per_op = 0,
    max_bytes_per_op = 0
)]
fn should_negative_match_late_fail(ctx: &mut StressContext) {
    record_group(ctx);
    let pattern = Pattern::new("notify://acme/*/items/history/created");
    let route_fail = Route::new("notify://acme/orders/items/history/updated");

    ctx.measure("negative_match_late_fail", || {
        black_box(pattern.matches(black_box(&route_fail)))
    });
}

macro_rules! depth_bench {
    ($fn_name:ident, $bench_name:literal, $route:literal) => {
        #[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx);
            let pattern = Pattern::new("notify://acme/orders/**");
            let route = Route::new($route);

            ctx.measure($bench_name, || {
                black_box(pattern.matches(black_box(&route)))
            });
        }
    };
}

depth_bench!(should_match_depth_1, "depth_1", "notify://acme/orders/x");
depth_bench!(
    should_match_depth_3,
    "depth_3",
    "notify://acme/orders/a/b/c"
);
depth_bench!(
    should_match_depth_5,
    "depth_5",
    "notify://acme/orders/a/b/c/d/e"
);
depth_bench!(
    should_match_depth_10,
    "depth_10",
    "notify://acme/orders/a/b/c/d/e/f/g/h/i/j"
);

macro_rules! pattern_complexity_bench {
    ($fn_name:ident, $bench_name:literal, $pattern:literal) => {
        #[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx);
            let pattern = Pattern::new($pattern);
            let route = Route::new("notify://acme/orders/items/history/created");

            ctx.measure($bench_name, || {
                black_box(pattern.matches(black_box(&route)))
            });
        }
    };
}

pattern_complexity_bench!(
    should_match_all_literals,
    "all_literals",
    "notify://acme/orders/items/history/created"
);
pattern_complexity_bench!(
    should_match_one_star,
    "one_star",
    "notify://acme/orders/*/history/created"
);
pattern_complexity_bench!(
    should_match_two_stars,
    "two_stars",
    "notify://acme/*/items/*/created"
);
pattern_complexity_bench!(
    should_match_three_stars,
    "three_stars",
    "notify://acme/*/*/*/*"
);
pattern_complexity_bench!(
    should_match_double_star,
    "double_star",
    "notify://acme/**/created"
);

macro_rules! backtracking_bench {
    ($fn_name:ident, $bench_name:literal, $route:literal) => {
        #[stress(tier = 1, max_allocs_per_op = 0, max_bytes_per_op = 0)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx);
            let pattern = Pattern::new("notify://acme/**/items/created");
            let route = Route::new($route);

            ctx.measure($bench_name, || {
                black_box(pattern.matches(black_box(&route)))
            });
        }
    };
}

backtracking_bench!(
    should_backtrack_0_segments,
    "backtrack_0_segments",
    "notify://acme/items/created"
);
backtracking_bench!(
    should_backtrack_1_segment,
    "backtrack_1_segment",
    "notify://acme/orders/items/created"
);
backtracking_bench!(
    should_backtrack_2_segments,
    "backtrack_2_segments",
    "notify://acme/orders/details/items/created"
);
backtracking_bench!(
    should_backtrack_4_segments,
    "backtrack_4_segments",
    "notify://acme/a/b/c/d/items/created"
);

stress_main!();
