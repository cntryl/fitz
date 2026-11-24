use crate::boot::Runtime;
use std::fmt::Write as _;

pub(super) fn append_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_notice_subscriptions_active Active subscriptions\n");
    output.push_str("# TYPE fitz_notice_subscriptions_active gauge\n");
    let _ = writeln!(
        output,
        "fitz_notice_subscriptions_active {}",
        runtime.notice_subscriptions_active()
    );
    output.push('\n');

    output.push_str("# HELP fitz_notice_response_drops_total Total Notice responses dropped by this broker process\n# TYPE fitz_notice_response_drops_total counter\n");
    let _ = writeln!(
        output,
        "fitz_notice_response_drops_total {}",
        runtime.notice_response_drops_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_notice_routes_active Active notice routes\n");
    output.push_str("# TYPE fitz_notice_routes_active gauge\n");
    let _ = writeln!(
        output,
        "fitz_notice_routes_active {}",
        runtime.notice_routes_active()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_notice_max_route_subscribers Peak subscribers on a single notice route\n",
    );
    output.push_str("# TYPE fitz_notice_max_route_subscribers gauge\n");
    let _ = writeln!(
        output,
        "fitz_notice_max_route_subscribers {}",
        runtime.notice_max_route_subscribers()
    );
    output.push('\n');

    output.push_str("# HELP fitz_notice_unsubscribes_total Total notice unsubscriptions processed by this broker process\n");
    output.push_str("# TYPE fitz_notice_unsubscribes_total counter\n");
    let _ = writeln!(
        output,
        "fitz_notice_unsubscribes_total {}",
        runtime.notice_unsubscribes_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_notice_delivery_drops_total Total notice deliveries dropped by this broker process\n");
    output.push_str("# TYPE fitz_notice_delivery_drops_total counter\n");
    let _ = writeln!(
        output,
        "fitz_notice_delivery_drops_total {}",
        runtime.notice_delivery_drops_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_notice_wildcard_limit_rejects_total Total notice wildcard deliveries rejected because of the route limit\n");
    output.push_str("# TYPE fitz_notice_wildcard_limit_rejects_total counter\n");
    let _ = writeln!(
        output,
        "fitz_notice_wildcard_limit_rejects_total {}",
        runtime.notice_wildcard_limit_rejects_total()
    );
    output.push('\n');
}
