use crate::boot::Runtime;
use std::fmt::Write as _;

pub(super) fn append_broker_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_uptime_seconds Broker uptime in seconds\n");
    output.push_str("# TYPE fitz_uptime_seconds gauge\n");
    let _ = writeln!(output, "fitz_uptime_seconds {}", runtime.uptime().as_secs());
    output.push('\n');

    output.push_str("# HELP fitz_connections_total Total number of active connections\n");
    output.push_str("# TYPE fitz_connections_total gauge\n");
    let _ = writeln!(
        output,
        "fitz_connections_total {}",
        runtime.connection_count()
    );
    output.push('\n');

    output.push_str("# HELP fitz_sessions_total Total number of active sessions\n");
    output.push_str("# TYPE fitz_sessions_total gauge\n");
    let _ = writeln!(output, "fitz_sessions_total {}", runtime.session_count());
    output.push('\n');

    output.push_str("# HELP fitz_messages_received_total Total messages received\n");
    output.push_str("# TYPE fitz_messages_received_total counter\n");
    let _ = writeln!(
        output,
        "fitz_messages_received_total {}",
        runtime.messages_received()
    );
    output.push('\n');

    output.push_str("# HELP fitz_messages_sent_total Total messages sent\n");
    output.push_str("# TYPE fitz_messages_sent_total counter\n");
    let _ = writeln!(
        output,
        "fitz_messages_sent_total {}",
        runtime.messages_sent()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_router_backpressure_total Total router delivery failures caused by normal-lane mailbox saturation\n",
    );
    output.push_str("# TYPE fitz_router_backpressure_total counter\n");
    let _ = writeln!(
        output,
        "fitz_router_backpressure_total {}",
        runtime.router_backpressure_total()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_router_high_lane_backpressure_total Total router delivery failures caused by control-plane high-lane saturation\n",
    );
    output.push_str("# TYPE fitz_router_high_lane_backpressure_total counter\n");
    let _ = writeln!(
        output,
        "fitz_router_high_lane_backpressure_total {}",
        runtime.router_high_lane_backpressure_total()
    );
    output.push('\n');
}
