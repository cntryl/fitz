use crate::boot::Runtime;

pub(super) fn append_broker_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_uptime_seconds Broker uptime in seconds\n");
    output.push_str("# TYPE fitz_uptime_seconds gauge\n");
    output.push_str(&format!(
        "fitz_uptime_seconds {}\n",
        runtime.uptime().as_secs()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_connections_total Total number of active connections\n");
    output.push_str("# TYPE fitz_connections_total gauge\n");
    output.push_str(&format!(
        "fitz_connections_total {}\n",
        runtime.connection_count()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_sessions_total Total number of active sessions\n");
    output.push_str("# TYPE fitz_sessions_total gauge\n");
    output.push_str(&format!(
        "fitz_sessions_total {}\n",
        runtime.session_count()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_messages_received_total Total messages received\n");
    output.push_str("# TYPE fitz_messages_received_total counter\n");
    output.push_str(&format!(
        "fitz_messages_received_total {}\n",
        runtime.messages_received()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_messages_sent_total Total messages sent\n");
    output.push_str("# TYPE fitz_messages_sent_total counter\n");
    output.push_str(&format!(
        "fitz_messages_sent_total {}\n",
        runtime.messages_sent()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_router_backpressure_total Total router delivery failures caused by normal-lane mailbox saturation\n",
    );
    output.push_str("# TYPE fitz_router_backpressure_total counter\n");
    output.push_str(&format!(
        "fitz_router_backpressure_total {}\n",
        runtime.router_backpressure_total()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_router_high_lane_backpressure_total Total router delivery failures caused by control-plane high-lane saturation\n",
    );
    output.push_str("# TYPE fitz_router_high_lane_backpressure_total counter\n");
    output.push_str(&format!(
        "fitz_router_high_lane_backpressure_total {}\n",
        runtime.router_high_lane_backpressure_total()
    ));
    output.push('\n');
}
