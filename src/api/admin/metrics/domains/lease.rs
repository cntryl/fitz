use crate::boot::Runtime;
use std::fmt::Write as _;

pub(super) fn append_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_lease_active Active leases\n");
    output.push_str("# TYPE fitz_lease_active gauge\n");
    let _ = writeln!(output, "fitz_lease_active {}", runtime.lease_active());
    output.push('\n');

    output.push_str("# HELP fitz_lease_response_drops_total Total Lease responses dropped by this broker process\n# TYPE fitz_lease_response_drops_total counter\n");
    let _ = writeln!(
        output,
        "fitz_lease_response_drops_total {}",
        runtime.lease_response_drops_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_lease_notify_drops_total Total Lease notifications dropped by this broker process\n# TYPE fitz_lease_notify_drops_total counter\n");
    let _ = writeln!(
        output,
        "fitz_lease_notify_drops_total {}",
        runtime.lease_notify_drops_total()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_lease_oldest_lease_age_seconds Oldest active lease age in seconds\n",
    );
    output.push_str("# TYPE fitz_lease_oldest_lease_age_seconds gauge\n");
    let _ = writeln!(
        output,
        "fitz_lease_oldest_lease_age_seconds {}",
        runtime.lease_oldest_lease_age_seconds()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_lease_waiter_depth Total queued lease waiters across all resources\n",
    );
    output.push_str("# TYPE fitz_lease_waiter_depth gauge\n");
    let _ = writeln!(
        output,
        "fitz_lease_waiter_depth {}",
        runtime.lease_waiter_depth()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_lease_ownership_churn_total Successful lease renewals and ownership churn events\n",
    );
    output.push_str("# TYPE fitz_lease_ownership_churn_total counter\n");
    let _ = writeln!(
        output,
        "fitz_lease_ownership_churn_total {}",
        runtime.lease_ownership_churn_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_lease_acquire_timeouts_total Total lease acquire requests that timed out before ownership was granted\n");
    output.push_str("# TYPE fitz_lease_acquire_timeouts_total counter\n");
    let _ = writeln!(
        output,
        "fitz_lease_acquire_timeouts_total {}",
        runtime.lease_acquire_timeouts_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_lease_forced_releases_total Total lease releases forced by administrative or conflict handling paths\n");
    output.push_str("# TYPE fitz_lease_forced_releases_total counter\n");
    let _ = writeln!(
        output,
        "fitz_lease_forced_releases_total {}",
        runtime.lease_forced_releases_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_lease_invalid_token_rejects_total Total lease operations rejected because the provided token was invalid\n");
    output.push_str("# TYPE fitz_lease_invalid_token_rejects_total counter\n");
    let _ = writeln!(
        output,
        "fitz_lease_invalid_token_rejects_total {}",
        runtime.lease_invalid_token_rejects_total()
    );
    output.push('\n');
}
