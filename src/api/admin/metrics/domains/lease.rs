use crate::boot::Runtime;

pub(super) fn append_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_lease_active Active leases\n");
    output.push_str("# TYPE fitz_lease_active gauge\n");
    output.push_str(&format!("fitz_lease_active {}\n", runtime.lease_active()));
    output.push('\n');

    output.push_str(
        "# HELP fitz_lease_oldest_lease_age_seconds Oldest active lease age in seconds\n",
    );
    output.push_str("# TYPE fitz_lease_oldest_lease_age_seconds gauge\n");
    output.push_str(&format!(
        "fitz_lease_oldest_lease_age_seconds {}\n",
        runtime.lease_oldest_lease_age_seconds()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_lease_waiter_depth Total queued lease waiters across all resources\n",
    );
    output.push_str("# TYPE fitz_lease_waiter_depth gauge\n");
    output.push_str(&format!(
        "fitz_lease_waiter_depth {}\n",
        runtime.lease_waiter_depth()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_lease_ownership_churn_total Successful lease renewals and ownership churn events\n",
    );
    output.push_str("# TYPE fitz_lease_ownership_churn_total counter\n");
    output.push_str(&format!(
        "fitz_lease_ownership_churn_total {}\n",
        runtime.lease_ownership_churn_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_lease_acquire_timeouts_total Total lease acquire requests that timed out before ownership was granted\n");
    output.push_str("# TYPE fitz_lease_acquire_timeouts_total counter\n");
    output.push_str(&format!(
        "fitz_lease_acquire_timeouts_total {}\n",
        runtime.lease_acquire_timeouts_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_lease_forced_releases_total Total lease releases forced by administrative or conflict handling paths\n");
    output.push_str("# TYPE fitz_lease_forced_releases_total counter\n");
    output.push_str(&format!(
        "fitz_lease_forced_releases_total {}\n",
        runtime.lease_forced_releases_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_lease_invalid_token_rejects_total Total lease operations rejected because the provided token was invalid\n");
    output.push_str("# TYPE fitz_lease_invalid_token_rejects_total counter\n");
    output.push_str(&format!(
        "fitz_lease_invalid_token_rejects_total {}\n",
        runtime.lease_invalid_token_rejects_total()
    ));
    output.push('\n');
}
