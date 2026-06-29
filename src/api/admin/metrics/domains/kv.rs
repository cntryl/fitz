use crate::boot::Runtime;

pub(super) fn append_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_kv_transactions_active Active KV transactions\n");
    output.push_str("# TYPE fitz_kv_transactions_active gauge\n");
    output.push_str(&format!(
        "fitz_kv_transactions_active {}\n",
        runtime.kv_transactions_active()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_kv_keys_total Total number of keys\n");
    output.push_str("# TYPE fitz_kv_keys_total gauge\n");
    output.push_str(&format!("fitz_kv_keys_total {}\n", runtime.kv_keys_total()));
    output.push('\n');

    output.push_str(
        "# HELP fitz_kv_commits_failed_total Total KV transaction commits that failed to persist\n",
    );
    output.push_str("# TYPE fitz_kv_commits_failed_total counter\n");
    output.push_str(&format!(
        "fitz_kv_commits_failed_total {}\n",
        runtime.kv_commits_failed_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_kv_rollbacks_total Total KV transaction rollbacks processed by this broker process\n");
    output.push_str("# TYPE fitz_kv_rollbacks_total counter\n");
    output.push_str(&format!(
        "fitz_kv_rollbacks_total {}\n",
        runtime.kv_rollbacks_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_kv_invalid_transaction_rejects_total Total KV operations rejected because the transaction was invalid\n");
    output.push_str("# TYPE fitz_kv_invalid_transaction_rejects_total counter\n");
    output.push_str(&format!(
        "fitz_kv_invalid_transaction_rejects_total {}\n",
        runtime.kv_invalid_transaction_rejects_total()
    ));
    output.push('\n');
}
