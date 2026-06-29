use crate::boot::Runtime;
use std::fmt::Write as _;

pub(super) fn append_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_kv_transactions_active Active KV transactions\n");
    output.push_str("# TYPE fitz_kv_transactions_active gauge\n");
    let _ = writeln!(
        output,
        "fitz_kv_transactions_active {}",
        runtime.kv_transactions_active()
    );
    output.push('\n');

    output.push_str("# HELP fitz_kv_keys_total Total number of keys\n");
    output.push_str("# TYPE fitz_kv_keys_total gauge\n");
    let _ = writeln!(output, "fitz_kv_keys_total {}", runtime.kv_keys_total());
    output.push('\n');

    output.push_str(
        "# HELP fitz_kv_commits_failed_total Total KV transaction commits that failed to persist\n",
    );
    output.push_str("# TYPE fitz_kv_commits_failed_total counter\n");
    let _ = writeln!(
        output,
        "fitz_kv_commits_failed_total {}",
        runtime.kv_commits_failed_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_kv_rollbacks_total Total KV transaction rollbacks processed by this broker process\n");
    output.push_str("# TYPE fitz_kv_rollbacks_total counter\n");
    let _ = writeln!(
        output,
        "fitz_kv_rollbacks_total {}",
        runtime.kv_rollbacks_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_kv_invalid_transaction_rejects_total Total KV operations rejected because the transaction was invalid\n");
    output.push_str("# TYPE fitz_kv_invalid_transaction_rejects_total counter\n");
    let _ = writeln!(
        output,
        "fitz_kv_invalid_transaction_rejects_total {}",
        runtime.kv_invalid_transaction_rejects_total()
    );
    output.push('\n');
}
