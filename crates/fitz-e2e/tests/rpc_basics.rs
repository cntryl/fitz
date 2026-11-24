//! RPC wire-protocol error-code contract tests.
//!
//! These lock in the numeric values of documented RPC error codes, which
//! external clients branch on. The rest of RPC request/response behavior
//! (dispatch, sequencing, timeouts, reassembly) is covered end-to-end in
//! `rpc_advanced.rs` and `rpc_e2e/` against a real running broker.

#[test]
fn should_define_error_code_6053_rpc_invalid_sequence() {
    let code = fitz::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE;
    assert_eq!(code, 6053, "6053 = RPC_INVALID_SEQUENCE");
}

#[test]
fn should_define_error_code_6054_rpc_duplicate_correlation() {
    let code = fitz::protocol::error_codes::rpc::ERR_RPC_DUPLICATE_CORRELATION;
    assert_eq!(code, 6054, "6054 = RPC_DUPLICATE_CORRELATION");
}

#[test]
fn should_define_error_code_6055_rpc_wrong_worker() {
    let code = fitz::protocol::error_codes::rpc::ERR_RPC_WRONG_WORKER;
    assert_eq!(code, 6055, "6055 = RPC_WRONG_WORKER");
}
