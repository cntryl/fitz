//! RPC wire-protocol error-code contract tests.
//!
//! These lock in the numeric values of documented RPC error codes, which
//! external clients branch on. The rest of RPC request/response behavior
//! (dispatch, sequencing, timeouts, reassembly) is covered end-to-end in
//! `rpc_advanced.rs` and `rpc_e2e/` against a real running broker.

#[test]
fn should_define_error_code_6006_rpc_invalid_sequence() {
    let code = fitz::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE;
    assert_eq!(code, 6006, "6006 = RPC_INVALID_SEQUENCE");
}

#[test]
fn should_define_error_code_6007_rpc_duplicate_correlation() {
    let code = fitz::protocol::error_codes::rpc::ERR_RPC_DUPLICATE_CORRELATION;
    assert_eq!(code, 6007, "6007 = RPC_DUPLICATE_CORRELATION");
}

#[test]
fn should_define_error_code_6008_rpc_wrong_worker() {
    let code = fitz::protocol::error_codes::rpc::ERR_RPC_WRONG_WORKER;
    assert_eq!(code, 6008, "6008 = RPC_WRONG_WORKER");
}
