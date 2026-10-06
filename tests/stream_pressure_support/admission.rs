pub fn batch_limit(error: &str) -> bool {
    // The actor validates before storage and emits the normalized message.
    // Storage's stricter checks retain their explicit prefix. Other generic
    // backend errors (including write stalls) must remain failed diagnostics.
    error == "Stream error code 2012: batch too large"
        || error.starts_with("Stream error code 2012: ERR_BATCH_TOO_LARGE:")
}

pub fn rollback_success(body: &[u8]) -> Result<(), String> {
    if body == [0] {
        Ok(())
    } else {
        Err("unexpected Stream ROLLBACK response data".into())
    }
}
