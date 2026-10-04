use super::McpResponse;
use crate::api::mcp::McpExecutionContext;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Response, StatusCode};

pub(super) const MAX_RESPONSE_BYTES: usize = 600 * 1024;

pub(super) fn result_fits(value: &impl serde::Serialize) -> bool {
    let mut writer = BudgetWriter(0);
    serde_json::to_writer(&mut writer, value).is_ok()
}

struct BudgetWriter(usize);

impl std::io::Write for BudgetWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_RESPONSE_BYTES.saturating_sub(self.0) {
            return Err(std::io::Error::other("MCP serialization budget exceeded"));
        }
        self.0 += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) async fn bound_response(
    response: McpResponse,
    context: &McpExecutionContext,
    request_id: Option<serde_json::Value>,
) -> McpResponse {
    // Compatibility GET streams carry no operational result. Bounded POST
    // calls use JSON; collect the actual encoded envelope, including its ID.
    if response
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes().starts_with(b"text/event-stream"))
    {
        return response;
    }
    let (parts, body) = response.into_parts();
    if let Ok(collected) = Limited::new(body, MAX_RESPONSE_BYTES).collect().await {
        Response::from_parts(parts, Full::new(collected.to_bytes()).boxed())
    } else {
        context.record_transport_denial("http_mcp", "protocol_response_over_limit");
        super::json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({
                "jsonrpc": "2.0", "id": request_id,
                "error": {"code": -32603, "message": "MCP response exceeds the protocol response limit"}
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::permissions::SessionPermissions;
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::{Response, StatusCode};

    #[tokio::test]
    async fn should_enforce_actual_envelope_bytes_including_request_id() {
        // Arrange
        let request_id = serde_json::Value::String("i".repeat(16 * 1024));
        let value = serde_json::json!({
            "jsonrpc": "2.0", "id": request_id,
            "result": {"content": "x".repeat(MAX_RESPONSE_BYTES - 100)}
        });
        let encoded = serde_json::to_vec(&value).unwrap();
        assert!(encoded.len() > MAX_RESPONSE_BYTES);
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(encoded)).boxed())
            .unwrap();
        let context = McpExecutionContext::anonymous(SessionPermissions::empty());

        // Act
        let response = bound_response(response, &context, Some(request_id.clone())).await;
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let reply: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        // Assert
        assert!(bytes.len() <= MAX_RESPONSE_BYTES);
        assert_eq!(reply["id"], request_id);
        assert_eq!(reply["error"]["code"], -32603);
        assert_eq!(
            context.audit_records()[0].result_summary,
            "protocol_response_over_limit"
        );
    }

    #[test]
    fn should_count_structured_content_and_text_fallback_during_serialization() {
        // Arrange
        let value = serde_json::json!({"text": "\"".repeat(MAX_RESPONSE_BYTES / 6)});
        let result = rmcp::model::CallToolResult::structured(value.clone());

        // Act
        let value_fits = result_fits(&value);
        let envelope_content_fits = result_fits(&result);

        // Assert
        assert!(value_fits);
        assert!(!envelope_content_fits);
    }
}
