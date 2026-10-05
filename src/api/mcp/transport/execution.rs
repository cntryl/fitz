use crate::api::mcp::McpExecutionContext;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

pub(super) fn admit(
    slots: &Arc<Semaphore>,
    context: &McpExecutionContext,
    operation: &str,
    cancellation: &CancellationToken,
) -> Result<OwnedSemaphorePermit, String> {
    if cancellation.is_cancelled() {
        context.record_transport_denial(operation, "request_cancelled_before_admission");
        return Err("MCP request was cancelled before admission".into());
    }
    slots.clone().try_acquire_owned().map_err(|_| {
        crate::api::mcp::telemetry::record_overload();
        context.record_transport_denial(operation, "execution_capacity_full");
        "MCP execution capacity is full; retry later".to_string()
    })
}

pub(super) fn spawn(
    permit: OwnedSemaphorePermit,
    cancellation: CancellationToken,
    context: McpExecutionContext,
    operation: String,
    work: impl FnOnce() -> Result<serde_json::Value, String> + Send + 'static,
) -> tokio::task::JoinHandle<Result<serde_json::Value, String>> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if cancellation.is_cancelled() {
            context.record_transport_denial(&operation, "request_cancelled_before_worker_start");
            return Err("MCP request was cancelled before execution".into());
        }
        work()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::permissions::SessionPermissions;

    #[test]
    fn should_reject_cancelled_request_before_execution_admission() {
        // Arrange
        let slots = Arc::new(Semaphore::new(1));
        let context = McpExecutionContext::anonymous(SessionPermissions::empty());
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        // Act
        let result = admit(&slots, &context, "test_read", &cancellation);

        // Assert
        assert!(result.is_err());
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(
            context.audit_records()[0].result_summary,
            "request_cancelled_before_admission"
        );
    }

    #[test]
    fn should_reject_when_execution_admission_is_full_without_waiting() {
        // Arrange
        let slots = Arc::new(Semaphore::new(1));
        let held = slots.clone().try_acquire_owned().unwrap();
        let context = McpExecutionContext::anonymous(SessionPermissions::empty());

        // Act
        let result = admit(&slots, &context, "test_read", &CancellationToken::new());

        // Assert
        assert!(result.is_err());
        assert_eq!(
            context.audit_records()[0].result_summary,
            "execution_capacity_full"
        );
        drop(held);
    }

    #[tokio::test]
    async fn should_keep_worker_permit_until_completion_after_disconnect() {
        // Arrange
        let slots = Arc::new(Semaphore::new(1));
        let context = McpExecutionContext::anonymous(SessionPermissions::empty());
        let cancellation = CancellationToken::new();
        let permit = admit(&slots, &context, "test_read", &cancellation).unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = spawn(
            permit,
            cancellation.clone(),
            context,
            "test_read".into(),
            move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(serde_json::Value::Null)
            },
        );
        entered_rx.await.unwrap();

        // Act
        cancellation.cancel();
        drop(worker);
        let retained = slots.available_permits();
        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while slots.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        // Assert
        assert_eq!(retained, 0);
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn should_skip_queued_execution_cancelled_before_worker_starts() {
        // Arrange
        let slots = Arc::new(Semaphore::new(1));
        let context = McpExecutionContext::anonymous(SessionPermissions::empty());
        let cancellation = CancellationToken::new();
        let permit = admit(&slots, &context, "test_read", &cancellation).unwrap();
        cancellation.cancel();
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_by_worker = called.clone();

        // Act
        let result = spawn(
            permit,
            cancellation,
            context,
            "test_read".into(),
            move || {
                called_by_worker.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(serde_json::Value::Null)
            },
        )
        .await
        .unwrap();

        // Assert
        assert!(result.is_err());
        assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(slots.available_permits(), 1);
    }
}
