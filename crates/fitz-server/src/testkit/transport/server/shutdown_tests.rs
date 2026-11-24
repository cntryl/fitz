use super::*;
use crate::runtime::{DeliveryError, Envelope, MailboxSink};
use std::sync::atomic::{AtomicUsize, Ordering};

struct StorageRetainingCleanupSink {
    _store: Arc<cntryl_midge::Engine>,
    attempts: Arc<AtomicUsize>,
}

impl MailboxSink for StorageRetainingCleanupSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(DeliveryError::MailboxFull {
                capacity: 1,
                current_len: 1,
            });
        }
        // A synchronous cleanup retry may legitimately outlive the fixture's
        // two-second wait for exclusive storage ownership.
        std::thread::sleep(Duration::from_secs(3));
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[tokio::test]
async fn should_drain_storage_retaining_cleanup_before_test_server_shutdown() {
    // Arrange
    let server = TestServer::start().await.expect("start server");
    let attempts = Arc::new(AtomicUsize::new(0));
    server.runtime.router().register_domain_pattern(
        "rpc",
        Arc::new(StorageRetainingCleanupSink {
            _store: server.store.clone(),
            attempts: attempts.clone(),
        }),
    );
    let _client = server.connect().await.expect("connect client");
    server
        .wait_for_session_count(1)
        .await
        .expect("session open");

    // Act
    let result = server.shutdown().await;

    // Assert
    assert!(result.is_ok(), "cleanup must release storage: {result:?}");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}
