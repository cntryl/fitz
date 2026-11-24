use super::TestServer;

impl TestServer {
    /// Start a memory broker with queue retention expressed in seconds for deadline tests.
    ///
    /// # Errors
    /// Returns an error if storage, runtime, or listeners cannot be initialized.
    pub async fn start_with_queue_retention(
        retention: crate::domains::queue::retention::QueueRetention,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options_and_message_limit(
            false,
            None,
            crate::boot::runtime::StorageMode::Memory,
            false,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            super::default_test_route_family_mappings(),
            16_777_216,
            retention,
        )
        .await
    }
}
