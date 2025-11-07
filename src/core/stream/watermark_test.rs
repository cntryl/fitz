//! Tests to confirm that reading area streams respects the watermark
//! and prevents consumers from getting ahead of uncommitted transactions.

#[cfg(test)]
mod tests {
    use crate::core::stream::service::{StreamService, StreamOperationParams, StreamResponse};
    use crate::core::stream::types::StreamOperation;
    use crate::storage::traits::{KvStore, KvTransaction};
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Mock KV store with transaction support for testing watermark behavior
    struct MockKvStore {
        data: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    }

    impl MockKvStore {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl KvStore for MockKvStore {
        fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
            self.data
                .lock()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<Bytes>, String> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .get(key)
                .map(|v| Bytes::from(v.clone())))
        }

        fn delete(&self, key: &[u8]) -> Result<(), String> {
            self.data.lock().unwrap().remove(key);
            Ok(())
        }

        fn put_batch(&self, writes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
            let mut data = self.data.lock().unwrap();
            for (key, value) in writes {
                data.insert(key, value);
            }
            Ok(())
        }

        fn delete_batch(&self, keys: Vec<Vec<u8>>) -> Result<(), String> {
            let mut data = self.data.lock().unwrap();
            for key in keys {
                data.remove(&key);
            }
            Ok(())
        }

        fn scan(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<(Bytes, Bytes)>, String> {
            Ok(vec![])
        }

        fn flush(&self) -> Result<(), String> {
            Ok(())
        }

        fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>, String> {
            Ok(Box::new(MockTransaction {
                data: self.data.clone(),
                writes: HashMap::new(),
                committed: false,
            }))
        }
    }

    /// Mock transaction that buffers writes until commit
    struct MockTransaction {
        data: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
        writes: HashMap<Vec<u8>, Vec<u8>>,
        committed: bool,
    }

    impl KvTransaction for MockTransaction {
        fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
            self.writes.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<Bytes>, String> {
            // Check buffered writes first
            if let Some(value) = self.writes.get(key) {
                return Ok(Some(Bytes::from(value.clone())));
            }
            // Fall back to committed data
            Ok(self
                .data
                .lock()
                .unwrap()
                .get(key)
                .map(|v| Bytes::from(v.clone())))
        }

        fn delete(&mut self, key: &[u8]) -> Result<(), String> {
            self.writes.remove(key);
            Ok(())
        }

        fn scan(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<(Bytes, Bytes)>, String> {
            // Not needed for these tests
            Ok(vec![])
        }

        fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
            // Check if key exists in committed data or buffered writes
            if self.writes.contains_key(key) {
                return Err("Key already exists".to_string());
            }
            if self.data.lock().unwrap().contains_key(key) {
                return Err("Key already exists".to_string());
            }
            self.writes.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn commit(mut self: Box<Self>) -> Result<(), String> {
            let mut data = self.data.lock().unwrap();
            for (key, value) in &self.writes {
                data.insert(key.clone(), value.clone());
            }
            self.committed = true;
            Ok(())
        }

        fn rollback(self: Box<Self>) -> Result<(), String> {
            // Just drop buffered writes
            Ok(())
        }
    }

    #[tokio::test]
    async fn should_not_return_uncommitted_events_when_reading_area() {
        // Arrange
        let store = Arc::new(MockKvStore::new());
        let mut service = StreamService::new(store.clone());
        let route = "realm/area/stream1";
        let channel_id = 1;

        // Begin append transaction
        let begin_result = service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::BeginAppend,
                route,
                channel_id,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: None,
                limit: None,
            })
            .await;
        assert!(begin_result.is_ok());

        // Append an event (but don't commit yet)
        let append_result = service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::Append,
                route,
                channel_id,
                body: Some(b"uncommitted event".to_vec()),
                metadata: None,
                is_end: false,
                from_seq: None,
                limit: None,
            })
            .await;
        assert!(append_result.is_ok());

        // Act - Try to read from area (before commit)
        let read_result = service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::ReadArea,
                route: "realm/area",
                channel_id: 2, // Different channel
                body: None,
                metadata: None,
                is_end: false,
                from_seq: Some(0),
                limit: Some(10),
            })
            .await;

        // Assert - Should succeed but return no events (watermark hasn't advanced)
        assert!(read_result.is_ok());
        match read_result.unwrap() {
            StreamResponse::AreaRead(response) => {
                assert_eq!(
                    response.events.len(),
                    0,
                    "Should not return uncommitted events"
                );
                assert_eq!(
                    response.watermark, 0,
                    "Watermark should be 0 (no committed events)"
                );
            }
            _ => panic!("Expected AreaRead response"),
        }
    }

    #[tokio::test]
    async fn should_return_events_only_after_commit_advances_watermark() {
        // Arrange
        let store = Arc::new(MockKvStore::new());
        let mut service = StreamService::new(store.clone());
        let route = "realm/area/stream1";
        let channel_id = 1;

        // Begin append transaction
        service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::BeginAppend,
                route,
                channel_id,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: None,
                limit: None,
            })
            .await
            .unwrap();

        // Append an event
        service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::Append,
                route,
                channel_id,
                body: Some(b"committed event".to_vec()),
                metadata: None,
                is_end: true,
                from_seq: None,
                limit: None,
            })
            .await
            .unwrap();

        // Commit the transaction
        service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::CommitAppend,
                route,
                channel_id,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: None,
                limit: None,
            })
            .await
            .unwrap();

        // Finalize the stream to assign area sequences and advance watermark
        service.finalize_stream(route).await.unwrap();

        // Act - Read from area (after commit and finalize)
        let read_result = service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::ReadArea,
                route: "realm/area",
                channel_id: 2,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: Some(0),
                limit: Some(10),
            })
            .await;

        // Assert - Should now return the committed event
        assert!(read_result.is_ok());
        match read_result.unwrap() {
            StreamResponse::AreaRead(response) => {
                assert_eq!(
                    response.events.len(),
                    1,
                    "Should return committed event after finalization"
                );
                assert_eq!(
                    response.watermark, 1,
                    "Watermark should advance to 1 after finalization"
                );
            }
            _ => panic!("Expected AreaRead response"),
        }
    }

    #[tokio::test]
    async fn should_only_return_events_up_to_watermark_with_out_of_order_commits() {
        // Arrange
        let store = Arc::new(MockKvStore::new());
        let mut service = StreamService::new(store.clone());

        // Simulate two streams committing out of order
        // Stream 1 will commit first (area_seq 0-0)
        let stream1_route = "realm/area/stream1";
        let stream2_route = "realm/area/stream2";

        // Start stream 2 first (allocates area_seq 0, but doesn't commit yet)
        service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::BeginAppend,
                route: stream2_route,
                channel_id: 2,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: None,
                limit: None,
            })
            .await
            .unwrap();

        service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::Append,
                route: stream2_route,
                channel_id: 2,
                body: Some(b"stream2 event".to_vec()),
                metadata: None,
                is_end: true,
                from_seq: None,
                limit: None,
            })
            .await
            .unwrap();

        // Note: stream2 NOT committed yet

        // Start and commit stream 1 (will get area_seq 1 when finalized)
        service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::BeginAppend,
                route: stream1_route,
                channel_id: 1,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: None,
                limit: None,
            })
            .await
            .unwrap();

        service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::Append,
                route: stream1_route,
                channel_id: 1,
                body: Some(b"stream1 event".to_vec()),
                metadata: None,
                is_end: true,
                from_seq: None,
                limit: None,
            })
            .await
            .unwrap();

        service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::CommitAppend,
                route: stream1_route,
                channel_id: 1,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: None,
                limit: None,
            })
            .await
            .unwrap();

        // Finalize stream1 (gets area_seq 0 since it finalizes first)
        service.finalize_stream(stream1_route).await.unwrap();

        // Act - Read from area before stream2 commits
        let read_result = service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::ReadArea,
                route: "realm/area",
                channel_id: 3,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: Some(0),
                limit: Some(10),
            })
            .await
            .unwrap();

        // Assert - Should only return stream1's event (watermark at 1)
        match read_result {
            StreamResponse::AreaRead(response) => {
                assert_eq!(
                    response.events.len(),
                    1,
                    "Should return only committed stream1 event"
                );
                assert_eq!(response.watermark, 1, "Watermark should be at 1");
            }
            _ => panic!("Expected AreaRead response"),
        }

        // Now commit stream2
        service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::CommitAppend,
                route: stream2_route,
                channel_id: 2,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: None,
                limit: None,
            })
            .await
            .unwrap();

        // Finalize stream2 (gets area_seq 1)
        service.finalize_stream(stream2_route).await.unwrap();

        // Read again
        let read_result2 = service
            .handle_operation(StreamOperationParams {
                operation: StreamOperation::ReadArea,
                route: "realm/area",
                channel_id: 3,
                body: None,
                metadata: None,
                is_end: false,
                from_seq: Some(0),
                limit: Some(10),
            })
            .await
            .unwrap();

        // Assert - Should now return both events (watermark at 2)
        match read_result2 {
            StreamResponse::AreaRead(response) => {
                assert_eq!(response.events.len(), 2, "Should return both events");
                assert_eq!(response.watermark, 2, "Watermark should advance to 2");
            }
            _ => panic!("Expected AreaRead response"),
        }
    }
}
