use crate::core::engine::EngineHandle;

/// KV API: simple key-value storage over the engine + store.
#[derive(Clone, Debug)]
pub struct Kv {
    engine: EngineHandle,
}

impl Kv {
    pub fn new(engine: EngineHandle) -> Self {
        Self { engine }
    }

    /// Put a key-value pair. Route is the kv namespace (e.g., "kv://realm/area/bucket").
    pub async fn put(&self, route: String, key: String, value: Vec<u8>) -> Result<(), String> {
        self.engine.kv_put(route, key, value).await
    }

    /// Get a value by key. Returns None if not found.
    pub async fn get(&self, route: String, key: String) -> Result<Option<Vec<u8>>, String> {
        self.engine.kv_get(route, key).await
    }

    /// Delete a key. Returns Ok(()) even if the key didn't exist.
    pub async fn delete(&self, route: String, key: String) -> Result<(), String> {
        self.engine.kv_delete(route, key).await
    }

    /// Scan keys starting from `start_key` (inclusive) up to `limit` results.
    /// Returns a vector of (key, value) tuples ordered by key.
    pub async fn scan_ge(&self, route: String, start_key: String, limit: usize) -> Result<Vec<(String, Vec<u8>)>, String> {
        self.engine.kv_scan_ge(route, start_key, limit).await
    }

    /// Put multiple key-value pairs in a single batch operation.
    pub async fn put_batch(&self, route: String, items: Vec<(String, Vec<u8>)>) -> Result<(), String> {
        self.engine.kv_put_batch(route, items).await
    }

    /// Get multiple values by keys in a single batch operation.
    /// Returns a vector of (key, Option<value>) tuples in the same order as requested.
    pub async fn get_batch(&self, route: String, keys: Vec<String>) -> Result<Vec<(String, Option<Vec<u8>>)>, String> {
        self.engine.kv_get_batch(route, keys).await
    }

    /// Delete all keys in the range [start_key, end_key) (start inclusive, end exclusive).
    /// Returns the number of keys deleted.
    pub async fn delete_range(&self, route: String, start_key: String, end_key: String) -> Result<u64, String> {
        self.engine.kv_delete_range(route, start_key, end_key).await
    }
}
