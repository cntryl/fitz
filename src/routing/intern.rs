//! GlobalInternTable
//!
//! High-performance global string interner for Fitz route segments.
//!
//! Goals:
//! - Deduplicate all commonly repeated route segments
//! - Single source of truth for scheme/realm/area/resource/operation tokens
//! - Fast concurrent non-blocking reads via DashMap
//! - Arc<str> storage avoids per-segment allocations
//! - u32 IDs ensure tiny, cache-friendly route structs
//!
//! This is safe to use from all domains simultaneously.
//!
//! # Example
//!
//! ```rust
//! use fitz::routing::GlobalInternTable;
//! use std::sync::Arc;
//!
//! let table = Arc::new(GlobalInternTable::new());
//!
//! // Intern common route segments
//! let ftz_id = table.intern("ftz");
//! let realm_id = table.intern("realm123");
//! let kv_id = table.intern("kv");
//!
//! // Repeated calls return the same ID (no allocation)
//! assert_eq!(table.intern("ftz"), ftz_id);
//! assert_eq!(table.intern("realm123"), realm_id);
//!
//! // Retrieve strings by ID
//! assert_eq!(table.get(ftz_id).unwrap().as_ref(), "ftz");
//!
//! // Safe to use across threads
//! let table_clone = Arc::clone(&table);
//! std::thread::spawn(move || {
//!     let id = table_clone.intern("ftz");
//!     assert_eq!(id, ftz_id); // Same ID across threads
//! });
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use dashmap::DashMap;

/// Type of interned token IDs.
///
/// NOTE: u32 gives you 4 billion unique segments.
/// If you ever reach this, the world has ended.
pub type InternId = u32;

#[derive(Debug)]
pub struct GlobalInternTable {
    /// Maps string → id
    map: DashMap<Arc<str>, InternId>,

    /// Reverse-lookup ID → string (index = ID)
    ///
    /// Stored inside an RwLock so we can push to the vec as IDs are created.
    reverse: parking_lot::RwLock<Vec<Arc<str>>>,

    /// Next ID to allocate
    next_id: AtomicU32,
}

impl GlobalInternTable {
    /// Create a new empty table
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            reverse: parking_lot::RwLock::new(Vec::new()),
            next_id: AtomicU32::new(0),
        }
    }

    /// Intern a &str, returning its stable u32 ID.
    ///
    /// Fast path:
    ///     - DashMap lookup finds existing ID (no alloc)
    ///
    /// Slow path:
    ///     - Allocate Arc<str>
    ///     - Use DashMap::entry API to atomically insert or get existing
    ///     - On successful insert, push into reverse table
    ///     - Return ID (either new or existing)
    #[inline]
    pub fn intern(&self, s: &str) -> InternId {
        // Fast path: check map without allocating
        if let Some(id) = self.map.get(s) {
            return *id;
        }

        // Slow path: allocate Arc<str>
        let arc_str: Arc<str> = Arc::from(s);

        // Use entry API for atomic insert-or-get
        let entry = self.map.entry(arc_str.clone());
        
        let id = entry.or_insert_with(|| {
            // Only allocate ID if we're the first to insert this string
            let new_id = self.next_id.fetch_add(1, Ordering::Relaxed);
            
            // Update reverse table (id → string)
            {
                let mut reverse = self.reverse.write();
                if reverse.len() <= new_id as usize {
                    reverse.resize(new_id as usize + 1, Arc::from(""));
                }
                reverse[new_id as usize] = arc_str;
            }
            
            new_id
        });

        *id
    }

    /// Get interned string by ID.
    #[inline]
    pub fn get(&self, id: InternId) -> Option<Arc<str>> {
        let reverse = self.reverse.read();
        reverse.get(id as usize).cloned()
    }

    /// Return the number of unique interned segments.
    #[inline]
    pub fn len(&self) -> usize {
        self.next_id.load(Ordering::Relaxed) as usize
    }

    /// Returns true if no strings have been interned.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for GlobalInternTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn should_intern_string_and_return_id() {
        // Arrange
        let table = GlobalInternTable::new();

        // Act
        let id = table.intern("foo");

        // Assert
        assert_eq!(id, 0);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn should_return_same_id_for_duplicate_string() {
        // Arrange
        let table = GlobalInternTable::new();

        // Act
        let id1 = table.intern("bar");
        let id2 = table.intern("bar");

        // Assert
        assert_eq!(id1, id2);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn should_assign_different_ids_for_different_strings() {
        // Arrange
        let table = GlobalInternTable::new();

        // Act
        let id1 = table.intern("one");
        let id2 = table.intern("two");
        let id3 = table.intern("three");

        // Assert
        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn should_retrieve_string_by_id() {
        // Arrange
        let table = GlobalInternTable::new();
        let id = table.intern("hello");

        // Act
        let result = table.get(id);

        // Assert
        assert_eq!(result.unwrap().as_ref(), "hello");
    }

    #[test]
    fn should_return_none_for_invalid_id() {
        // Arrange
        let table = GlobalInternTable::new();

        // Act
        let result = table.get(999);

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_handle_concurrent_interning_correctly() {
        // Arrange
        let table = Arc::new(GlobalInternTable::new());
        let num_threads = 8;
        let inserts_per_thread = 1000;

        // Act
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let table_clone = Arc::clone(&table);
                thread::spawn(move || {
                    for i in 0..inserts_per_thread {
                        // Each thread interns both shared and unique strings
                        let shared = format!("shared_{}", i % 100);
                        let unique = format!("thread_{}_val_{}", thread_id, i);
                        
                        table_clone.intern(&shared);
                        table_clone.intern(&unique);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        // Should have 100 shared strings + (num_threads * inserts_per_thread) unique strings
        let expected_shared = 100;
        let expected_unique = num_threads * inserts_per_thread;
        assert_eq!(table.len(), expected_shared + expected_unique);
    }

    #[test]
    fn should_deduplicate_common_route_segments() {
        // Arrange
        let table = GlobalInternTable::new();

        // Act
        let ftz1 = table.intern("ftz");
        let ftz2 = table.intern("ftz");
        let realm1 = table.intern("realm123");
        let realm2 = table.intern("realm123");
        let kv1 = table.intern("kv");
        let kv2 = table.intern("kv");

        // Assert
        assert_eq!(ftz1, ftz2);
        assert_eq!(realm1, realm2);
        assert_eq!(kv1, kv2);
        assert_eq!(table.len(), 3); // Only 3 unique strings
    }

    #[test]
    fn should_support_empty_strings() {
        // Arrange
        let table = GlobalInternTable::new();

        // Act
        let id = table.intern("");

        // Assert
        assert_eq!(id, 0);
        assert_eq!(table.get(id).unwrap().as_ref(), "");
    }

    #[test]
    fn should_handle_unicode_strings() {
        // Arrange
        let table = GlobalInternTable::new();

        // Act
        let id = table.intern("🚀路由");

        // Assert
        assert_eq!(table.get(id).unwrap().as_ref(), "🚀路由");
    }

    #[test]
    fn should_report_empty_state_correctly() {
        // Arrange
        let table = GlobalInternTable::new();

        // Assert
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);

        // Act
        table.intern("test");

        // Assert
        assert!(!table.is_empty());
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn should_handle_race_condition_during_insertion() {
        // Arrange
        let table = Arc::new(GlobalInternTable::new());
        let num_threads = 16;

        // Act - All threads try to intern the same string simultaneously
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let table_clone = Arc::clone(&table);
                thread::spawn(move || {
                    table_clone.intern("race_me")
                })
            })
            .collect();

        let ids: Vec<InternId> = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        // Assert
        // All threads should get the same ID
        assert!(ids.iter().all(|&id| id == ids[0]));
        assert_eq!(table.len(), 1); // Only one entry should exist
    }
}
