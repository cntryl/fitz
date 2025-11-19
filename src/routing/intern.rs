//! GlobalInternTable
//!
//! High-performance global string interner for Fitz route segments.
//!
//! - Arc<str> storage
//! - u32 IDs for compact route structs
//! - Concurrent, lock-free reads via DashMap
//! - Append-only reverse table (id → Arc<str>)

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

/// Interned ID – 4 bytes, cache-friendly.
pub type InternId = u32;

#[derive(Debug)]
pub struct GlobalInternTable {
    /// string → id
    map: DashMap<Arc<str>, InternId>,
    /// id → string (index = id)
    reverse: RwLock<Vec<Arc<str>>>,
    /// Next ID to allocate
    next_id: AtomicU32,
}

impl GlobalInternTable {
    /// Create a new empty table.
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            reverse: RwLock::new(Vec::new()),
            next_id: AtomicU32::new(0),
        }
    }

    /// Intern a string and return its stable u32 ID.
    ///
    /// Fast path:
    /// - `DashMap::get` using `&str` (no allocation) – works because `Arc<str>: Borrow<str>`.
    ///
    /// Slow path:
    /// - Allocate `Arc<str>`
    /// - Insert via `entry` and push to `reverse` on first insert.
    #[inline]
    pub fn intern(&self, s: &str) -> InternId {
        // Fast path: no alloc if already present
        if let Some(id_ref) = self.map.get(s) {
            return *id_ref;
        }

        // Slow path: allocate once
        let arc_str: Arc<str> = Arc::from(s);
        let arc_for_reverse = arc_str.clone();

        // Entry API: atomically insert or get existing
        let entry = self.map.entry(arc_str);
        let id_ref = entry.or_insert_with(|| {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);

            // Append to reverse table (id → Arc<str>)
            let mut rev = self.reverse.write();
            rev.push(arc_for_reverse);

            id
        });

        *id_ref
    }

    /// Get interned string by ID.
    #[inline]
    pub fn get(&self, id: InternId) -> Option<Arc<str>> {
        let rev = self.reverse.read();
        rev.get(id as usize).cloned()
    }

    /// Number of unique interned strings.
    #[inline]
    pub fn len(&self) -> usize {
        self.next_id.load(Ordering::Relaxed) as usize
    }

    /// True if no strings have been interned.
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
    use std::sync::Arc as StdArc;
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
        let table = StdArc::new(GlobalInternTable::new());
        let num_threads = 8;
        let inserts_per_thread = 1000;

        // Act
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let table_clone = StdArc::clone(&table);
                thread::spawn(move || {
                    for i in 0..inserts_per_thread {
                        let shared = format!("shared_{}", i % 100);
                        let unique = format!("thread_{}_val_{}", thread_id, i);

                        table_clone.intern(&shared);
                        table_clone.intern(&unique);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Assert
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
        assert_eq!(table.len(), 3);
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

        // Act
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
        table.intern("test");

        // Assert
        assert!(!table.is_empty());
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn should_handle_race_condition_during_insertion() {
        // Arrange
        let table = StdArc::new(GlobalInternTable::new());
        let num_threads = 16;

        // Act
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let t = StdArc::clone(&table);
                thread::spawn(move || t.intern("race_me"))
            })
            .collect();

        let ids: Vec<InternId> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Assert
        assert!(ids.iter().all(|&id| id == ids[0]));
        assert_eq!(table.len(), 1);
    }
}
