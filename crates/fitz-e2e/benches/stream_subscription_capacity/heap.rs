//! Measure still-live Rust allocations, including the in-process broker.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static LIVE: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

struct TrackingAllocator;

// SAFETY: Forward the same pointer and layout to the system allocator. The
// counter is independent of allocator correctness.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds the GlobalAlloc contract.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        // SAFETY: The caller upholds the GlobalAlloc contract.
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, new: usize) -> *mut u8 {
        // SAFETY: The caller upholds the GlobalAlloc contract.
        let ptr = unsafe { System.realloc(ptr, old, new) };
        if !ptr.is_null() {
            LIVE.fetch_add(new, Ordering::Relaxed);
            LIVE.fetch_sub(old.size(), Ordering::Relaxed);
        }
        ptr
    }
}

pub(super) fn live_bytes() -> usize {
    LIVE.load(Ordering::Relaxed)
}
