pub mod chunk_disk;
pub mod consistency;
pub mod exemplars;
pub mod histogram;
pub mod kafka;
pub mod limits;
pub mod metrics;
pub mod ooo_merge;
pub mod protection;
pub mod proto;
pub mod record;
pub mod ring_client;
pub mod runtime_config;
pub mod segment;
pub mod service;
pub mod store;
pub mod trackers;
pub mod wire;
pub mod xor;

// Counts live heap bytes in tests, so memory tests measure the store's structures exactly.
#[cfg(test)]
pub(crate) mod test_allocator {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Counting;

    static LIVE: AtomicUsize = AtomicUsize::new(0);

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            LIVE.fetch_add(new_size, Ordering::Relaxed);
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: Counting = Counting;

    /// Heap bytes currently allocated by the whole test process.
    pub(crate) fn live_bytes() -> usize {
        LIVE.load(Ordering::Relaxed)
    }
}
