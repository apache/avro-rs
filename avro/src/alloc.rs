// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Optional custom allocator support.
//!
//! This module is only available when the `custom_allocator` feature is enabled.
//! It is **opt-in**: nothing here changes how `apache-avro` allocates unless a
//! downstream program explicitly installs one of these types as its
//! `#[global_allocator]`.
//!
//! It provides two custom [`GlobalAlloc`] implementations:
//!
//! 1. [`CustomAllocator`] — a minimal allocator that delegates every operation
//!    to the [`System`] allocator. It is a ready-made base you can install
//!    directly, or extend, to add your own allocation behavior without
//!    reimplementing the platform allocation logic.
//! 2. [`CountingAllocator`] — a [`GlobalAlloc`] adapter that wraps any other
//!    allocator (the [`System`] allocator by default) and keeps live, peak and
//!    cumulative allocation statistics. Installing it as the process
//!    `#[global_allocator]` lets you observe exactly how much memory
//!    `apache-avro` (and the rest of your program) allocates while reading or
//!    writing data.
//!
//! [`CountingAllocator`] complements the per-length decoding guard configured
//! through [`crate::util::max_allocation_bytes`]. That guard rejects any
//! *single* oversized length prefix up front, whereas [`CountingAllocator`]
//! measures the *actual* live memory of the whole process, so it can also
//! surface pressure that builds up across many individually-small allocations.
//!
//! # Examples
//!
//! Install the plain [`CustomAllocator`] as the global allocator:
//!
//! ```no_run
//! use apache_avro::alloc::CustomAllocator;
//!
//! #[global_allocator]
//! static GLOBAL: CustomAllocator = CustomAllocator;
//! ```
//!
//! Install [`CountingAllocator`] instead and read the counters:
//!
//! ```no_run
//! use apache_avro::alloc::CountingAllocator;
//!
//! #[global_allocator]
//! static GLOBAL: CountingAllocator = CountingAllocator::system();
//!
//! fn main() {
//!     // ... run some apache-avro encoding/decoding ...
//!     println!("live bytes: {}", GLOBAL.allocated());
//!     println!("peak bytes: {}", GLOBAL.peak_allocated());
//! }
//! ```

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};

/// A minimal custom [`GlobalAlloc`] that delegates every operation to the
/// [`System`] allocator.
///
/// This type can be installed directly as a `#[global_allocator]`, or used as a
/// base to build a more elaborate custom allocator (for example one that adds
/// logging or pooling) without having to reimplement the platform allocation
/// logic. It is behaviorally identical to using the default system allocator.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CustomAllocator;

// SAFETY: Every method simply forwards to `System`, which is a correct
// `GlobalAlloc` implementation, preserving all of its invariants.
unsafe impl GlobalAlloc for CustomAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Upheld by the caller of the `GlobalAlloc` method.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Upheld by the caller of the `GlobalAlloc` method.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Upheld by the caller of the `GlobalAlloc` method.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: Upheld by the caller of the `GlobalAlloc` method.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// A [`GlobalAlloc`] adapter that forwards every request to an inner allocator
/// while tracking live, peak and cumulative allocation statistics.
///
/// The inner allocator defaults to [`System`]. Any type implementing
/// [`GlobalAlloc`] can be wrapped instead (for example a third-party allocator),
/// making the counters composable with other allocation strategies.
///
/// All counters are maintained with relaxed atomic operations, so reading them
/// from another thread yields a recent—though not necessarily perfectly
/// synchronized—value. This is intended for observability and profiling rather
/// than as a hard real-time accounting mechanism.
#[derive(Debug)]
pub struct CountingAllocator<A = System> {
    inner: A,
    /// Bytes currently allocated (allocations minus deallocations).
    live: AtomicUsize,
    /// Highest value [`Self::live`] has ever reached.
    peak: AtomicUsize,
    /// Total bytes ever handed out (never decreases).
    total: AtomicUsize,
    /// Number of allocation (and reallocation) calls served.
    allocations: AtomicUsize,
    /// Number of deallocation calls served.
    deallocations: AtomicUsize,
}

impl CountingAllocator<System> {
    /// Create a [`CountingAllocator`] backed by the [`System`] allocator.
    ///
    /// This is a `const fn` so it can be used to initialize a
    /// `#[global_allocator]` static.
    #[must_use]
    pub const fn system() -> Self {
        Self::new(System)
    }
}

impl<A> CountingAllocator<A> {
    /// Create a [`CountingAllocator`] that wraps the given inner allocator.
    #[must_use]
    pub const fn new(inner: A) -> Self {
        Self {
            inner,
            live: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
            deallocations: AtomicUsize::new(0),
        }
    }

    /// Bytes currently allocated but not yet freed.
    #[must_use]
    pub fn allocated(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    /// The maximum value [`Self::allocated`] has reached since construction or
    /// the last call to [`Self::reset_peak`].
    #[must_use]
    pub fn peak_allocated(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Total number of bytes ever allocated. This value never decreases.
    #[must_use]
    pub fn total_allocated(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    /// Number of allocation (and growing reallocation) calls served.
    #[must_use]
    pub fn allocation_count(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }

    /// Number of deallocation calls served.
    #[must_use]
    pub fn deallocation_count(&self) -> usize {
        self.deallocations.load(Ordering::Relaxed)
    }

    /// Reset the recorded peak to the current live value.
    ///
    /// Useful to measure the peak of a specific section of code without the
    /// influence of earlier allocations.
    pub fn reset_peak(&self) {
        let live = self.live.load(Ordering::Relaxed);
        self.peak.store(live, Ordering::Relaxed);
    }

    /// Record that `size` bytes were allocated and update the peak.
    fn on_alloc(&self, size: usize) {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(size, Ordering::Relaxed);
        let live = self.live.fetch_add(size, Ordering::Relaxed) + size;
        self.peak.fetch_max(live, Ordering::Relaxed);
    }

    /// Record that `size` bytes were freed.
    fn on_dealloc(&self, size: usize) {
        self.deallocations.fetch_add(1, Ordering::Relaxed);
        self.live.fetch_sub(size, Ordering::Relaxed);
    }

    /// Record a successful reallocation from `old_size` to `new_size` bytes.
    fn on_realloc(&self, old_size: usize, new_size: usize) {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        if new_size >= old_size {
            let grew_by = new_size - old_size;
            self.total.fetch_add(grew_by, Ordering::Relaxed);
            let live = self.live.fetch_add(grew_by, Ordering::Relaxed) + grew_by;
            self.peak.fetch_max(live, Ordering::Relaxed);
        } else {
            self.live.fetch_sub(old_size - new_size, Ordering::Relaxed);
        }
    }
}

impl<A: Default> Default for CountingAllocator<A> {
    fn default() -> Self {
        Self::new(A::default())
    }
}

// SAFETY: Every method forwards to `inner`, which is a correct `GlobalAlloc`
// implementation, and only additionally updates atomic counters. The pointer
// returned to (and passed back by) the caller is exactly the one produced by
// `inner`, so all `GlobalAlloc` invariants are preserved.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Upheld by the caller of the `GlobalAlloc` method.
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            self.on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Upheld by the caller of the `GlobalAlloc` method.
        unsafe { self.inner.dealloc(ptr, layout) };
        self.on_dealloc(layout.size());
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Upheld by the caller of the `GlobalAlloc` method.
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        if !ptr.is_null() {
            self.on_alloc(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: Upheld by the caller of the `GlobalAlloc` method.
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            self.on_realloc(layout.size(), new_size);
        }
        new_ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_custom_allocator_delegates_to_system() {
        let allocator = CustomAllocator;
        let layout = Layout::from_size_align(48, 8).unwrap();

        // SAFETY: The block is allocated and freed through the same allocator
        // with the same layout.
        unsafe {
            let ptr = allocator.alloc(layout);
            assert!(!ptr.is_null());
            ptr.write_bytes(0xAB, layout.size());
            allocator.dealloc(ptr, layout);
        }
    }

    #[test]
    fn test_alloc_and_dealloc_update_counters() {
        let allocator = CountingAllocator::system();
        let layout = Layout::from_size_align(1024, 8).unwrap();

        // SAFETY: `layout` has a non-zero size; the block is freed below with the
        // same layout.
        unsafe {
            let ptr = allocator.alloc(layout);
            assert!(!ptr.is_null());
            assert_eq!(allocator.allocated(), 1024);
            assert_eq!(allocator.total_allocated(), 1024);
            assert_eq!(allocator.peak_allocated(), 1024);
            assert_eq!(allocator.allocation_count(), 1);
            assert_eq!(allocator.deallocation_count(), 0);

            allocator.dealloc(ptr, layout);
            assert_eq!(allocator.allocated(), 0);
            // Peak and total are cumulative and do not shrink on dealloc.
            assert_eq!(allocator.peak_allocated(), 1024);
            assert_eq!(allocator.total_allocated(), 1024);
            assert_eq!(allocator.deallocation_count(), 1);
        }
    }

    #[test]
    fn test_peak_tracks_high_water_mark() {
        let allocator = CountingAllocator::system();
        let small = Layout::from_size_align(64, 8).unwrap();
        let big = Layout::from_size_align(4096, 8).unwrap();

        // SAFETY: Each block is freed with the same layout used to allocate it.
        unsafe {
            let a = allocator.alloc(big);
            assert!(!a.is_null());
            allocator.dealloc(a, big);

            let b = allocator.alloc(small);
            assert!(!b.is_null());

            // Live is back down to the small block, but peak remembers the big one.
            assert_eq!(allocator.allocated(), 64);
            assert_eq!(allocator.peak_allocated(), 4096);

            allocator.dealloc(b, small);
        }
    }

    #[test]
    fn test_reset_peak() {
        let allocator = CountingAllocator::system();
        let layout = Layout::from_size_align(256, 8).unwrap();

        // SAFETY: The block is freed with the same layout used to allocate it.
        unsafe {
            let ptr = allocator.alloc(layout);
            assert!(!ptr.is_null());
            allocator.dealloc(ptr, layout);
        }

        assert_eq!(allocator.peak_allocated(), 256);
        allocator.reset_peak();
        // After reset the peak equals the current live value (0 here).
        assert_eq!(allocator.peak_allocated(), 0);
    }

    #[test]
    fn test_realloc_growth_and_shrink() {
        let allocator = CountingAllocator::system();
        let layout = Layout::from_size_align(32, 8).unwrap();

        // SAFETY: `ptr` is grown then shrunk then freed, always with the layout
        // that matches its current size.
        unsafe {
            let ptr = allocator.alloc(layout);
            assert!(!ptr.is_null());
            assert_eq!(allocator.allocated(), 32);

            let grown = allocator.realloc(ptr, layout, 128);
            assert!(!grown.is_null());
            assert_eq!(allocator.allocated(), 128);
            assert_eq!(allocator.peak_allocated(), 128);

            let grown_layout = Layout::from_size_align(128, 8).unwrap();
            let shrunk = allocator.realloc(grown, grown_layout, 16);
            assert!(!shrunk.is_null());
            assert_eq!(allocator.allocated(), 16);
            // Peak still reflects the largest live size seen.
            assert_eq!(allocator.peak_allocated(), 128);

            let shrunk_layout = Layout::from_size_align(16, 8).unwrap();
            allocator.dealloc(shrunk, shrunk_layout);
            assert_eq!(allocator.allocated(), 0);
        }
    }
}
