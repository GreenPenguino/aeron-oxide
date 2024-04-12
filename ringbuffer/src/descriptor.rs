#![allow(deprecated)]

//! Descriptor.
//!
//! The descriptor contains the following fields:
//! - `tail_position`: end of data, incremented on writes, only ever increasing
//! - `head_cache_position`
//! - `head_position`: start of data, incremented on reads, only ever increasing
//!
//!
//!
//!
use std::{
    cell::UnsafeCell,
    mem::size_of,
    ptr::NonNull,
    sync::atomic::{AtomicI64, Ordering},
};

use crate::AERON_CACHE_LINE_LENGTH;

#[derive(Debug)]
#[repr(C, align(4))]
pub(crate) struct RawDescriptor {
    _begin_pad: [UnsafeCell<u8>; 2 * AERON_CACHE_LINE_LENGTH],
    pub tail_position: AtomicI64,
    // pub tail_position: i64,
    _tail_pad: [UnsafeCell<u8>; 2 * AERON_CACHE_LINE_LENGTH - size_of::<AtomicI64>()],
    pub head_cache_position: AtomicI64,
    // pub head_cache_position: i64,
    _head_cache_pad: [UnsafeCell<u8>; 2 * AERON_CACHE_LINE_LENGTH - size_of::<AtomicI64>()],
    pub head_position: AtomicI64,
    // pub head_position: i64,
    _head_pad: [UnsafeCell<u8>; 2 * AERON_CACHE_LINE_LENGTH - size_of::<AtomicI64>()],
    pub correlation_counter: AtomicI64,
    // pub correlation_counter: i64,
    _correlation_counter_pad:
        [UnsafeCell<u8>; 2 * AERON_CACHE_LINE_LENGTH - size_of::<AtomicI64>()],
    pub consumer_heartbeat: AtomicI64,
    // pub consumer_heartbeat: i64,
    _consumer_heartbeat_pad: [UnsafeCell<u8>; 2 * AERON_CACHE_LINE_LENGTH - size_of::<AtomicI64>()],
}

#[derive(Clone, Copy)]
pub struct Descriptor(NonNull<RawDescriptor>);

impl Descriptor {
    pub fn new(ptr: *mut u8) -> Self {
        Self(NonNull::new(ptr as *mut RawDescriptor).unwrap())
    }

    pub fn reset(&self) {
        self.tail_position()
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.head_cache_position()
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.head_position()
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.correlation_counter()
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.consumer_heartbeat()
            .store(0, std::sync::atomic::Ordering::Release);
    }

    pub fn tail_position(&self) -> &AtomicI64 {
        &self.raw_descriptor().tail_position
    }
    pub fn head_cache_position(&self) -> &AtomicI64 {
        &self.raw_descriptor().head_cache_position
    }
    pub fn head_position(&self) -> &AtomicI64 {
        &self.raw_descriptor().head_position
    }
    pub fn correlation_counter(&self) -> &AtomicI64 {
        &self.raw_descriptor().correlation_counter
    }
    pub fn consumer_heartbeat(&self) -> &AtomicI64 {
        &self.raw_descriptor().consumer_heartbeat
    }

    fn raw_descriptor(&self) -> &RawDescriptor {
        unsafe { self.0.as_ref() }
    }
}

impl std::fmt::Debug for Descriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

pub(crate) struct SenderDescriptor {
    pub tail_position: ReadWriteTail,
    pub head_cache_position: ReadWriteHeadCache,
    pub head_position: ReadOnlyHead,
}

impl From<Descriptor> for SenderDescriptor {
    fn from(descriptor: Descriptor) -> Self {
        Self {
            tail_position: ReadWriteTail(descriptor.tail_position()),
            head_cache_position: ReadWriteHeadCache(descriptor.head_cache_position()),
            head_position: ReadOnlyHead(descriptor.head_position()),
        }
    }
}

pub(crate) struct ReceiverDescriptor {
    pub tail_position: ReadOnlyTail,
    pub head_cache_position: ReadOnlyHeadCache,
    pub head_position: ReadWriteHead,
}

impl From<Descriptor> for ReceiverDescriptor {
    fn from(descriptor: Descriptor) -> Self {
        Self {
            tail_position: ReadOnlyTail(descriptor.tail_position()),
            head_cache_position: ReadOnlyHeadCache(descriptor.head_cache_position()),
            head_position: ReadWriteHead(descriptor.head_position()),
        }
    }
}

pub type Head = i64;
pub type AtomicHead = AtomicI64;
pub struct ReadWriteHead(*const AtomicHead);
pub struct ReadOnlyHead(*const AtomicHead);

pub type HeadCache = i64;
pub type AtomicHeadCache = AtomicI64;
pub struct ReadWriteHeadCache(*const AtomicHeadCache);
pub struct ReadOnlyHeadCache(*const AtomicHeadCache);

pub type Tail = i64;
pub type AtomicTail = AtomicI64;
pub struct ReadWriteTail(*const AtomicTail);
pub struct ReadOnlyTail(*const AtomicTail);

impl ReadWriteHead {
    pub fn new(ptr: *const AtomicHead) -> Self {
        Self(ptr)
    }

    // IDEA: does #[inline(always)] make a difference in benchmarks?

    #[cfg(not(debug_assertions))]
    pub fn store_atomic(&self, val: Head, ord: Ordering) {
        let atomic = unsafe {
            let atomic = &*self.0;
        };

        atomic.store(val, ord);
    }

    #[cfg(debug_assertions)]
    pub fn store_atomic(&self, val: Head, ord: Ordering) {
        let atomic = unsafe { &*self.0 };

        let old = atomic.swap(val, ord);
        debug_assert!(val >= old);
    }

    // IDEA: does #[inline(always)] make a difference in benchmarks?
    pub fn load_atomic(&self, ord: Ordering) -> Head {
        let atomic = unsafe { &*self.0 };

        atomic.load(ord)
    }
}

impl ReadOnlyHead {
    pub fn new(ptr: *const AtomicHead) -> Self {
        Self(ptr)
    }

    // IDEA: does #[inline(always)] make a difference in benchmarks?
    pub fn load_atomic(&self, ord: Ordering) -> Head {
        let atomic = unsafe { &*self.0 };

        atomic.load(ord)
    }
}

impl ReadWriteHeadCache {
    pub fn new(ptr: *const AtomicHead) -> Self {
        Self(ptr)
    }

    // IDEA: does #[inline(always)] make a difference in benchmarks?
    #[cfg(not(debug_assertions))]
    pub fn store_atomic(&self, val: Head, ord: Ordering) {
        let atomic = unsafe { &*self.0 };

        atomic.store(val, ord);
    }

    #[cfg(debug_assertions)]
    pub fn store_atomic(&self, val: Head, ord: Ordering) {
        let atomic = unsafe { &*self.0 };

        let old = atomic.swap(val, ord);
        debug_assert!(val >= old);
    }

    // IDEA: does #[inline(always)] make a difference in benchmarks?
    pub fn load_atomic(&self, ord: Ordering) -> HeadCache {
        let atomic = unsafe { &*self.0 };

        atomic.load(ord)
    }
}

impl ReadOnlyHeadCache {
    pub fn new(ptr: *const AtomicHeadCache) -> Self {
        Self(ptr)
    }

    // IDEA: does #[inline(always)] make a difference in benchmarks?
    pub fn load_atomic(&self, ord: Ordering) -> HeadCache {
        let atomic = unsafe { &*self.0 };

        atomic.load(ord)
    }
}

impl ReadWriteTail {
    pub fn new(ptr: *const AtomicHead) -> Self {
        Self(ptr)
    }

    // IDEA: does #[inline(always)] make a difference in benchmarks?
    #[cfg(not(debug_assertions))]
    pub fn store_atomic(&self, val: Head, ord: Ordering) {
        let atomic = unsafe { &*self.0 };

        atomic.store(val, ord);
    }

    #[cfg(debug_assertions)]
    pub fn store_atomic(&self, val: Head, ord: Ordering) {
        let atomic = unsafe { &*self.0 };

        let old = atomic.swap(val, ord);
        debug_assert!(val >= old);
    }

    // IDEA: does #[inline(always)] make a difference in benchmarks?
    pub fn load_atomic(&self, ord: Ordering) -> Tail {
        let atomic = unsafe { &*self.0 };

        atomic.load(ord)
    }

    pub fn cas(&self, expected: i64, desired: i64, ord: Ordering) -> bool {
        let atomic = unsafe { &*self.0 };

        let original = atomic.compare_and_swap(expected, desired, ord);
        original == expected
    }
}

impl ReadOnlyTail {
    pub fn new(ptr: *const AtomicTail) -> Self {
        Self(ptr)
    }

    // IDEA: does #[inline(always)] make a difference in benchmarks?
    pub fn read_atomic(&self, ord: Ordering) -> Tail {
        let atomic = unsafe { &*self.0 };

        atomic.load(ord)
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::alloc;
    use std::alloc::Layout;
    use std::cell::UnsafeCell;
    use std::mem::MaybeUninit;
    use std::mem::{align_of, offset_of, size_of};

    use crate::descriptor::RawDescriptor;
    // use crate::RingBufferData;
    use crate::AERON_CACHE_LINE_LENGTH;

    use super::Descriptor;

    #[test]
    fn ringbuffer_descriptor_layout_alignment() {
        assert_eq!(size_of::<RawDescriptor>(), 6 * 2 * AERON_CACHE_LINE_LENGTH);
        assert_eq!(align_of::<RawDescriptor>(), 8);

        assert_eq!(
            offset_of!(RawDescriptor, tail_position),
            1 * 2 * AERON_CACHE_LINE_LENGTH
        );
        assert_eq!(
            offset_of!(RawDescriptor, head_cache_position),
            2 * 2 * AERON_CACHE_LINE_LENGTH
        );
        assert_eq!(
            offset_of!(RawDescriptor, head_position),
            3 * 2 * AERON_CACHE_LINE_LENGTH
        );
        assert_eq!(
            offset_of!(RawDescriptor, correlation_counter),
            4 * 2 * AERON_CACHE_LINE_LENGTH
        );
        assert_eq!(
            offset_of!(RawDescriptor, consumer_heartbeat),
            5 * 2 * AERON_CACHE_LINE_LENGTH
        );
    }

    // #[test]
    // fn ringbuffer_data_layout_alignment() {
    //     assert_eq!(
    //         size_of::<RingBufferData<1024>>(),
    //         size_of::<[UnsafeCell<u8>; 1024]>() + size_of::<UnsafeCell<RawDescriptor>>()
    //     );

    //     assert_eq!(offset_of!(RingBufferData<1024>, buffer), 0);
    //     assert_eq!(offset_of!(RingBufferData<1024>, descriptor), 1024);
    // }
}
