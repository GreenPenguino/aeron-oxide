use std::{cell::UnsafeCell, mem::size_of, ptr::NonNull, sync::atomic::AtomicI64};

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

pub(crate) struct Descriptor(NonNull<RawDescriptor>);

impl Descriptor {
    pub fn new(ptr: *mut u8) -> Self {
        let raw: &RawDescriptor = unsafe { &*(ptr as *mut RawDescriptor) };
        raw.tail_position
            .store(0, std::sync::atomic::Ordering::Relaxed);
        raw.head_cache_position
            .store(0, std::sync::atomic::Ordering::Relaxed);
        raw.head_position
            .store(0, std::sync::atomic::Ordering::Relaxed);
        raw.correlation_counter
            .store(0, std::sync::atomic::Ordering::Relaxed);
        raw.consumer_heartbeat
            .store(0, std::sync::atomic::Ordering::Release);
        Self(NonNull::new(ptr as *mut RawDescriptor).unwrap())
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
