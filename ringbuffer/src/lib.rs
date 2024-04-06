#![allow(dead_code, unused_variables)]

use core::slice;
use std::{
    cell::UnsafeCell,
    mem::size_of,
    ptr,
    sync::atomic::{compiler_fence, AtomicI32, AtomicI64, Ordering},
};

// TODO: move somewhere else (aeron-client/src/main/c/util/aeron_binutil.h)
const AERON_CACHE_LINE_LENGTH: usize = 64;

const AERON_RB_TRAILER_LENGTH: usize = size_of::<Descriptor>();
const AERON_RB_RECORD_HEADER_LENGTH: usize = size_of::<RecordDescriptor>();
const AERON_MPSC_RB_MIN_CAPACITY: usize = AERON_RB_RECORD_HEADER_LENGTH;
const AERON_RB_ALIGNMENT: usize = 2 * size_of::<i32>();
const AERON_RB_PADDING_MSG_TYPE_ID: i32 = -1;

// TODO: check if buffer can be a slice instead.
pub struct RingBuffer<'buffer> {
    buffer: &'buffer [UnsafeCell<u8>],
    descriptor: &'buffer Descriptor,
    capacity: usize,
    max_message_length: usize,
}

impl<'rb> RingBuffer<'rb> {
    pub unsafe fn new(buffer: *const u8, length: usize) -> Result<Self, ()> {
        let capacity: usize = length - AERON_RB_TRAILER_LENGTH;

        if is_capacity_valid(capacity, AERON_MPSC_RB_MIN_CAPACITY) {
            let buffer: &[UnsafeCell<u8>] =
                slice::from_raw_parts(buffer as *const UnsafeCell<u8>, length);
            let (buffer, descriptor_bytes) = buffer.split_at(capacity);

            Ok(Self {
                buffer,
                capacity,
                descriptor: unsafe { &*(descriptor_bytes[0].get() as *mut Descriptor) },
                max_message_length: aeron_rb_max_message_length(
                    capacity,
                    AERON_MPSC_RB_MIN_CAPACITY,
                ),
            })
        } else {
            Err(()) // TODO: create actual error type with "Invalid capacity: {capacity}" message
        }
    }

    pub fn write(&mut self, msg_type_id: i32, msg: &[u8]) -> Result<(), ()> {
        if msg.len() > self.max_message_length || aeron_rb_invalid_msg_type_id(msg_type_id) {
            return Err(());
        }

        let record_length: usize = msg.len() + AERON_RB_RECORD_HEADER_LENGTH;
        let record_index = self.claim_capacity(record_length)?;

        let record_header: *mut RecordDescriptor =
            self.buffer[record_index as usize].get() as *mut RecordDescriptor;

        aeron_put_ordered_i32(&unsafe { &*record_header }.length, -(record_length as i32)); // Probably wrong, should likely be an atomic (lets see if loom caches this).

        let index = aeron_rb_message_offset(record_index as usize);
        let destination_ptr: *mut u8 = self.buffer[index].get();
        unsafe { ptr::copy_nonoverlapping(msg.as_ptr(), destination_ptr, msg.len()) };

        unsafe { &mut *record_header }.msg_type_id = msg_type_id;
        aeron_put_ordered_i32(&unsafe { &*record_header }.length, record_length as i32);

        Ok(())
    }

    // simplified version of read without handler.
    // TODO: implement full version
    pub fn read(&mut self, message_count_limit: usize) -> Vec<(i32, Vec<u8>)> {
        let head: i64 = self.descriptor.head_position.load(Ordering::Relaxed);
        let head_index: usize = head as usize & (self.capacity - 1);
        let contiguous_block_length: usize = self.capacity - head_index;
        let mut messages_read: usize = 0;
        let mut bytes_read: usize = 0;

        // TODO: replace buffer by message handler
        let mut read_buffer: Vec<(i32, Vec<u8>)> = Vec::new();

        while bytes_read < contiguous_block_length && messages_read < message_count_limit {
            let record_index: usize = head_index + bytes_read;
            let header: &RecordDescriptor = {
                let ptr: *mut u8 = self.buffer[record_index as usize].get();
                unsafe { &*(ptr as *const RecordDescriptor) }
            };

            let mut record_length: i32 = 0;
            aeron_get_volatile_i32(&mut record_length, &header.length);

            if record_length <= 0 {
                break;
            }

            bytes_read += aeron_align(record_length as usize, AERON_RB_ALIGNMENT);
            let msg_type_id: i32 = header.msg_type_id;

            if msg_type_id == AERON_RB_PADDING_MSG_TYPE_ID {
                continue;
            }

            messages_read += 1;
            let data: &[u8] = {
                let ptr = self.buffer[aeron_rb_message_offset(record_index)].get();
                unsafe {
                    slice::from_raw_parts(
                        ptr,
                        record_length as usize - AERON_RB_RECORD_HEADER_LENGTH,
                    )
                }
            };
            read_buffer.push((msg_type_id, data.to_vec()));
        }

        if bytes_read != 0 {
            // Set all the bytes read to 0 with memset
            let destination_ptr: *mut u8 = self.buffer[head_index].get();
            unsafe { destination_ptr.write_bytes(0, bytes_read) };
            aeron_put_ordered_i64(&self.descriptor.head_position, head + bytes_read as i64);
        }

        // return vec
        read_buffer
    }

    // TODO: check if result can be changed to u32
    fn claim_capacity(&mut self, record_length: usize) -> Result<i32, ()> {
        let required_capacity: usize = aeron_align(record_length, AERON_RB_ALIGNMENT);
        let mask: usize = self.capacity - 1;
        let mut head: i64 = 0;
        let mut tail: i64 = 0;
        let mut tail_index: usize;
        let mut padding: usize;

        aeron_get_volatile_i64(&mut head, &self.descriptor.head_cache_position);

        loop {
            // DO STUFF
            aeron_get_volatile_i64(&mut tail, &self.descriptor.tail_position);

            let available_capacity = self.capacity - (tail as usize - head as usize);

            if required_capacity > available_capacity {
                aeron_get_volatile_i64(&mut head, &self.descriptor.head_position);

                if required_capacity > (self.capacity - (tail as usize - head as usize)) {
                    return Err(());
                }

                aeron_put_ordered_i64(&self.descriptor.head_cache_position, head);
            }

            padding = 0;
            tail_index = tail as usize & mask;
            let to_buffer_end_length = self.capacity - tail_index;

            if required_capacity > to_buffer_end_length {
                // The message doesn't fit between tail index and end of buffer.

                let mut head_index = head as usize & mask;

                if required_capacity > head_index {
                    // The message doesn't fit between start of buffer and **cached** head index.

                    aeron_get_volatile_i64(&mut head, &self.descriptor.head_position);
                    head_index = head as usize & mask;

                    if required_capacity > head_index {
                        // The message doesn't fit between start of buffer and head index.
                        return Err(());
                    }

                    aeron_put_ordered_i64(&self.descriptor.head_cache_position, head);
                }

                padding = to_buffer_end_length;
            }

            // Exit condition
            if aeron_cas_i64(
                &self.descriptor.tail_position,
                tail,
                tail + required_capacity as i64 + padding as i64,
            ) {
                break;
            }
        }

        if padding != 0 {
            let record_header: &mut RecordDescriptor = {
                let ptr: *mut u8 = self.buffer[tail_index].get();
                unsafe { &mut *(ptr as *mut RecordDescriptor) }
            };

            aeron_put_ordered_i32(&mut record_header.length, -(padding as i32));
            record_header.msg_type_id = AERON_RB_PADDING_MSG_TYPE_ID;
            aeron_put_ordered_i32(&record_header.length, padding as i32);
            tail_index = 0;
        }

        Ok(tail_index as i32)
    }
}

#[repr(C, align(4))]
struct Descriptor {
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

#[repr(C, align(4))]
struct RecordDescriptor {
    // Length should probably be an atomic
    length: AtomicI32,
    msg_type_id: i32,
}

// TODO: move somewhere else
// Most likely wrong, should use atomic
fn aeron_get_volatile_i32(dst: &mut i32, src: &AtomicI32) {
    *dst = src.load(Ordering::Relaxed);
    compiler_fence(std::sync::atomic::Ordering::SeqCst);
}

// TODO: move somewhere else
// Most likely wrong, should use atomic
fn aeron_get_volatile_i64(dst: &mut i64, src: &AtomicI64) {
    *dst = src.load(Ordering::Relaxed);
    compiler_fence(std::sync::atomic::Ordering::SeqCst);
}

// TODO: move somewhere else
// Most likely wrong, should use atomic
fn aeron_put_ordered_i64(dst: &AtomicI64, src: i64) {
    compiler_fence(std::sync::atomic::Ordering::SeqCst);
    dst.store(src, Ordering::Relaxed);
}

// TODO: move somewhere else
// Most likely wrong, should use atomic
fn aeron_put_ordered_i32(dst: &AtomicI32, src: i32) {
    compiler_fence(std::sync::atomic::Ordering::SeqCst);
    dst.store(src, Ordering::Relaxed);
}

fn aeron_cas_i64(dst: &AtomicI64, expected: i64, desired: i64) -> bool {
    let original = dst.compare_and_swap(expected, desired, Ordering::SeqCst);
    original == expected
}

// TODO: move somewhere else
fn is_capacity_valid(capacity: usize, min_capacity: usize) -> bool {
    capacity.is_power_of_two() && capacity >= min_capacity
}

const fn aeron_rb_max_message_length(capacity: usize, min_capacity: usize) -> usize {
    if capacity == min_capacity {
        0
    } else {
        capacity / 8
    }
}

const fn aeron_rb_invalid_msg_type_id(id: i32) -> bool {
    id < 1
}

fn aeron_rb_message_offset(index: usize) -> usize {
    index + size_of::<RecordDescriptor>()
}

fn aeron_align(value: usize, alignment: usize) -> usize {
    (value + (alignment - 1)) & !(alignment - 1)
}

#[cfg(test)]
mod tests {
    use std::mem::{align_of, offset_of, size_of};

    use crate::AERON_CACHE_LINE_LENGTH;

    use super::Descriptor;
    use super::RingBuffer;

    #[test]
    fn ringbuffer_descriptor_layout_alignment() {
        assert_eq!(size_of::<Descriptor>(), 6 * 2 * AERON_CACHE_LINE_LENGTH);
        assert_eq!(align_of::<Descriptor>(), 8);

        assert_eq!(
            offset_of!(Descriptor, tail_position),
            1 * 2 * AERON_CACHE_LINE_LENGTH
        );
        assert_eq!(
            offset_of!(Descriptor, head_cache_position),
            2 * 2 * AERON_CACHE_LINE_LENGTH
        );
        assert_eq!(
            offset_of!(Descriptor, head_position),
            3 * 2 * AERON_CACHE_LINE_LENGTH
        );
        assert_eq!(
            offset_of!(Descriptor, correlation_counter),
            4 * 2 * AERON_CACHE_LINE_LENGTH
        );
        assert_eq!(
            offset_of!(Descriptor, consumer_heartbeat),
            5 * 2 * AERON_CACHE_LINE_LENGTH
        );
    }

    #[test]
    fn read_write_read_single_message() {
        let buffer = [0u8; 1024 + size_of::<Descriptor>()];

        let mut reader = unsafe { RingBuffer::new(buffer.as_ptr(), buffer.len()) }.unwrap();
        let mut writer = unsafe { RingBuffer::new(buffer.as_ptr(), buffer.len()) }.unwrap();

        let message = (88, [54, 33, 77, 11, 123]);

        writer.write(message.0, &message.1).unwrap();

        let mut received = reader.read(1);

        assert_eq!(received.len(), 1);

        let received_message = received.remove(0);
        assert_eq!(received_message.0, message.0);
        assert_eq!(received_message.1, message.1);
    }

    #[test]
    fn read_write_read_multiple_messages() {
        let mut buffer = [0u8; 1024 + size_of::<Descriptor>()];

        let mut reader =
            unsafe { RingBuffer::new(buffer.as_mut_ptr() as *const u8, buffer.len()) }.unwrap();
        let mut writer =
            unsafe { RingBuffer::new(buffer.as_mut_ptr() as *const u8, buffer.len()) }.unwrap();

        let message_one = (88, [54, 33, 77, 11, 123]);

        writer.write(message_one.0, &message_one.1).unwrap();

        let mut received = reader.read(1);

        assert_eq!(received.len(), 1);

        let received_message = received.remove(0);
        assert_eq!(received_message.0, message_one.0);
        assert_eq!(received_message.1, message_one.1);

        let message_two = (94, [44, 11]);

        writer.write(message_two.0, &message_two.1).unwrap();

        let mut received = reader.read(1);

        assert_eq!(received.len(), 1);

        let received_message = received.remove(0);
        assert_eq!(received_message.0, message_two.0);
        assert_eq!(received_message.1, message_two.1);
    }

    #[test]
    fn write_read_single_message_multithread() {
        let buffer = [0u8; 1024 + size_of::<Descriptor>()];

        std::thread::scope(|s| {
            s.spawn(|| {
                let message = (88, [54, 33, 77, 11, 123]);
                let mut writer = unsafe { RingBuffer::new(buffer.as_ptr(), buffer.len()) }.unwrap();
                writer.write(message.0, &message.1).unwrap();
            });
            s.spawn(|| {
                let mut reader = unsafe { RingBuffer::new(buffer.as_ptr(), buffer.len()) }.unwrap();
                let mut received = reader.read(1);

                assert_eq!(received.len(), 1);

                let received_message = received.remove(0);
                assert_eq!(received_message.0, 88);
                assert_eq!(received_message.1, [54, 33, 77, 11, 123]);
            });
        })
    }
}
