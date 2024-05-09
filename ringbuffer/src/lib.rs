#![allow(dead_code, unused_variables)]

pub mod descriptor;
pub mod receiver;
pub mod sender;

// #![allow(dead_code, unused_variables)]

use descriptor::{Descriptor, RawDescriptor};
use receiver::Receiver;
use sender::Sender;
use std::{
    alloc::{alloc, dealloc, Layout},
    mem::{align_of, size_of},
    ptr::NonNull,
    sync::{
        atomic::{compiler_fence, AtomicI32, AtomicI64, AtomicUsize, Ordering},
        Arc,
    },
};

// TODO: move somewhere else (aeron-client/src/main/c/util/aeron_binutil.h)
const AERON_CACHE_LINE_LENGTH: usize = 64;

const AERON_RB_TRAILER_LENGTH: usize = size_of::<RawDescriptor>();
const AERON_RB_RECORD_HEADER_LENGTH: usize = size_of::<RecordDescriptor>();
const AERON_MPSC_RB_MIN_CAPACITY: usize = AERON_RB_RECORD_HEADER_LENGTH;
const AERON_RB_ALIGNMENT: usize = 2 * size_of::<i32>();
const AERON_RB_PADDING_MSG_TYPE_ID: i32 = -1;

// TODO: check if buffer can be a slice instead.
#[derive(Debug)]
pub struct RingBuffer {
    buffer: NonNull<u8>,
    capacity: usize,
    descriptor: Descriptor,
    max_message_length: usize,
    reference_count: Arc<AtomicUsize>,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Result<Self, ()> {
        let length: usize = capacity + AERON_RB_TRAILER_LENGTH;

        if is_capacity_valid(capacity, AERON_MPSC_RB_MIN_CAPACITY) {
            let layout = Layout::from_size_align(length, align_of::<RawDescriptor>()).unwrap();
            let buffer = unsafe { alloc(layout) } as *mut u8;
            Ok(Self {
                buffer: NonNull::new(buffer).unwrap(),
                descriptor: {
                    let descriptor_ptr = unsafe { buffer.byte_add(capacity) };
                    let descriptor = Descriptor::new(descriptor_ptr);
                    descriptor.reset();
                    descriptor
                },
                capacity,
                max_message_length: aeron_rb_max_message_length(
                    capacity,
                    AERON_MPSC_RB_MIN_CAPACITY,
                ),
                reference_count: Arc::new(AtomicUsize::new(1)),
            })
        } else {
            Err(()) // TODO: create actual error type with "Invalid capacity: {capacity}" message
        }
    }

    pub unsafe fn from_memory(buffer: *mut u8, length: usize) -> Result<Self, ()> {
        let capacity: usize = length - AERON_RB_TRAILER_LENGTH;

        if is_capacity_valid(capacity, AERON_MPSC_RB_MIN_CAPACITY) {
            Ok(Self {
                buffer: NonNull::new(buffer).unwrap(),
                descriptor: Descriptor::new(buffer.byte_add(capacity)),
                capacity,
                max_message_length: aeron_rb_max_message_length(
                    capacity,
                    AERON_MPSC_RB_MIN_CAPACITY,
                ),
                reference_count: Arc::new(AtomicUsize::new(2)), // This reference + original allocator
            })
        } else {
            Err(()) // TODO: create actual error type with "Invalid capacity: {capacity}" message
        }
    }

    pub fn split(self) -> (Sender, Receiver) {
        self.reference_count.fetch_add(1, Ordering::Relaxed); // By splitting the refcount is increased by one
        (
            Sender {
                buffer: self.buffer,
                capacity: self.capacity,
                descriptor: self.descriptor.into(),
                max_message_length: self.max_message_length,
                reference_count: self.reference_count.clone(),
            },
            Receiver {
                buffer: self.buffer,
                capacity: self.capacity,
                descriptor: self.descriptor.into(),
                _max_message_length: self.max_message_length,
                reference_count: self.reference_count.clone(),
            },
        )
    }

    // pub fn write(&mut self, msg_type_id: i32, msg: &[u8]) -> Result<(), ()> {
    //     if msg.len() > self.max_message_length || aeron_rb_invalid_msg_type_id(msg_type_id) {
    //         return Err(());
    //     }

    //     let record_length: usize = msg.len() + AERON_RB_RECORD_HEADER_LENGTH;
    //     let record_index = self.claim_capacity(record_length)?;

    //     let record_header: *mut RecordDescriptor =
    //         unsafe { self.buffer.as_ptr().byte_add(record_index as usize) }
    //             as *mut RecordDescriptor;

    //     aeron_put_ordered_i32(&unsafe { &*record_header }.length, -(record_length as i32)); // Probably wrong, should likely be an atomic (lets see if loom caches this).

    //     let index = aeron_rb_message_offset(record_index as usize);
    //     let destination_ptr: *mut u8 = unsafe { self.buffer.as_ptr().byte_add(index) };
    //     unsafe { ptr::copy_nonoverlapping(msg.as_ptr(), destination_ptr, msg.len()) };

    //     unsafe { &mut *record_header }.msg_type_id = msg_type_id;
    //     aeron_put_ordered_i32(&unsafe { &*record_header }.length, record_length as i32);

    //     Ok(())
    // }

    // // simplified version of read without handler.
    // // TODO: implement full version
    // pub fn read(&mut self, message_count_limit: usize) -> Vec<(i32, Vec<u8>)> {
    //     let head: i64 = self.descriptor.head_position().load(Ordering::Relaxed);
    //     let head_index: usize = head as usize & (self.capacity - 1);
    //     let contiguous_block_length: usize = self.capacity - head_index;
    //     let mut messages_read: usize = 0;
    //     let mut bytes_read: usize = 0;

    //     // TODO: replace buffer by message handler
    //     let mut read_buffer: Vec<(i32, Vec<u8>)> = Vec::new();

    //     while bytes_read < contiguous_block_length && messages_read < message_count_limit {
    //         let record_index: usize = head_index + bytes_read;
    //         let header: &RecordDescriptor = {
    //             let ptr: *mut u8 = unsafe { self.buffer.as_ptr().byte_add(record_index) };
    //             unsafe { &*(ptr as *const RecordDescriptor) }
    //         };

    //         let mut record_length: i32 = 0;
    //         aeron_get_volatile_i32(&mut record_length, &header.length);

    //         if record_length <= 0 {
    //             break;
    //         }

    //         bytes_read += aeron_align(record_length as usize, AERON_RB_ALIGNMENT);
    //         let msg_type_id: i32 = header.msg_type_id;

    //         if msg_type_id == AERON_RB_PADDING_MSG_TYPE_ID {
    //             continue;
    //         }

    //         messages_read += 1;
    //         let data: &[u8] = {
    //             let index = aeron_rb_message_offset(record_index);
    //             let ptr = unsafe { self.buffer.as_ptr().byte_add(index) };
    //             unsafe {
    //                 slice::from_raw_parts(
    //                     ptr,
    //                     record_length as usize - AERON_RB_RECORD_HEADER_LENGTH,
    //                 )
    //             }
    //         };
    //         read_buffer.push((msg_type_id, data.to_vec()));
    //     }

    //     if bytes_read != 0 {
    //         // Set all the bytes read to 0 with memset
    //         let destination_ptr: *mut u8 = unsafe { self.buffer.as_ptr().byte_add(head_index) };
    //         unsafe { destination_ptr.write_bytes(0, bytes_read) };
    //         aeron_put_ordered_i64(self.descriptor.head_position(), head + bytes_read as i64);
    //     }

    //     // return vec
    //     read_buffer
    // }
}

impl Drop for RingBuffer {
    fn drop(&mut self) {
        if self.reference_count.fetch_sub(1, Ordering::Relaxed) == 0 {
            unsafe {
                free_buffer(self.buffer, self.capacity);
            }
        }
    }
}

unsafe fn free_buffer(buffer: NonNull<u8>, capacity: usize) {
    let length = capacity + AERON_RB_TRAILER_LENGTH;
    let layout = Layout::from_size_align(length, align_of::<RawDescriptor>())
        .expect("expect to create the same layout as used for allocation");
    unsafe { dealloc(buffer.as_ptr(), layout) };
}

#[repr(C, align(4))]
struct RecordDescriptor {
    length: AtomicI32,
    msg_type_id: AtomicI32,
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

// fn aeron_cas_i64(dst: &AtomicI64, expected: i64, desired: i64) -> bool {
//     let original = dst.compare_and_swap(expected, desired, Ordering::SeqCst);
//     original == expected
// }

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
    use super::RingBuffer;

    #[test]
    fn read_write_read_single_message() {
        let (mut sender, mut receiver) = RingBuffer::new(1024).unwrap().split();

        let message = (88, [54, 33, 77, 11, 123]);

        sender.send(message.0, &message.1).unwrap();

        let mut received = receiver.receive(1);

        assert_eq!(received.len(), 1);

        let received_message = received.remove(0);
        assert_eq!(received_message.0, message.0);
        assert_eq!(received_message.1, message.1);
    }

    #[test]
    fn read_write_read_multiple_messages() {
        let (mut sender, mut receiver) = RingBuffer::new(1024).unwrap().split();

        let message_one = (88, [54, 33, 77, 11, 123]);

        sender.send(message_one.0, &message_one.1).unwrap();

        let mut received = receiver.receive(1);

        assert_eq!(received.len(), 1);

        let received_message = received.remove(0);
        assert_eq!(received_message.0, message_one.0);
        assert_eq!(received_message.1, message_one.1);

        let message_two = (94, [44, 11]);

        sender.send(message_two.0, &message_two.1).unwrap();

        let mut received = receiver.receive(1);

        assert_eq!(received.len(), 1);

        let received_message = received.remove(0);
        assert_eq!(received_message.0, message_two.0);
        assert_eq!(received_message.1, message_two.1);
    }

    #[test]
    fn write_read_single_message_multithread() {
        std::thread::scope(|s| {
            let (mut sender, mut receiver) = RingBuffer::new(1024).unwrap().split();

            let t = s.spawn(move || {
                let message = (88, [54, 33, 77, 11, 123]);
                sender.send(message.0, &message.1).unwrap();
            });

            t.join().unwrap();

            s.spawn(move || {
                let mut received = receiver.receive(1);

                assert_eq!(received.len(), 1);

                let received_message = received.remove(0);
                assert_eq!(received_message.0, 88);
                assert_eq!(received_message.1, [54, 33, 77, 11, 123]);
            });
        })
    }
}
